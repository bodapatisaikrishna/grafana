package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	testingclock "k8s.io/utils/clock/testing"

	"github.com/grafana/grafana-app-sdk/logging"
	provisioning "github.com/grafana/grafana/apps/provisioning/pkg/apis/provisioning/v0alpha1"
	"github.com/grafana/grafana/apps/provisioning/pkg/connection"
	fakeclientset "github.com/grafana/grafana/apps/provisioning/pkg/generated/clientset/versioned/fake"
	listers "github.com/grafana/grafana/apps/provisioning/pkg/generated/listers/provisioning/v0alpha1"
	common "github.com/grafana/grafana/pkg/apimachinery/apis/common/v0alpha1"
	"github.com/grafana/grafana/pkg/infra/tracing"
	"github.com/grafana/grafana/pkg/registry/apis/provisioning/controller/mocks"
	"github.com/grafana/grafana/pkg/registry/apis/provisioning/informer"
	usinformer "github.com/grafana/grafana/pkg/storage/unified/informer"
)

type mockConnectionWithToken struct {
	connection.Connection
	connection.TokenConnection
}

func TestConnectionController_process(t *testing.T) {
	testCases := []struct {
		name          string
		setupMocks    func() (*mockConnectionLister, *MockConnectionHealthChecker, *MockConnectionStatusPatcher, *connection.MockFactory)
		conn          *provisioning.Connection
		expectError   bool
		errorContains string
		// wantSecureToken, when set, asserts that the main resource (never
		// statusPatcher) received a /secure/token patch with this "create" value.
		wantSecureToken string
	}{
		{
			name: "deletion timestamp - skip without error",
			setupMocks: func() (*mockConnectionLister, *MockConnectionHealthChecker, *MockConnectionStatusPatcher, *connection.MockFactory) {
				mockLister := &mockConnectionLister{
					conn: &provisioning.Connection{
						ObjectMeta: metav1.ObjectMeta{
							Name:              "test-conn",
							Namespace:         "default",
							DeletionTimestamp: &metav1.Time{Time: time.Now()},
						},
					},
				}
				return mockLister, nil, nil, nil
			},
			conn: &provisioning.Connection{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "test-conn",
					Namespace:         "default",
					DeletionTimestamp: &metav1.Time{Time: time.Now()},
				},
			},
			expectError: false,
		},
		{
			name: "no reconcile needed",
			setupMocks: func() (*mockConnectionLister, *MockConnectionHealthChecker, *MockConnectionStatusPatcher, *connection.MockFactory) {
				mockLister := &mockConnectionLister{
					conn: &provisioning.Connection{
						ObjectMeta: metav1.ObjectMeta{
							Name:       "test-conn",
							Namespace:  "default",
							Generation: 1,
						},
						Status: provisioning.ConnectionStatus{
							ObservedGeneration: 1,
							Health: provisioning.HealthStatus{
								Healthy: true,
								Checked: time.Now().UnixMilli(),
							},
						},
					},
				}
				mockHealthChecker := NewMockConnectionHealthChecker(t)
				mockHealthChecker.EXPECT().ShouldCheckHealth(mock.IsType(&provisioning.Connection{})).Return(false)
				mockFactory := connection.NewMockFactory(t)
				mockConnection := connection.NewMockConnection(t)
				// Build is called before checking if reconciliation is needed, so we need to provide a mock
				mockFactory.EXPECT().Build(mock.Anything, mock.Anything).Return(mockConnection, nil)
				return mockLister, mockHealthChecker, nil, mockFactory
			},
			conn: &provisioning.Connection{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-conn",
					Namespace:  "default",
					Generation: 1,
				},
				Status: provisioning.ConnectionStatus{
					ObservedGeneration: 1,
					Health: provisioning.HealthStatus{
						Healthy: true,
						Checked: time.Now().UnixMilli(),
					},
				},
			},
			expectError: false,
		},
		{
			name: "spec changed - full reconciliation",
			setupMocks: func() (*mockConnectionLister, *MockConnectionHealthChecker, *MockConnectionStatusPatcher, *connection.MockFactory) {
				mockLister := &mockConnectionLister{
					conn: &provisioning.Connection{
						ObjectMeta: metav1.ObjectMeta{
							Name:       "test-conn",
							Namespace:  "default",
							Generation: 2,
						},
						Status: provisioning.ConnectionStatus{
							ObservedGeneration: 1,
							Health: provisioning.HealthStatus{
								Healthy: true,
								Checked: time.Now().Add(-10 * time.Minute).UnixMilli(),
							},
						},
						Spec: provisioning.ConnectionSpec{
							Type: provisioning.GithubConnectionType,
							GitHub: &provisioning.GitHubConnectionConfig{
								AppID:          "123",
								InstallationID: "456",
							},
						},
					},
				}
				mockHealthChecker := NewMockConnectionHealthChecker(t)
				mockStatusPatcher := NewMockConnectionStatusPatcher(t)
				mockFactory := connection.NewMockFactory(t)
				mockConnection := connection.NewMockConnection(t)

				testResults := &provisioning.TestResults{
					Success: true,
					Code:    http.StatusOK,
				}
				healthStatus := provisioning.HealthStatus{
					Healthy: true,
					Checked: time.Now().UnixMilli(),
				}

				mockHealthChecker.EXPECT().ShouldCheckHealth(mock.Anything).Return(true)
				mockFactory.EXPECT().Build(mock.Anything, mock.Anything).Return(mockConnection, nil)
				mockHealthChecker.EXPECT().RefreshHealthWithPatchOps(mock.Anything, mock.Anything).
					Return(ConnectionHealthResultWithPatchOps{
						TestResults:  testResults,
						HealthStatus: healthStatus,
						PatchOps: []map[string]interface{}{
							{"op": "replace", "path": "/status/health", "value": healthStatus},
						},
					}, nil)
				mockStatusPatcher.EXPECT().Patch(
					mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
				).Return(nil)

				return mockLister, mockHealthChecker, mockStatusPatcher, mockFactory
			},
			conn: &provisioning.Connection{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-conn",
					Namespace:  "default",
					Generation: 2,
				},
				Status: provisioning.ConnectionStatus{
					ObservedGeneration: 1,
					Health: provisioning.HealthStatus{
						Healthy: true,
						Checked: time.Now().Add(-10 * time.Minute).UnixMilli(),
					},
				},
				Spec: provisioning.ConnectionSpec{
					Type: provisioning.GithubConnectionType,
					GitHub: &provisioning.GitHubConnectionConfig{
						AppID:          "123",
						InstallationID: "456",
					},
				},
			},
			expectError: false,
		},
		{
			name: "unhealthy with token regeneration",
			setupMocks: func() (*mockConnectionLister, *MockConnectionHealthChecker, *MockConnectionStatusPatcher, *connection.MockFactory) {
				mockLister := &mockConnectionLister{
					conn: &provisioning.Connection{
						ObjectMeta: metav1.ObjectMeta{
							Name:       "test-conn",
							Namespace:  "default",
							Generation: 1,
						},
						Status: provisioning.ConnectionStatus{
							ObservedGeneration: 1,
							Health: provisioning.HealthStatus{
								Healthy: false,
								Checked: time.Now().Add(-2 * time.Minute).UnixMilli(),
								Error:   provisioning.HealthFailureHealth,
							},
						},
						Spec: provisioning.ConnectionSpec{
							Type: provisioning.GithubConnectionType,
							GitHub: &provisioning.GitHubConnectionConfig{
								AppID:          "123",
								InstallationID: "456",
							},
						},
						Secure: provisioning.ConnectionSecure{
							Token: common.InlineSecureValue{
								Name: "existing-token",
							},
						},
					},
				}
				mockHealthChecker := NewMockConnectionHealthChecker(t)
				mockStatusPatcher := NewMockConnectionStatusPatcher(t)
				mockFactory := connection.NewMockFactory(t)
				mockConnection := connection.NewMockConnection(t)
				mockTokenConnection := connection.NewMockTokenConnection(t)
				mockConnWithToken := &mockConnectionWithToken{
					Connection:      mockConnection,
					TokenConnection: mockTokenConnection,
				}

				testResults := &provisioning.TestResults{
					Success: false,
					Code:    http.StatusBadRequest,
					Errors:  []provisioning.ErrorDetails{{Detail: "connection failed"}},
				}
				healthStatus := provisioning.HealthStatus{
					Healthy: false,
					Checked: time.Now().UnixMilli(),
					Error:   provisioning.HealthFailureHealth,
					Message: []string{"connection failed"},
				}

				mockHealthChecker.EXPECT().ShouldCheckHealth(mock.Anything).Return(true)
				mockFactory.EXPECT().Build(mock.Anything, mock.Anything).Return(mockConnWithToken, nil)
				// Token expires in 2 minutes - should trigger regeneration
				mockTokenConnection.EXPECT().ValidateToken().Return(time.Now().Add(2*time.Minute), nil)
				mockTokenConnection.EXPECT().GenerateConnectionToken(mock.Anything).Return(&connection.ExpirableSecureValue{Token: "new-token"}, nil)
				mockHealthChecker.EXPECT().RefreshHealthWithPatchOps(mock.Anything, mock.Anything).
					Return(ConnectionHealthResultWithPatchOps{
						TestResults:  testResults,
						HealthStatus: healthStatus,
						PatchOps: []map[string]interface{}{
							{"op": "replace", "path": "/status/health", "value": healthStatus},
						},
					}, nil)
				mockStatusPatcher.EXPECT().Patch(
					mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
				).Return(nil)

				return mockLister, mockHealthChecker, mockStatusPatcher, mockFactory
			},
			conn: &provisioning.Connection{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-conn",
					Namespace:  "default",
					Generation: 1,
				},
				Status: provisioning.ConnectionStatus{
					ObservedGeneration: 1,
					Health: provisioning.HealthStatus{
						Healthy: false,
						Checked: time.Now().Add(-2 * time.Minute).UnixMilli(),
						Error:   provisioning.HealthFailureHealth,
					},
				},
				Spec: provisioning.ConnectionSpec{
					Type: provisioning.GithubConnectionType,
					GitHub: &provisioning.GitHubConnectionConfig{
						AppID:          "123",
						InstallationID: "456",
					},
				},
				Secure: provisioning.ConnectionSecure{
					Token: common.InlineSecureValue{
						Name: "existing-token",
					},
				},
			},
			expectError:     false,
			wantSecureToken: "new-token",
		},
		{
			name: "token not expired and not regenerated as it's new",
			setupMocks: func() (*mockConnectionLister, *MockConnectionHealthChecker, *MockConnectionStatusPatcher, *connection.MockFactory) {
				mockLister := &mockConnectionLister{
					conn: &provisioning.Connection{
						ObjectMeta: metav1.ObjectMeta{
							Name:       "test-conn",
							Namespace:  "default",
							Generation: 1,
						},
						Status: provisioning.ConnectionStatus{
							ObservedGeneration: 1,
							Health: provisioning.HealthStatus{
								Healthy: true,
								Checked: time.Now().Add(-10 * time.Minute).UnixMilli(),
							},
						},
						Spec: provisioning.ConnectionSpec{
							Type: provisioning.GithubConnectionType,
						},
						Secure: provisioning.ConnectionSecure{
							Token: common.InlineSecureValue{
								Name: "existing-token",
							},
						},
					},
				}
				mockHealthChecker := NewMockConnectionHealthChecker(t)
				mockStatusPatcher := NewMockConnectionStatusPatcher(t)
				mockFactory := connection.NewMockFactory(t)
				mockConnection := connection.NewMockConnection(t)
				mockTokenConnection := connection.NewMockTokenConnection(t)
				mockConnWithToken := &mockConnectionWithToken{
					Connection:      mockConnection,
					TokenConnection: mockTokenConnection,
				}

				testResults := &provisioning.TestResults{
					Success: true,
					Code:    http.StatusOK,
				}
				healthStatus := provisioning.HealthStatus{
					Healthy: true,
					Checked: time.Now().UnixMilli(),
				}

				mockHealthChecker.EXPECT().ShouldCheckHealth(mock.Anything).Return(true)
				mockFactory.EXPECT().Build(mock.Anything, mock.Anything).Return(mockConnWithToken, nil)
				// Token was written very recently (see the fixture's status), so no
				// refresh should happen.
				mockTokenConnection.EXPECT().ValidateToken().Return(time.Now().Add(2*time.Hour), nil)
				mockHealthChecker.EXPECT().RefreshHealthWithPatchOps(mock.Anything, mock.Anything).
					Return(ConnectionHealthResultWithPatchOps{
						TestResults:  testResults,
						HealthStatus: healthStatus,
						PatchOps: []map[string]interface{}{
							{"op": "replace", "path": "/status/health", "value": healthStatus},
						},
					}, nil)
				mockStatusPatcher.EXPECT().Patch(
					mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
				).Run(
					func(ctx context.Context, conn *provisioning.Connection, patchOperations ...map[string]interface{}) {
						found := false
						for _, op := range patchOperations {
							if op["op"].(string) == "replace" &&
								op["path"].(string) == "/secure/token" &&
								op["value"].(map[string]string)["create"] == "someToken" {
								found = true
							}
						}
						require.False(t, found)
					},
				).Return(nil)

				return mockLister, mockHealthChecker, mockStatusPatcher, mockFactory
			},
			conn: &provisioning.Connection{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-conn",
					Namespace:  "default",
					Generation: 1,
				},
				Status: provisioning.ConnectionStatus{
					ObservedGeneration: 1,
					Health: provisioning.HealthStatus{
						Healthy: true,
						Checked: time.Now().Add(-10 * time.Minute).UnixMilli(),
					},
					Token: provisioning.TokenStatus{
						LastUpdated: time.Now().Add(-8 * time.Second).UnixMilli(),
					},
				},
				Spec: provisioning.ConnectionSpec{
					Type: provisioning.GithubConnectionType,
				},
				Secure: provisioning.ConnectionSecure{
					Token: common.InlineSecureValue{
						Name: "existing-token",
					},
				},
			},
			expectError: false,
		},
		{
			name: "token not expired and not regenerated",
			setupMocks: func() (*mockConnectionLister, *MockConnectionHealthChecker, *MockConnectionStatusPatcher, *connection.MockFactory) {
				mockLister := &mockConnectionLister{
					conn: &provisioning.Connection{
						ObjectMeta: metav1.ObjectMeta{
							Name:       "test-conn",
							Namespace:  "default",
							Generation: 1,
						},
						Status: provisioning.ConnectionStatus{
							ObservedGeneration: 1,
							Health: provisioning.HealthStatus{
								Healthy: true,
								Checked: time.Now().Add(-10 * time.Minute).UnixMilli(),
							},
						},
						Spec: provisioning.ConnectionSpec{
							Type: provisioning.GithubConnectionType,
						},
						Secure: provisioning.ConnectionSecure{
							Token: common.InlineSecureValue{
								Name: "existing-token",
							},
						},
					},
				}
				mockHealthChecker := NewMockConnectionHealthChecker(t)
				mockStatusPatcher := NewMockConnectionStatusPatcher(t)
				mockFactory := connection.NewMockFactory(t)
				mockConnection := connection.NewMockConnection(t)
				mockTokenConnection := connection.NewMockTokenConnection(t)
				mockConnWithToken := &mockConnectionWithToken{
					Connection:      mockConnection,
					TokenConnection: mockTokenConnection,
				}

				testResults := &provisioning.TestResults{
					Success: true,
					Code:    http.StatusOK,
				}
				healthStatus := provisioning.HealthStatus{
					Healthy: true,
					Checked: time.Now().UnixMilli(),
				}

				mockHealthChecker.EXPECT().ShouldCheckHealth(mock.Anything).Return(true)
				mockFactory.EXPECT().Build(mock.Anything, mock.Anything).Return(mockConnWithToken, nil)
				// Token expires in 15 minutes - with buffer of 10m10s (2*5m + 10s), this will NOT trigger regeneration
				mockTokenConnection.EXPECT().ValidateToken().Return(time.Now().Add(15*time.Minute), nil)
				mockHealthChecker.EXPECT().RefreshHealthWithPatchOps(mock.Anything, mock.Anything).
					Return(ConnectionHealthResultWithPatchOps{
						TestResults:  testResults,
						HealthStatus: healthStatus,
						PatchOps: []map[string]interface{}{
							{"op": "replace", "path": "/status/health", "value": healthStatus},
						},
					}, nil)
				mockStatusPatcher.EXPECT().Patch(
					mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
				).Run(
					func(ctx context.Context, conn *provisioning.Connection, patchOperations ...map[string]interface{}) {
						found := false
						for _, op := range patchOperations {
							if op["op"].(string) == "replace" &&
								op["path"].(string) == "/secure/token" &&
								op["value"].(map[string]string)["create"] == "someToken" {
								found = true
							}
						}
						require.False(t, found)
					},
				).Return(nil)

				return mockLister, mockHealthChecker, mockStatusPatcher, mockFactory
			},
			conn: &provisioning.Connection{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-conn",
					Namespace:  "default",
					Generation: 1,
				},
				Status: provisioning.ConnectionStatus{
					ObservedGeneration: 1,
					Health: provisioning.HealthStatus{
						Healthy: true,
						Checked: time.Now().Add(-10 * time.Minute).UnixMilli(),
					},
				},
				Spec: provisioning.ConnectionSpec{
					Type: provisioning.GithubConnectionType,
				},
				Secure: provisioning.ConnectionSecure{
					Token: common.InlineSecureValue{
						Name: "existing-token",
					},
				},
			},
			expectError: false,
		},
		{
			name: "token not expired but regenerated",
			setupMocks: func() (*mockConnectionLister, *MockConnectionHealthChecker, *MockConnectionStatusPatcher, *connection.MockFactory) {
				mockLister := &mockConnectionLister{
					conn: &provisioning.Connection{
						ObjectMeta: metav1.ObjectMeta{
							Name:       "test-conn",
							Namespace:  "default",
							Generation: 1,
						},
						Status: provisioning.ConnectionStatus{
							ObservedGeneration: 1,
							Health: provisioning.HealthStatus{
								Healthy: true,
								Checked: time.Now().Add(-10 * time.Minute).UnixMilli(),
							},
						},
						Spec: provisioning.ConnectionSpec{
							Type: provisioning.GithubConnectionType,
						},
						Secure: provisioning.ConnectionSecure{
							Token: common.InlineSecureValue{
								Name: "existing-token",
							},
						},
					},
				}
				mockHealthChecker := NewMockConnectionHealthChecker(t)
				mockStatusPatcher := NewMockConnectionStatusPatcher(t)
				mockFactory := connection.NewMockFactory(t)
				mockConnection := connection.NewMockConnection(t)
				mockTokenConnection := connection.NewMockTokenConnection(t)
				mockConnWithToken := &mockConnectionWithToken{
					Connection:      mockConnection,
					TokenConnection: mockTokenConnection,
				}

				testResults := &provisioning.TestResults{
					Success: true,
					Code:    http.StatusOK,
				}
				healthStatus := provisioning.HealthStatus{
					Healthy: true,
					Checked: time.Now().UnixMilli(),
				}

				mockHealthChecker.EXPECT().ShouldCheckHealth(mock.Anything).Return(true)
				mockFactory.EXPECT().Build(mock.Anything, mock.Anything).Return(mockConnWithToken, nil)
				// Token expires in 9 minutes - with buffer of 10m10s (2*5m + 10s), this WILL trigger regeneration
				mockTokenConnection.EXPECT().ValidateToken().Return(time.Now().Add(9*time.Minute), nil)
				mockTokenConnection.EXPECT().GenerateConnectionToken(mock.Anything).Return(&connection.ExpirableSecureValue{Token: "someToken"}, nil)
				mockHealthChecker.EXPECT().RefreshHealthWithPatchOps(mock.Anything, mock.Anything).
					Return(ConnectionHealthResultWithPatchOps{
						TestResults:  testResults,
						HealthStatus: healthStatus,
						PatchOps: []map[string]interface{}{
							{"op": "replace", "path": "/status/health", "value": healthStatus},
						},
					}, nil)
				mockStatusPatcher.EXPECT().Patch(
					mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
				).Return(nil)

				return mockLister, mockHealthChecker, mockStatusPatcher, mockFactory
			},
			conn: &provisioning.Connection{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-conn",
					Namespace:  "default",
					Generation: 1,
				},
				Status: provisioning.ConnectionStatus{
					ObservedGeneration: 1,
					Health: provisioning.HealthStatus{
						Healthy: true,
						Checked: time.Now().Add(-10 * time.Minute).UnixMilli(),
					},
				},
				Spec: provisioning.ConnectionSpec{
					Type: provisioning.GithubConnectionType,
				},
				Secure: provisioning.ConnectionSecure{
					Token: common.InlineSecureValue{
						Name: "existing-token",
					},
				},
			},
			expectError:     false,
			wantSecureToken: "someToken",
		},
		{
			name: "token expired but health check not needed",
			setupMocks: func() (*mockConnectionLister, *MockConnectionHealthChecker, *MockConnectionStatusPatcher, *connection.MockFactory) {
				mockLister := &mockConnectionLister{
					conn: &provisioning.Connection{
						ObjectMeta: metav1.ObjectMeta{
							Name:       "test-conn",
							Namespace:  "default",
							Generation: 1,
						},
						Status: provisioning.ConnectionStatus{
							ObservedGeneration: 1,
							Health: provisioning.HealthStatus{
								Healthy: true,
								Checked: time.Now().Add(-1 * time.Minute).UnixMilli(), // Recently checked
							},
						},
						Spec: provisioning.ConnectionSpec{
							Type: provisioning.GithubConnectionType,
						},
						Secure: provisioning.ConnectionSecure{
							Token: common.InlineSecureValue{
								Name: "existing-token",
							},
						},
					},
				}
				mockHealthChecker := NewMockConnectionHealthChecker(t)
				mockStatusPatcher := NewMockConnectionStatusPatcher(t)
				mockFactory := connection.NewMockFactory(t)
				mockConnection := connection.NewMockConnection(t)
				mockTokenConnection := connection.NewMockTokenConnection(t)
				mockConnWithToken := &mockConnectionWithToken{
					Connection:      mockConnection,
					TokenConnection: mockTokenConnection,
				}

				testResults := &provisioning.TestResults{
					Success: true,
					Code:    http.StatusOK,
				}
				healthStatus := provisioning.HealthStatus{
					Healthy: true,
					Checked: time.Now().UnixMilli(),
				}

				// Health check is NOT needed (recently checked)
				mockHealthChecker.EXPECT().ShouldCheckHealth(mock.Anything).Return(false)
				mockFactory.EXPECT().Build(mock.Anything, mock.Anything).Return(mockConnWithToken, nil)
				// Token expires in 9 minutes - with buffer of 10m10s (2*5m + 10s), this WILL trigger regeneration
				mockTokenConnection.EXPECT().ValidateToken().Return(time.Now().Add(9*time.Minute), nil)
				mockTokenConnection.EXPECT().GenerateConnectionToken(mock.Anything).Return(&connection.ExpirableSecureValue{Token: "new-token"}, nil)
				// Health check is still performed as part of reconciliation even though ShouldCheckHealth returned false
				mockHealthChecker.EXPECT().RefreshHealthWithPatchOps(mock.Anything, mock.Anything).
					Return(ConnectionHealthResultWithPatchOps{
						TestResults:  testResults,
						HealthStatus: healthStatus,
						PatchOps: []map[string]interface{}{
							{"op": "replace", "path": "/status/health", "value": healthStatus},
						},
					}, nil)
				mockStatusPatcher.EXPECT().Patch(
					mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
				).Return(nil)

				return mockLister, mockHealthChecker, mockStatusPatcher, mockFactory
			},
			conn: &provisioning.Connection{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-conn",
					Namespace:  "default",
					Generation: 1,
				},
				Status: provisioning.ConnectionStatus{
					ObservedGeneration: 1,
					Health: provisioning.HealthStatus{
						Healthy: true,
						Checked: time.Now().Add(-1 * time.Minute).UnixMilli(),
					},
				},
				Spec: provisioning.ConnectionSpec{
					Type: provisioning.GithubConnectionType,
				},
				Secure: provisioning.ConnectionSecure{
					Token: common.InlineSecureValue{
						Name: "existing-token",
					},
				},
			},
			expectError:     false,
			wantSecureToken: "new-token",
		},
		{
			name: "health check failure",
			setupMocks: func() (*mockConnectionLister, *MockConnectionHealthChecker, *MockConnectionStatusPatcher, *connection.MockFactory) {
				mockLister := &mockConnectionLister{
					conn: &provisioning.Connection{
						ObjectMeta: metav1.ObjectMeta{
							Name:       "test-conn",
							Namespace:  "default",
							Generation: 1,
						},
						Status: provisioning.ConnectionStatus{
							ObservedGeneration: 1,
							Health: provisioning.HealthStatus{
								Healthy: true,
								Checked: time.Now().Add(-10 * time.Minute).UnixMilli(),
							},
						},
						Spec: provisioning.ConnectionSpec{
							Type: provisioning.GithubConnectionType,
						},
					},
				}
				mockHealthChecker := NewMockConnectionHealthChecker(t)
				mockFactory := connection.NewMockFactory(t)
				mockConnection := connection.NewMockConnection(t)

				mockHealthChecker.EXPECT().ShouldCheckHealth(mock.Anything).Return(true)
				mockFactory.EXPECT().Build(mock.Anything, mock.Anything).Return(mockConnection, nil)
				mockHealthChecker.EXPECT().RefreshHealthWithPatchOps(mock.Anything, mock.Anything).
					Return(ConnectionHealthResultWithPatchOps{}, errors.New("health check failed"))

				return mockLister, mockHealthChecker, nil, mockFactory
			},
			conn: &provisioning.Connection{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-conn",
					Namespace:  "default",
					Generation: 1,
				},
				Status: provisioning.ConnectionStatus{
					ObservedGeneration: 1,
					Health: provisioning.HealthStatus{
						Healthy: true,
						Checked: time.Now().Add(-10 * time.Minute).UnixMilli(),
					},
				},
				Spec: provisioning.ConnectionSpec{
					Type: provisioning.GithubConnectionType,
				},
			},
			expectError:   true,
			errorContains: "update health status",
		},
		{
			name: "patch error",
			setupMocks: func() (*mockConnectionLister, *MockConnectionHealthChecker, *MockConnectionStatusPatcher, *connection.MockFactory) {
				mockLister := &mockConnectionLister{
					conn: &provisioning.Connection{
						ObjectMeta: metav1.ObjectMeta{
							Name:       "test-conn",
							Namespace:  "default",
							Generation: 2,
						},
						Status: provisioning.ConnectionStatus{
							ObservedGeneration: 1,
						},
						Spec: provisioning.ConnectionSpec{
							Type: provisioning.GithubConnectionType,
						},
					},
				}
				mockHealthChecker := NewMockConnectionHealthChecker(t)
				mockStatusPatcher := NewMockConnectionStatusPatcher(t)
				mockFactory := connection.NewMockFactory(t)
				mockConnection := connection.NewMockConnection(t)

				testResults := &provisioning.TestResults{
					Success: true,
					Code:    http.StatusOK,
				}
				healthStatus := provisioning.HealthStatus{
					Healthy: true,
					Checked: time.Now().UnixMilli(),
				}

				mockHealthChecker.EXPECT().ShouldCheckHealth(mock.Anything).Return(true)
				mockFactory.EXPECT().Build(mock.Anything, mock.Anything).Return(mockConnection, nil)
				mockHealthChecker.EXPECT().RefreshHealthWithPatchOps(mock.Anything, mock.Anything).
					Return(ConnectionHealthResultWithPatchOps{
						TestResults:  testResults,
						HealthStatus: healthStatus,
						PatchOps: []map[string]interface{}{
							{"op": "replace", "path": "/status/health", "value": healthStatus},
						},
					}, nil)
				mockStatusPatcher.EXPECT().Patch(
					mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
				).Return(errors.New("patch failed"))

				return mockLister, mockHealthChecker, mockStatusPatcher, mockFactory
			},
			conn: &provisioning.Connection{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-conn",
					Namespace:  "default",
					Generation: 2,
				},
				Status: provisioning.ConnectionStatus{
					ObservedGeneration: 1,
				},
				Spec: provisioning.ConnectionSpec{
					Type: provisioning.GithubConnectionType,
				},
			},
			expectError:   true,
			errorContains: "failed to update connection status",
		},
		{
			name: "connection not found",
			setupMocks: func() (*mockConnectionLister, *MockConnectionHealthChecker, *MockConnectionStatusPatcher, *connection.MockFactory) {
				mockLister := &mockConnectionLister{}

				return mockLister, nil, nil, nil
			},
			conn: &provisioning.Connection{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-conn",
					Namespace: "default",
				},
			},
			expectError: true,
		},
		{
			name: "build connection error",
			setupMocks: func() (*mockConnectionLister, *MockConnectionHealthChecker, *MockConnectionStatusPatcher, *connection.MockFactory) {
				mockLister := &mockConnectionLister{
					conn: &provisioning.Connection{
						ObjectMeta: metav1.ObjectMeta{
							Name:       "test-conn",
							Namespace:  "default",
							Generation: 2,
						},
						Status: provisioning.ConnectionStatus{
							ObservedGeneration: 1,
						},
						Spec: provisioning.ConnectionSpec{
							Type: provisioning.GithubConnectionType,
						},
					},
				}
				mockHealthChecker := NewMockConnectionHealthChecker(t)
				mockFactory := connection.NewMockFactory(t)

				mockHealthChecker.EXPECT().ShouldCheckHealth(mock.Anything).Return(true)
				mockFactory.EXPECT().Build(mock.Anything, mock.Anything).
					Return(nil, errors.New("failed to build connection"))

				return mockLister, mockHealthChecker, nil, mockFactory
			},
			conn: &provisioning.Connection{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-conn",
					Namespace:  "default",
					Generation: 2,
				},
				Status: provisioning.ConnectionStatus{
					ObservedGeneration: 1,
				},
				Spec: provisioning.ConnectionSpec{
					Type: provisioning.GithubConnectionType,
				},
			},
			expectError:   true,
			errorContains: "failed to build connection",
		},
		{
			name: "missing token triggers initial token generation",
			setupMocks: func() (*mockConnectionLister, *MockConnectionHealthChecker, *MockConnectionStatusPatcher, *connection.MockFactory) {
				mockLister := &mockConnectionLister{
					conn: &provisioning.Connection{
						ObjectMeta: metav1.ObjectMeta{
							Name:       "test-conn",
							Namespace:  "default",
							Generation: 1,
						},
						Status: provisioning.ConnectionStatus{
							ObservedGeneration: 1,
							Health: provisioning.HealthStatus{
								Healthy: true,
								Checked: time.Now().Add(-10 * time.Minute).UnixMilli(),
							},
						},
						Spec: provisioning.ConnectionSpec{
							Type: provisioning.GithubConnectionType,
						},
						Secure: provisioning.ConnectionSecure{
							// Token missing (IsZero()), but PrivateKey already set - a
							// GitHub App connection always has its private key configured
							// before a token is ever generated, so /secure is never
							// actually empty by the time the token write happens.
							PrivateKey: common.InlineSecureValue{Name: "existing-private-key"},
						},
					},
				}
				mockHealthChecker := NewMockConnectionHealthChecker(t)
				mockFactory := connection.NewMockFactory(t)
				mockConnection := connection.NewMockConnection(t)
				mockTokenConnection := connection.NewMockTokenConnection(t)
				mockConnWithToken := &mockConnectionWithToken{
					Connection:      mockConnection,
					TokenConnection: mockTokenConnection,
				}

				mockHealthChecker.EXPECT().ShouldCheckHealth(mock.Anything).Return(true)
				mockFactory.EXPECT().Build(mock.Anything, mock.Anything).Return(mockConnWithToken, nil)
				// Token is missing, so controller should generate it without checking its state
				mockTokenConnection.EXPECT().GenerateConnectionToken(mock.Anything).Return(&connection.ExpirableSecureValue{Token: "new-token"}, nil)
				// Health check should be performed after token generation
				mockHealthChecker.EXPECT().RefreshHealthWithPatchOps(mock.Anything, mock.Anything).Return(
					ConnectionHealthResultWithPatchOps{
						TestResults:  &provisioning.TestResults{Success: true},
						HealthStatus: provisioning.HealthStatus{Healthy: true},
						PatchOps:     []map[string]interface{}{},
					},
					nil,
				)

				mockPatcher := NewMockConnectionStatusPatcher(t)
				mockPatcher.EXPECT().Patch(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

				return mockLister, mockHealthChecker, mockPatcher, mockFactory
			},
			conn: &provisioning.Connection{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-conn",
					Namespace:  "default",
					Generation: 1,
				},
				Status: provisioning.ConnectionStatus{
					ObservedGeneration: 1,
					Health: provisioning.HealthStatus{
						Healthy: true,
						Checked: time.Now().Add(-10 * time.Minute).UnixMilli(),
					},
				},
				Spec: provisioning.ConnectionSpec{
					Type: provisioning.GithubConnectionType,
				},
				Secure: provisioning.ConnectionSecure{
					// Token missing, but PrivateKey already set - see setupMocks above.
					PrivateKey: common.InlineSecureValue{Name: "existing-private-key"},
				},
			},
			expectError: false,
		},
		{
			name: "invalid token triggers regeneration",
			setupMocks: func() (*mockConnectionLister, *MockConnectionHealthChecker, *MockConnectionStatusPatcher, *connection.MockFactory) {
				mockLister := &mockConnectionLister{
					conn: &provisioning.Connection{
						ObjectMeta: metav1.ObjectMeta{
							Name:       "test-conn",
							Namespace:  "default",
							Generation: 1,
						},
						Status: provisioning.ConnectionStatus{
							ObservedGeneration: 1,
							Health: provisioning.HealthStatus{
								Healthy: true,
								Checked: time.Now().Add(-10 * time.Minute).UnixMilli(),
							},
						},
						Spec: provisioning.ConnectionSpec{
							Type: provisioning.GithubConnectionType,
						},
						Secure: provisioning.ConnectionSecure{
							Token: common.InlineSecureValue{
								Name: "existing-token",
							},
						},
					},
				}
				mockHealthChecker := NewMockConnectionHealthChecker(t)
				mockStatusPatcher := NewMockConnectionStatusPatcher(t)
				mockFactory := connection.NewMockFactory(t)
				mockConnection := connection.NewMockConnection(t)
				mockTokenConnection := connection.NewMockTokenConnection(t)
				mockConnWithToken := &mockConnectionWithToken{
					Connection:      mockConnection,
					TokenConnection: mockTokenConnection,
				}

				testResults := &provisioning.TestResults{
					Success: true,
					Code:    http.StatusOK,
				}
				healthStatus := provisioning.HealthStatus{
					Healthy: true,
					Checked: time.Now().UnixMilli(),
				}

				mockHealthChecker.EXPECT().ShouldCheckHealth(mock.Anything).Return(true)
				mockFactory.EXPECT().Build(mock.Anything, mock.Anything).Return(mockConnWithToken, nil)
				// Token is not usable - should trigger immediate regeneration
				mockTokenConnection.EXPECT().ValidateToken().Return(time.Time{}, errors.New("invalid token"))
				mockTokenConnection.EXPECT().GenerateConnectionToken(mock.Anything).Return(&connection.ExpirableSecureValue{Token: "new-token"}, nil)
				mockHealthChecker.EXPECT().RefreshHealthWithPatchOps(mock.Anything, mock.Anything).
					Return(ConnectionHealthResultWithPatchOps{
						TestResults:  testResults,
						HealthStatus: healthStatus,
						PatchOps: []map[string]interface{}{
							{"op": "replace", "path": "/status/health", "value": healthStatus},
						},
					}, nil)
				mockStatusPatcher.EXPECT().Patch(
					mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
				).Return(nil)

				return mockLister, mockHealthChecker, mockStatusPatcher, mockFactory
			},
			conn: &provisioning.Connection{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-conn",
					Namespace:  "default",
					Generation: 1,
				},
				Status: provisioning.ConnectionStatus{
					ObservedGeneration: 1,
					Health: provisioning.HealthStatus{
						Healthy: true,
						Checked: time.Now().Add(-10 * time.Minute).UnixMilli(),
					},
				},
				Spec: provisioning.ConnectionSpec{
					Type: provisioning.GithubConnectionType,
				},
				Secure: provisioning.ConnectionSecure{
					Token: common.InlineSecureValue{
						Name: "existing-token",
					},
				},
			},
			expectError:     false,
			wantSecureToken: "new-token",
		},
		{
			name: "token generation error - continues with health check",
			setupMocks: func() (*mockConnectionLister, *MockConnectionHealthChecker, *MockConnectionStatusPatcher, *connection.MockFactory) {
				mockLister := &mockConnectionLister{
					conn: &provisioning.Connection{
						ObjectMeta: metav1.ObjectMeta{
							Name:       "test-conn",
							Namespace:  "default",
							Generation: 1,
						},
						Status: provisioning.ConnectionStatus{
							ObservedGeneration: 1,
							Health: provisioning.HealthStatus{
								Healthy: false,
								Checked: time.Now().Add(-2 * time.Minute).UnixMilli(),
							},
						},
						Spec: provisioning.ConnectionSpec{
							Type: provisioning.GithubConnectionType,
						},
						Secure: provisioning.ConnectionSecure{
							Token: common.InlineSecureValue{
								Name: "existing-token",
							},
						},
					},
				}
				mockHealthChecker := NewMockConnectionHealthChecker(t)
				mockStatusPatcher := NewMockConnectionStatusPatcher(t)
				mockFactory := connection.NewMockFactory(t)
				mockConnection := connection.NewMockConnection(t)
				mockTokenConnection := connection.NewMockTokenConnection(t)
				mockConnWithToken := &mockConnectionWithToken{
					Connection:      mockConnection,
					TokenConnection: mockTokenConnection,
				}

				testResults := &provisioning.TestResults{
					Success: false,
					Code:    http.StatusBadRequest,
				}
				healthStatus := provisioning.HealthStatus{
					Healthy: false,
					Checked: time.Now().UnixMilli(),
				}

				mockHealthChecker.EXPECT().ShouldCheckHealth(mock.Anything).Return(true)
				mockFactory.EXPECT().Build(mock.Anything, mock.Anything).Return(mockConnWithToken, nil)
				// Token expires in 2 minutes - should trigger regeneration attempt (within 5-minute window)
				mockTokenConnection.EXPECT().ValidateToken().Return(time.Now().Add(2*time.Minute), nil)
				mockTokenConnection.EXPECT().GenerateConnectionToken(mock.Anything).
					Return(nil, errors.New("token generation failed"))
				mockHealthChecker.EXPECT().RefreshHealthWithPatchOps(mock.Anything, mock.Anything).
					Return(ConnectionHealthResultWithPatchOps{
						TestResults:  testResults,
						HealthStatus: healthStatus,
						PatchOps: []map[string]interface{}{
							{"op": "replace", "path": "/status/health", "value": healthStatus},
						},
					}, nil)
				mockStatusPatcher.EXPECT().Patch(
					mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
				).Return(nil)

				return mockLister, mockHealthChecker, mockStatusPatcher, mockFactory
			},
			conn: &provisioning.Connection{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-conn",
					Namespace:  "default",
					Generation: 1,
				},
				Status: provisioning.ConnectionStatus{
					ObservedGeneration: 1,
					Health: provisioning.HealthStatus{
						Healthy: false,
						Checked: time.Now().Add(-2 * time.Minute).UnixMilli(),
					},
				},
				Spec: provisioning.ConnectionSpec{
					Type: provisioning.GithubConnectionType,
				},
				Secure: provisioning.ConnectionSecure{
					Token: common.InlineSecureValue{
						Name: "existing-token",
					},
				},
			},
			expectError: false,
		},
		{
			name: "token secret not found - rebuilds and regenerates",
			setupMocks: func() (*mockConnectionLister, *MockConnectionHealthChecker, *MockConnectionStatusPatcher, *connection.MockFactory) {
				mockLister := &mockConnectionLister{
					conn: &provisioning.Connection{
						ObjectMeta: metav1.ObjectMeta{
							Name:       "test-conn",
							Namespace:  "default",
							Generation: 1,
						},
						Status: provisioning.ConnectionStatus{
							ObservedGeneration: 1,
							Health: provisioning.HealthStatus{
								Healthy: true,
								Checked: time.Now().UnixMilli(),
							},
						},
						Spec: provisioning.ConnectionSpec{
							Type: provisioning.GithubConnectionType,
							GitHub: &provisioning.GitHubConnectionConfig{
								AppID:          "123",
								InstallationID: "456",
							},
						},
						// Orphaned reference: name is set but the secret can't be decrypted.
						Secure: provisioning.ConnectionSecure{
							Token: common.InlineSecureValue{Name: "orphaned-token"},
						},
					},
				}
				mockHealthChecker := NewMockConnectionHealthChecker(t)
				mockStatusPatcher := NewMockConnectionStatusPatcher(t)
				mockFactory := connection.NewMockFactory(t)
				mockConnection := connection.NewMockConnection(t)
				mockTokenConnection := connection.NewMockTokenConnection(t)
				mockConnWithToken := &mockConnectionWithToken{
					Connection:      mockConnection,
					TokenConnection: mockTokenConnection,
				}

				// No spec change and health is fresh, so only the token path drives reconcile.
				mockHealthChecker.EXPECT().ShouldCheckHealth(mock.Anything).Return(false)

				// First build fails because the token secret is missing; after the reference
				// is cleared the rebuild succeeds.
				mockFactory.EXPECT().Build(mock.Anything, mock.Anything).
					Return(nil, fmt.Errorf("unable to decrypt token: %w", connection.ErrTokenNotFound)).Once()
				mockFactory.EXPECT().Build(mock.Anything, mock.Anything).
					Return(mockConnWithToken, nil).Once()

				// Token is now zero (cleared), so it is regenerated without validity checks.
				mockTokenConnection.EXPECT().GenerateConnectionToken(mock.Anything).
					Return(&connection.ExpirableSecureValue{Token: "new-token"}, nil)

				mockHealthChecker.EXPECT().RefreshHealthWithPatchOps(mock.Anything, mock.Anything).
					Return(ConnectionHealthResultWithPatchOps{
						TestResults:  &provisioning.TestResults{Success: true},
						HealthStatus: provisioning.HealthStatus{Healthy: true, Checked: time.Now().UnixMilli()},
					}, nil)
				mockStatusPatcher.EXPECT().Patch(
					mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
				).Return(nil)

				return mockLister, mockHealthChecker, mockStatusPatcher, mockFactory
			},
			conn: &provisioning.Connection{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-conn",
					Namespace: "default",
				},
				Spec: provisioning.ConnectionSpec{
					Type: provisioning.GithubConnectionType,
					GitHub: &provisioning.GitHubConnectionConfig{
						AppID:          "123",
						InstallationID: "456",
					},
				},
				// Mirrors mockLister.conn above: the orphaned reference already made
				// /secure non-empty, so /secure is never actually missing by the time
				// the regenerated token gets written.
				Secure: provisioning.ConnectionSecure{
					Token: common.InlineSecureValue{Name: "orphaned-token"},
				},
			},
			expectError:     false,
			wantSecureToken: "new-token",
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			mockLister, mockHealthChecker, mockStatusPatcher, mockFactory := tt.setupMocks()
			fakeClientset := fakeclientset.NewSimpleClientset(tt.conn)
			cc := &ConnectionController{
				conns:             informer.NewCachedConnectionGetter(mockLister),
				client:            fakeClientset.ProvisioningV0alpha1(),
				healthChecker:     mockHealthChecker,
				statusPatcher:     mockStatusPatcher,
				connectionFactory: mockFactory,
				logger:            logging.DefaultLogger,
				resyncInterval:    5 * time.Minute,
				tracer:            tracing.InitializeTracerForTest(),
			}

			key := tt.conn.Namespace + "/" + tt.conn.Name
			err := cc.process(t.Context(), key)

			if tt.expectError {
				assert.Error(t, err)
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
			} else {
				assert.NoError(t, err)
			}

			if tt.wantSecureToken != "" {
				assert.Equal(t, tt.wantSecureToken, findSecureTokenPatch(t, fakeClientset), "expected /secure/token to be patched on the main resource")
			}

			// Assert all mock expectations were met
			if mockHealthChecker != nil {
				mockHealthChecker.AssertExpectations(t)
			}
			if mockStatusPatcher != nil {
				mockStatusPatcher.AssertExpectations(t)
			}
			if mockFactory != nil {
				mockFactory.AssertExpectations(t)
			}
		})
	}
}

// findSecureTokenPatch returns the "create" value of the /secure/token (or
// /secure) patch op sent through the fake clientset - i.e. the main resource,
// never statusPatcher - during the test.
func findSecureTokenPatch(t *testing.T, fakeClientset *fakeclientset.Clientset) string {
	t.Helper()
	for _, action := range fakeClientset.Actions() {
		patchAction, ok := action.(k8stesting.PatchAction)
		if !ok || patchAction.GetResource().Resource != "connections" {
			continue
		}
		if create := findSecureTokenInPatchBytes(t, patchAction.GetPatch()); create != "" {
			return create
		}
	}
	return ""
}

// findSecureTokenInPatchBytes returns the "create" value of the /secure/token
// (or /secure) op within a raw JSON Patch document.
func findSecureTokenInPatchBytes(t *testing.T, patch []byte) string {
	t.Helper()
	if patch == nil {
		return ""
	}
	var ops []map[string]interface{}
	require.NoError(t, json.Unmarshal(patch, &ops))
	for _, op := range ops {
		path, _ := op["path"].(string)
		value, ok := op["value"].(map[string]interface{})
		if !ok {
			continue
		}
		if path == "/secure/token" {
			if create, ok := value["create"].(string); ok {
				return create
			}
		}
		if path == "/secure" {
			token, ok := value["token"].(map[string]interface{})
			if !ok {
				continue
			}
			if create, ok := token["create"].(string); ok {
				return create
			}
		}
	}
	return ""
}

// TestConnectionController_process_MainPatchFailureBlocksStatusPatch verifies
// that when the main-resource write (e.g. /secure/token) fails, the status
// write (e.g. /status/token's LastUpdated/expiration) is never attempted
// either. Without this, status could claim a token refresh that never
// actually landed, and a later reconcile trusting that timestamp would back
// off instead of retrying the write.
func TestConnectionController_process_MainPatchFailureBlocksStatusPatch(t *testing.T) {
	conn := &provisioning.Connection{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "test-conn",
			Namespace:  "default",
			Generation: 1,
		},
		Status: provisioning.ConnectionStatus{
			ObservedGeneration: 1,
			Health: provisioning.HealthStatus{
				Healthy: true,
				Checked: time.Now().Add(-10 * time.Minute).UnixMilli(),
			},
		},
		Spec: provisioning.ConnectionSpec{
			Type: provisioning.GithubConnectionType,
		},
		Secure: provisioning.ConnectionSecure{
			Token: common.InlineSecureValue{Name: "existing-token"},
		},
	}

	mockLister := &mockConnectionLister{conn: conn}
	mockHealthChecker := NewMockConnectionHealthChecker(t)
	mockFactory := connection.NewMockFactory(t)
	mockConnection := connection.NewMockConnection(t)
	mockTokenConnection := connection.NewMockTokenConnection(t)
	mockConnWithToken := &mockConnectionWithToken{
		Connection:      mockConnection,
		TokenConnection: mockTokenConnection,
	}

	mockHealthChecker.EXPECT().ShouldCheckHealth(mock.Anything).Return(true)
	mockFactory.EXPECT().Build(mock.Anything, mock.Anything).Return(mockConnWithToken, nil)
	// Token expires in 2 minutes - triggers regeneration.
	mockTokenConnection.EXPECT().ValidateToken().Return(time.Now().Add(2*time.Minute), nil)
	mockTokenConnection.EXPECT().GenerateConnectionToken(mock.Anything).Return(&connection.ExpirableSecureValue{Token: "new-token"}, nil)
	mockHealthChecker.EXPECT().RefreshHealthWithPatchOps(mock.Anything, mock.Anything).Return(
		ConnectionHealthResultWithPatchOps{
			TestResults:  &provisioning.TestResults{Success: true},
			HealthStatus: provisioning.HealthStatus{Healthy: true, Checked: time.Now().UnixMilli()},
		}, nil,
	)

	// No expectations set on Patch: the test fails immediately if it's ever
	// called, since the main-resource write below must block it.
	mockStatusPatcher := NewMockConnectionStatusPatcher(t)

	// Not seeded with conn, so the main-resource Patch fails with NotFound.
	fakeClientset := fakeclientset.NewSimpleClientset()

	cc := &ConnectionController{
		conns:             informer.NewCachedConnectionGetter(mockLister),
		client:            fakeClientset.ProvisioningV0alpha1(),
		healthChecker:     mockHealthChecker,
		statusPatcher:     mockStatusPatcher,
		connectionFactory: mockFactory,
		logger:            logging.DefaultLogger,
		resyncInterval:    5 * time.Minute,
		tracer:            tracing.InitializeTracerForTest(),
	}

	err := cc.process(t.Context(), "default/test-conn")
	require.Error(t, err)
	require.Contains(t, err.Error(), "main resource patch operations failed")

	mockHealthChecker.AssertExpectations(t)
	mockFactory.AssertExpectations(t)
	mockStatusPatcher.AssertExpectations(t)
}

func TestConnectionController_process_FieldErrors(t *testing.T) {
	tests := []struct {
		name                string
		testResults         *provisioning.TestResults
		expectedFieldErrors []provisioning.ErrorDetails
		description         string
	}{
		{
			name: "fieldErrors populated when test fails with errors",
			testResults: &provisioning.TestResults{
				Success: false,
				Code:    422,
				Errors: []provisioning.ErrorDetails{
					{
						Type:   metav1.CauseTypeFieldValueRequired,
						Field:  "spec.github.appID",
						Detail: "appID must be specified for GitHub connection",
					},
					{
						Type:   metav1.CauseTypeFieldValueInvalid,
						Field:  "spec.github.installationID",
						Detail: "installationID must be a valid number",
					},
				},
			},
			expectedFieldErrors: []provisioning.ErrorDetails{
				{
					Type:   metav1.CauseTypeFieldValueRequired,
					Field:  "spec.github.appID",
					Detail: "appID must be specified for GitHub connection",
				},
				{
					Type:   metav1.CauseTypeFieldValueInvalid,
					Field:  "spec.github.installationID",
					Detail: "installationID must be a valid number",
				},
			},
			description: "fieldErrors should contain all errors from testResults",
		},
		{
			name: "fieldErrors empty when test succeeds",
			testResults: &provisioning.TestResults{
				Success: true,
				Code:    200,
				Errors:  []provisioning.ErrorDetails{},
			},
			expectedFieldErrors: []provisioning.ErrorDetails{},
			description:         "fieldErrors should be empty when connection test succeeds",
		},
		{
			name: "fieldErrors empty when testResults has nil errors",
			testResults: &provisioning.TestResults{
				Success: true,
				Code:    200,
				Errors:  nil,
			},
			expectedFieldErrors: []provisioning.ErrorDetails{},
			description:         "fieldErrors should be nil when testResults.Errors is nil",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()

			// Create connection
			conn := &provisioning.Connection{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-conn",
					Namespace:  "default",
					Generation: 1,
				},
				Status: provisioning.ConnectionStatus{
					ObservedGeneration: 0, // Will trigger reconciliation
				},
			}

			// Create mock lister
			mockLister := &mockConnectionLister{
				conn: conn,
			}

			// Create mock status patcher
			mockPatcher := mocks.NewConnectionStatusPatcher(t)
			// Use mock.Anything for all arguments since variadic args are expanded
			mockPatcher.On("Patch", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
				Run(func(args mock.Arguments) {
					// Extract variadic patch operations - they come as individual arguments after the first two
					patchOps := []map[string]interface{}{}
					for i := 2; i < len(args); i++ {
						if op, ok := args[i].(map[string]interface{}); ok {
							patchOps = append(patchOps, op)
						}
					}
					// Verify fieldErrors patch operation exists
					fieldErrorsFound := false
					for _, op := range patchOps {
						if path, ok := op["path"].(string); ok && path == "/status/fieldErrors" {
							fieldErrorsFound = true
							value := op["value"]
							if tt.expectedFieldErrors == nil {
								assert.Nil(t, value, "fieldErrors should be nil")
							} else {
								fieldErrors, ok := value.([]provisioning.ErrorDetails)
								require.True(t, ok, "fieldErrors should be []ErrorDetails")
								assert.Equal(t, tt.expectedFieldErrors, fieldErrors, tt.description)
							}
							break
						}
					}
					assert.True(t, fieldErrorsFound, "fieldErrors patch operation should be present")
				}).
				Return(nil)

			// Create mock factory for tester
			mockFactory := connection.NewMockFactory(t)
			mockFactory.EXPECT().Validate(mock.Anything, mock.Anything).Return(nil).Maybe()
			mockConn := connection.NewMockConnection(t)
			mockFactory.EXPECT().Build(mock.Anything, mock.Anything).Return(mockConn, nil).Maybe()
			mockHealthChecker := NewMockConnectionHealthChecker(t)
			mockHealthChecker.EXPECT().ShouldCheckHealth(mock.IsType(&provisioning.Connection{})).Return(true)
			mockHealthChecker.EXPECT().RefreshHealthWithPatchOps(mock.Anything, conn).Return(
				ConnectionHealthResultWithPatchOps{
					TestResults:  tt.testResults,
					HealthStatus: provisioning.HealthStatus{Healthy: false},
				},
				nil,
			)

			// Create controller
			cc := &ConnectionController{
				conns:             informer.NewCachedConnectionGetter(mockLister),
				client:            fakeclientset.NewSimpleClientset().ProvisioningV0alpha1(),
				connectionFactory: mockFactory,
				healthChecker:     mockHealthChecker,
				statusPatcher:     mockPatcher,
				logger:            logging.DefaultLogger,
				tracer:            tracing.InitializeTracerForTest(),
			}

			// Process the connection
			key := "default/test-conn"
			err := cc.process(ctx, key)
			require.NoError(t, err, "process should succeed")
		})
	}
}

// mockConnectionLister implements listers.ConnectionLister for testing
type mockConnectionLister struct {
	conn *provisioning.Connection
}

func (m *mockConnectionLister) Connections(namespace string) listers.ConnectionNamespaceLister {
	return &mockConnectionNamespaceLister{conn: m.conn}
}

func (m *mockConnectionLister) List(selector labels.Selector) (ret []*provisioning.Connection, err error) {
	panic("not implemented")
}

// mockConnectionNamespaceLister implements listers.ConnectionNamespaceLister for testing
type mockConnectionNamespaceLister struct {
	conn *provisioning.Connection
}

func (m *mockConnectionNamespaceLister) List(_ labels.Selector) (ret []*provisioning.Connection, err error) {
	panic("not implemented")
}

func (m *mockConnectionNamespaceLister) Get(name string) (*provisioning.Connection, error) {
	if m.conn != nil && m.conn.Name == name {
		return m.conn, nil
	}
	return nil, apierrors.NewNotFound(schema.GroupResource{Resource: "connections"}, name)
}

// Ensure mockConnectionLister implements listers.ConnectionLister
var _ listers.ConnectionLister = (*mockConnectionLister)(nil)

// Ensure mockConnectionNamespaceLister implements listers.ConnectionNamespaceLister
var _ listers.ConnectionNamespaceLister = (*mockConnectionNamespaceLister)(nil)

// TestConnectionController_generateConnectionToken_ReturnsExpiry verifies the
// generated token's expiration is threaded back so the reconcile can persist it.
func TestConnectionController_generateConnectionToken_ReturnsExpiry(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	cc := &ConnectionController{tokenMetrics: registerConnectionTokenMetrics(reg)}

	exp := time.Now().Add(10 * time.Minute)
	mockConn := connection.NewMockTokenConnection(t)
	mockConn.EXPECT().GenerateConnectionToken(mock.Anything).
		Return(&connection.ExpirableSecureValue{Token: "tok", ExpiresAt: exp}, nil)

	token, expiresAt, ops, err := cc.generateConnectionToken(context.Background(), mockConn)
	require.NoError(t, err)
	assert.Equal(t, common.RawSecureValue("tok"), token)
	assert.True(t, exp.Equal(expiresAt))
	require.Len(t, ops, 1)
}

// TestConnectionController_shouldGenerateToken_ExpiredCounter verifies that the
// expired counter (from the persisted status.token.expiration) is incremented
// only for an already-expired token, mirroring the repository path. A nil
// TokenConnection is fine: with an empty Secure.Token the method records the
// expired state and returns before it touches the connection.
func TestConnectionController_shouldGenerateToken_ExpiredCounter(t *testing.T) {
	resyncInterval := 5 * time.Minute
	lastUpdated := time.Now().Add(-time.Hour).UnixMilli()

	tests := []struct {
		name        string
		expiration  int64 // epoch millis; 0 means non-expiring
		wantExpired float64
	}{
		{"expired", time.Now().Add(-time.Minute).UnixMilli(), 1},
		{"near expiry is not counted as expired", time.Now().Add(30 * time.Second).UnixMilli(), 0},
		{"valid far from expiry", time.Now().Add(2 * time.Hour).UnixMilli(), 0},
		{"non-expiring", 0, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reg := prometheus.NewPedanticRegistry()
			cc := &ConnectionController{
				tokenMetrics:   registerConnectionTokenMetrics(reg),
				resyncInterval: resyncInterval,
			}
			obj := &provisioning.Connection{
				Status: provisioning.ConnectionStatus{
					Token: provisioning.TokenStatus{LastUpdated: lastUpdated, Expiration: tt.expiration},
				},
			}

			cc.shouldGenerateToken(context.Background(), obj, nil)

			assert.Equal(t, tt.wantExpired, counterValue(t, reg, "grafana_provisioning_connection_tokens_expired_total"))
		})
	}
}

// TestConnectionController_shouldGenerateToken_BackfillsExpiredFromLiveExpiry
// verifies that a token persisted before expiration tracking (no
// status.token.expiration) still counts as expired via the live validated
// expiry, so pre-upgrade tokens are covered during rollout.
func TestConnectionController_shouldGenerateToken_BackfillsExpiredFromLiveExpiry(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	cc := &ConnectionController{
		tokenMetrics:   registerConnectionTokenMetrics(reg),
		resyncInterval: 5 * time.Minute,
	}
	obj := &provisioning.Connection{
		Status: provisioning.ConnectionStatus{
			// Pre-upgrade token: LastUpdated set, but no persisted expiration.
			Token: provisioning.TokenStatus{LastUpdated: time.Now().Add(-time.Hour).UnixMilli(), Expiration: 0},
		},
		Secure: provisioning.ConnectionSecure{Token: common.InlineSecureValue{Create: "existing-token"}},
	}
	mockConn := connection.NewMockTokenConnection(t)
	mockConn.EXPECT().ValidateToken().Return(time.Now().Add(-time.Minute), nil)

	cc.shouldGenerateToken(context.Background(), obj, mockConn)

	assert.Equal(t, 1.0, counterValue(t, reg, "grafana_provisioning_connection_tokens_expired_total"))
}

func newConnectionControllerForQueueTest(t *testing.T) (*ConnectionController, *prometheus.Registry) {
	t.Helper()
	reg := prometheus.NewPedanticRegistry()
	cc := NewConnectionController(nil, nil, nil, nil, nil, time.Minute, 5*time.Second, reg, nil, false)
	t.Cleanup(cc.queue.ShutDown)
	return cc, reg
}

// Advance only the queue clock: Kubernetes' global error backoff keeps a wall
// clock timestamp that cannot be used inside a synctest bubble.
func useFakeConnectionQueueClock(t *testing.T, cc *ConnectionController) *testingclock.FakeClock {
	t.Helper()
	cc.queue.ShutDown()
	clock := testingclock.NewFakeClock(time.Now())
	cc.queue = workqueue.NewTypedRateLimitingQueueWithConfig(
		workqueue.DefaultTypedControllerRateLimiter[string](),
		workqueue.TypedRateLimitingQueueConfig[string]{Clock: clock},
	)
	t.Cleanup(cc.queue.ShutDown)
	return clock
}

func TestConnectionController_DeduplicatesEnqueueBeforeProcessing(t *testing.T) {
	cc, _ := newConnectionControllerForQueueTest(t)
	var processedKeys []string
	cc.processFn = func(_ context.Context, key string) error {
		processedKeys = append(processedKeys, key)
		return nil
	}

	conns := []*provisioning.Connection{
		{ObjectMeta: metav1.ObjectMeta{Namespace: "ns-a", Name: "conn-a"}},
		{ObjectMeta: metav1.ObjectMeta{Namespace: "ns-a", Name: "conn-b"}},
		{ObjectMeta: metav1.ObjectMeta{Namespace: "ns-b", Name: "conn-a"}},
	}
	handler := cc.EventHandler()
	for _, conn := range conns {
		handler.AddFunc(conn, true)
		for range 5 {
			handler.UpdateFunc(conn, conn.DeepCopy())
		}
	}

	require.Equal(t, len(conns), cc.queue.Len())
	for range conns {
		require.Positive(t, cc.queue.Len())
		require.True(t, cc.processNextWorkItem(t.Context()))
	}
	assert.ElementsMatch(t, []string{"ns-a/conn-a", "ns-a/conn-b", "ns-b/conn-a"}, processedKeys)
	assert.Zero(t, cc.queue.Len())
	assert.Empty(t, cc.triggers)
}

func TestConnectionController_DeduplicatesEnqueueWhileProcessing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cc, _ := newConnectionControllerForQueueTest(t)
		ctx, cancel := context.WithCancel(t.Context())
		t.Cleanup(cancel)
		release := make(chan struct{})
		releaseFirst := sync.OnceFunc(func() { close(release) })
		t.Cleanup(releaseFirst)
		var processCount, otherProcessCount atomic.Int32
		cc.processFn = func(_ context.Context, key string) error {
			switch key {
			case "ns-a/conn":
				if processCount.Add(1) == 1 {
					<-release
				}
			case "ns-b/conn":
				otherProcessCount.Add(1)
			default:
				t.Errorf("unexpected connection key %q", key)
			}
			return nil
		}

		conn := &provisioning.Connection{ObjectMeta: metav1.ObjectMeta{Namespace: "ns-a", Name: "conn"}}
		handler := cc.EventHandler()
		handler.AddFunc(conn, true)
		runDone := make(chan struct{})
		go func() {
			cc.Run(ctx, 2, func() {}, func() {})
			close(runDone)
		}()
		synctest.Wait()
		require.Equal(t, int32(1), processCount.Load())

		for range 5 {
			handler.UpdateFunc(conn, conn.DeepCopy())
		}
		handler.AddFunc(&provisioning.Connection{ObjectMeta: metav1.ObjectMeta{Namespace: "ns-b", Name: "conn"}}, false)
		synctest.Wait()
		assert.Equal(t, int32(1), processCount.Load(), "the in-flight key must not be processed by another worker")
		assert.Equal(t, int32(1), otherProcessCount.Load(), "another connection can be processed concurrently")

		releaseFirst()
		synctest.Wait()
		cancel()
		<-runDone
		assert.Equal(t, int32(2), processCount.Load(), "updates must coalesce into one additional reconciliation")
		assert.Equal(t, int32(1), otherProcessCount.Load())
		assert.Zero(t, cc.queue.Len())
		assert.Empty(t, cc.triggers)
	})
}

func TestConnectionController_RetriesAndClearsState(t *testing.T) {
	unavailable := apierrors.NewServiceUnavailable("temporarily unavailable")
	terminal := errors.New("invalid connection")
	for _, tt := range []struct {
		name     string
		results  []error
		internal bool
	}{
		{name: "service unavailable exhausts attempts", results: []error{unavailable, unavailable, unavailable}},
		{name: "non-retryable error", results: []error{terminal}},
		{name: "retry succeeds", results: []error{unavailable, nil}},
		{name: "retry fails permanently", results: []error{unavailable, terminal}},
		{name: "internal retry has no attribution", results: []error{unavailable, nil}, internal: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cc, reg := newConnectionControllerForQueueTest(t)
			clock := useFakeConnectionQueueClock(t, cc)
			const key = "ns/conn"
			conn := &provisioning.Connection{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "conn", ResourceVersion: "5"}}
			processCount := 0
			cc.processFn = func(_ context.Context, gotKey string) error {
				require.Equal(t, key, gotKey)
				require.Less(t, processCount, len(tt.results), "unexpected retry")
				assert.Equal(t, processCount, cc.queue.NumRequeues(key))
				err := tt.results[processCount]
				processCount++
				return err
			}
			wantTrigger := "initial"
			if tt.internal {
				cc.queue.Add(key)
				wantTrigger = ""
			} else {
				cc.EventHandler().AddFunc(conn, true)
			}

			for i := range tt.results {
				require.True(t, cc.processNextWorkItem(t.Context()))
				if i < len(tt.results)-1 {
					assert.Equal(t, i+1, cc.queue.NumRequeues(key))
					if tt.internal {
						assert.Empty(t, cc.triggers)
					} else {
						assert.Equal(t, wantTrigger, string(cc.triggers[key]))
					}
					clock.Step(time.Second)
				}
			}
			assert.Equal(t, len(tt.results), processCount)
			assert.Zero(t, cc.queue.NumRequeues(key))
			assert.Empty(t, cc.triggers)
			assertOnlyProcessedTrigger(t, reg, "connections", wantTrigger)
			assert.Zero(t, cc.queue.Len())

			cc.processFn = func(_ context.Context, gotKey string) error {
				assert.Equal(t, key, gotKey)
				assert.Zero(t, cc.queue.NumRequeues(key), "a new event starts with a fresh retry budget")
				return nil
			}
			cc.EventHandler().AddFunc(conn, false)
			require.True(t, cc.processNextWorkItem(t.Context()))
			assert.Equal(t, 1.0, processedCounterValue(t, reg, "connections", "live"))
			assert.Zero(t, cc.queue.NumRequeues(key))
			assert.Zero(t, cc.queue.Len())
			assert.Empty(t, cc.triggers)
		})
	}
}

func TestConnectionController_DirtyRedeliveryKeepsTrigger(t *testing.T) {
	for _, tt := range []struct {
		name       string
		firstError error
	}{
		{name: "success"},
		{name: "terminal error", firstError: errors.New("invalid connection")},
		{name: "retryable error", firstError: apierrors.NewServiceUnavailable("temporarily unavailable")},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cc, reg := newConnectionControllerForQueueTest(t)
			clock := useFakeConnectionQueueClock(t, cc)
			conn := &provisioning.Connection{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "conn", ResourceVersion: "5"}}
			updated := conn.DeepCopy()
			updated.ResourceVersion = "6"
			processCount := 0
			cc.processFn = func(context.Context, string) error {
				processCount++
				if processCount == 1 {
					cc.EventHandler().UpdateFunc(conn, updated)
					return tt.firstError
				}
				return nil
			}

			cc.EventHandler().AddFunc(conn, true)
			require.True(t, cc.processNextWorkItem(t.Context()))
			require.Equal(t, 1, cc.queue.Len())
			assert.Equal(t, "live", string(cc.triggers["ns/conn"]), "completion and retry must preserve the newer event")
			require.True(t, cc.processNextWorkItem(t.Context()))

			if apierrors.IsServiceUnavailable(tt.firstError) {
				clock.Step(time.Second)
				// The dirty delivery consumes the retry budget. The delayed retry
				// still arrives, but has no informer event to count after success.
				require.True(t, cc.processNextWorkItem(t.Context()))
				assert.Equal(t, 3, processCount)
				assertOnlyProcessedTrigger(t, reg, "connections", "initial")
			} else {
				assert.Equal(t, 2, processCount)
				assert.Equal(t, 1.0, processedCounterValue(t, reg, "connections", "initial"))
				assert.Equal(t, 1.0, processedCounterValue(t, reg, "connections", "live"))
				assert.Zero(t, processedCounterValue(t, reg, "connections", "relist"))
			}
			assert.Zero(t, cc.queue.NumRequeues("ns/conn"))
			assert.Zero(t, cc.queue.Len())
			assert.Empty(t, cc.triggers)
		})
	}
}

func TestConnectionController_RecentlyWrittenTokenReschedulesKey(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cc, reg := newConnectionControllerForQueueTest(t)
		conn := &provisioning.Connection{
			ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "conn", ResourceVersion: "5"},
			Secure:     provisioning.ConnectionSecure{Token: common.InlineSecureValue{Name: "recent-token"}},
			Status:     provisioning.ConnectionStatus{Token: provisioning.TokenStatus{LastUpdated: time.Now().UnixMilli()}},
		}
		original := conn.DeepCopy()
		cc.conns = informer.NewCachedConnectionGetter(&mockConnectionLister{conn: conn})
		cc.tracer = tracing.NewNoopTracerService()
		healthChecker := NewMockConnectionHealthChecker(t)
		healthChecker.EXPECT().ShouldCheckHealth(conn).Return(false).Twice()
		cc.healthChecker = healthChecker
		factory := connection.NewMockFactory(t)
		factory.EXPECT().Build(mock.Anything, conn).
			Return(nil, fmt.Errorf("unable to decrypt token: %w", connection.ErrTokenNotFound)).Once()
		factory.EXPECT().Build(mock.Anything, conn).Return(connection.NewMockConnection(t), nil).Once()
		cc.connectionFactory = factory

		cc.EventHandler().AddFunc(conn, true)
		require.True(t, cc.processNextWorkItem(t.Context()))
		synctest.Wait()
		require.Zero(t, cc.queue.Len())

		time.Sleep(tokenWriteRetryDelay - time.Nanosecond)
		synctest.Wait()
		require.Zero(t, cc.queue.Len(), "token retry must wait for the configured delay")
		time.Sleep(time.Nanosecond)
		synctest.Wait()
		require.Equal(t, 1, cc.queue.Len())
		require.True(t, cc.processNextWorkItem(t.Context()))
		assert.Zero(t, cc.queue.Len())
		assert.Zero(t, cc.queue.NumRequeues("ns/conn"))
		assert.Empty(t, cc.triggers)
		assertOnlyProcessedTrigger(t, reg, "connections", "initial")
		assert.Equal(t, original, conn, "the recent token must not be cleared or regenerated")
	})
}

func TestConnectionController_Run_DrainWaitsForInFlight(t *testing.T) {
	processCh := make(chan struct{})
	processingStarted := make(chan struct{})
	var processed atomic.Bool

	cc := &ConnectionController{
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{
				Name: "test-connection-drain",
			},
		),
		logger:       logging.DefaultLogger.With("logger", "test"),
		drainTimeout: 5 * time.Second,
	}

	cc.processFn = func(ctx context.Context, key string) error {
		close(processingStarted)
		<-processCh
		processed.Store(true)
		return nil
	}

	cc.queue.Add("test/conn")

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() {
		cc.Run(ctx, 1, func() {}, func() {})
		close(runDone)
	}()

	// Wait until the worker has actually picked up the item
	<-processingStarted

	// Cancel context to trigger shutdown
	cancel()

	// Run should NOT return yet because item is still being processed
	select {
	case <-runDone:
		t.Fatal("Run returned before in-flight item completed")
	case <-time.After(200 * time.Millisecond):
		// Expected: still waiting for drain
	}

	// Complete the in-flight item
	close(processCh)

	// Now Run should return
	select {
	case <-runDone:
		assert.True(t, processed.Load(), "item should have been fully processed")
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after in-flight item completed")
	}
}

func TestConnectionController_Run_DrainTimeoutForcesShutdown(t *testing.T) {
	processingStarted := make(chan struct{})

	cc := &ConnectionController{
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{
				Name: "test-connection-drain-timeout",
			},
		),
		logger:       logging.DefaultLogger.With("logger", "test"),
		drainTimeout: 200 * time.Millisecond,
	}

	// processFn blocks forever to simulate a stuck reconciliation
	cc.processFn = func(ctx context.Context, key string) error {
		close(processingStarted)
		select {}
	}

	cc.queue.Add("test/stuck")

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() {
		cc.Run(ctx, 1, func() {}, func() {})
		close(runDone)
	}()

	// Wait until the worker has actually picked up the item
	<-processingStarted
	cancel()

	// Run should return within the drain timeout + some buffer
	select {
	case <-runDone:
		// Expected: drain timeout kicked in
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after drain timeout")
	}
}

func TestConnectionController_Run_OnShutdownCalledBeforeDrain(t *testing.T) {
	var shutdownCalledAt time.Time
	var runReturnedAt time.Time
	processCh := make(chan struct{})
	processingStarted := make(chan struct{})
	shutdownCalled := make(chan struct{})

	cc := &ConnectionController{
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{
				Name: "test-connection-shutdown-ordering",
			},
		),
		logger:       logging.DefaultLogger.With("logger", "test"),
		drainTimeout: 5 * time.Second,
	}

	cc.processFn = func(ctx context.Context, key string) error {
		close(processingStarted)
		<-processCh
		return nil
	}

	cc.queue.Add("test/ordering")

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() {
		cc.Run(ctx, 1, func() {}, func() {
			shutdownCalledAt = time.Now()
			close(shutdownCalled)
		})
		runReturnedAt = time.Now()
		close(runDone)
	}()

	<-processingStarted
	cancel()

	// Wait for onShutdown to be called before releasing the drain
	<-shutdownCalled
	close(processCh)

	select {
	case <-runDone:
		require.False(t, shutdownCalledAt.IsZero(), "onShutdown should have been called")
		require.False(t, runReturnedAt.IsZero(), "Run should have returned")
		assert.True(t, shutdownCalledAt.Before(runReturnedAt),
			"onShutdown should be called before Run returns (drain completes)")
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return")
	}
}

// TestConnectionController_RecordsProcessingByTrigger verifies the connection
// controller counts the start of each reconcile under resource="connections".
func TestConnectionController_RecordsProcessingByTrigger(t *testing.T) {
	conn := func(rv string) *provisioning.Connection {
		return &provisioning.Connection{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "conn", ResourceVersion: rv}}
	}
	tests := []struct {
		name        string
		natsBacked  bool
		feed        func(h cache.ResourceEventHandlerDetailedFuncs)
		wantTrigger string
	}{
		{
			name:        "apiserver live add",
			feed:        func(h cache.ResourceEventHandlerDetailedFuncs) { h.AddFunc(conn("5"), false) },
			wantTrigger: "live",
		},
		{
			name:        "initial list add",
			feed:        func(h cache.ResourceEventHandlerDetailedFuncs) { h.AddFunc(conn("5"), true) },
			wantTrigger: "initial",
		},
		{
			name:        "resync update is relist",
			feed:        func(h cache.ResourceEventHandlerDetailedFuncs) { h.UpdateFunc(conn("5"), conn("5")) },
			wantTrigger: "relist",
		},
		{
			name:        "apiserver live update",
			feed:        func(h cache.ResourceEventHandlerDetailedFuncs) { h.UpdateFunc(conn("5"), conn("6")) },
			wantTrigger: "live",
		},
		{
			name:        "nats relist add",
			natsBacked:  true,
			feed:        func(h cache.ResourceEventHandlerDetailedFuncs) { h.AddFunc(conn("5"), false) },
			wantTrigger: "relist",
		},
		{
			name:        "nats live add",
			natsBacked:  true,
			feed:        func(h cache.ResourceEventHandlerDetailedFuncs) { h.AddFunc(conn(""), false) },
			wantTrigger: "live",
		},
		{
			name: "coalesced updates keep initial trigger",
			feed: func(h cache.ResourceEventHandlerDetailedFuncs) {
				h.AddFunc(conn("5"), true)
				h.UpdateFunc(conn("5"), conn("6"))
				h.UpdateFunc(conn("6"), conn("6"))
			},
			wantTrigger: "initial",
		},
		{
			name: "coalesced relist keeps live trigger",
			feed: func(h cache.ResourceEventHandlerDetailedFuncs) {
				h.AddFunc(conn("5"), false)
				h.UpdateFunc(conn("5"), conn("5"))
			},
			wantTrigger: "live",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reg := prometheus.NewPedanticRegistry()
			processedDone := make(chan struct{})

			cc := &ConnectionController{
				queue: workqueue.NewTypedRateLimitingQueueWithConfig(
					workqueue.DefaultTypedControllerRateLimiter[string](),
					workqueue.TypedRateLimitingQueueConfig[string]{Name: "test-processed"},
				),
				logger:       logging.DefaultLogger.With("logger", "test"),
				drainTimeout: 5 * time.Second,
				processed:    usinformer.NewProcessedMetrics(reg, "connections", tt.natsBacked),
				processFn: func(context.Context, string) error {
					close(processedDone)
					return nil
				},
			}

			tt.feed(cc.EventHandler())

			ctx, cancel := context.WithCancel(context.Background())
			runDone := make(chan struct{})
			go func() {
				cc.Run(ctx, 1, func() {}, func() {})
				close(runDone)
			}()

			select {
			case <-processedDone:
			case <-time.After(5 * time.Second):
				t.Fatal("key was not processed")
			}
			cancel()
			<-runDone

			assertOnlyProcessedTrigger(t, reg, "connections", tt.wantTrigger)
		})
	}
}

// TestConnectionController_WorkerQueueWaitHistogram verifies the queue-wait histogram
// records one observation each time a worker picks a key up off the queue.
func TestConnectionController_WorkerQueueWaitHistogram(t *testing.T) {
	const metricName = "grafana_provisioning_connection_worker_queue_wait_seconds"

	reg := prometheus.NewRegistry()
	cc := NewConnectionController(
		nil, nil, nil, nil, nil,
		time.Minute, 30*time.Second,
		reg,
		nil,
		false,
	)

	require.Equal(t, uint64(0), histogramSampleCountByName(t, reg, metricName))

	cc.queue.Add("ns/conn-a")
	cc.queue.Add("ns/conn-a")
	cc.queue.Add("ns/conn-b")

	key, _ := cc.queue.Get()
	cc.queue.Done(key)
	require.Equal(t, uint64(1), histogramSampleCountByName(t, reg, metricName))

	key, _ = cc.queue.Get()
	cc.queue.Done(key)
	require.Equal(t, uint64(2), histogramSampleCountByName(t, reg, metricName))
}

// TestConnectionController_WorkerQueueSizeGauge verifies the worker-queue-size gauge
// reports the live depth of the replica's local work queue at scrape time.
func TestConnectionController_WorkerQueueSizeGauge(t *testing.T) {
	const metricName = "grafana_provisioning_connection_worker_queue_size"

	reg := prometheus.NewRegistry()
	cc := NewConnectionController(
		nil, nil, nil, nil, nil,
		time.Minute, 30*time.Second,
		reg,
		nil,
		false,
	)

	require.Equal(t, 0.0, gaugeValueByName(t, reg, metricName))

	cc.queue.Add("ns/conn-a")
	cc.queue.Add("ns/conn-a")
	cc.queue.Add("ns/conn-b")
	require.Equal(t, 2.0, gaugeValueByName(t, reg, metricName))

	// Get removes the key from the queue (Len drops); Done clears it from processing.
	key, _ := cc.queue.Get()
	cc.queue.Done(key)
	require.Equal(t, 1.0, gaugeValueByName(t, reg, metricName))
}
