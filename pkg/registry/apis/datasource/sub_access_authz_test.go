package datasource

import (
	"context"
	"testing"

	authlib "github.com/grafana/authlib/types"
	"github.com/stretchr/testify/require"
	requestctx "k8s.io/apiserver/pkg/endpoints/request"

	"github.com/grafana/grafana/pkg/apimachinery/identity"
	"github.com/grafana/grafana/pkg/plugins"
	"github.com/grafana/grafana/pkg/services/datasources"
)

func TestSubAccessAuthzREST_GetAccessInfo(t *testing.T) {
	accessClient := &recordingDatasourceAccessClient{
		response: authlib.BatchCheckResponse{Results: map[string]authlib.BatchCheckResult{
			"read":     {Allowed: true},
			"write":    {Allowed: true},
			"delete":   {Allowed: false},
			"query":    {Allowed: true},
			"getperms": {Allowed: true},
			"setperms": {Allowed: false},
		}},
	}
	builder, err := NewDataSourceAPIBuilder("test.datasource.grafana.app", plugins.JSONData{ID: "test"}, nil, nil, nil, accessClient, nil, DataSourceAPIBuilderConfig{}, nil, nil)
	require.NoError(t, err)

	ctx := requestctx.WithNamespace(context.Background(), "stacks-123")
	user := &identity.StaticRequester{OrgID: 1, UserID: 7, UserUID: "user-7"}
	ctx = identity.WithRequester(ctx, user)

	got, err := (&subAccessAuthzREST{builder: builder}).getAccessInfo(ctx, "datasource-uid")
	require.NoError(t, err)
	require.Equal(t, map[string]bool{
		datasources.ActionRead:            true,
		datasources.ActionWrite:           true,
		datasources.ActionQuery:           true,
		datasources.ActionPermissionsRead: true,
	}, got.Permissions)

	require.Same(t, user, accessClient.user)
	require.Equal(t, "stacks-123", accessClient.request.Namespace)
	require.Len(t, accessClient.request.Checks, len(datasourceAccessChecks))
	for _, check := range accessClient.request.Checks {
		require.Equal(t, "test.datasource.grafana.app", check.Group)
		require.Equal(t, "datasources", check.Resource)
		require.Equal(t, "datasource-uid", check.Name)
	}
}

type recordingDatasourceAccessClient struct {
	request  authlib.BatchCheckRequest
	user     authlib.AuthInfo
	response authlib.BatchCheckResponse
}

func (c *recordingDatasourceAccessClient) Check(context.Context, authlib.AuthInfo, authlib.CheckRequest, string) (authlib.CheckResponse, error) {
	return authlib.CheckResponse{}, nil
}

func (c *recordingDatasourceAccessClient) Compile(context.Context, authlib.AuthInfo, authlib.ListRequest) (authlib.ItemChecker, authlib.Zookie, error) {
	return nil, nil, nil
}

func (c *recordingDatasourceAccessClient) BatchCheck(_ context.Context, user authlib.AuthInfo, request authlib.BatchCheckRequest) (authlib.BatchCheckResponse, error) {
	c.user = user
	c.request = request
	return c.response, nil
}
