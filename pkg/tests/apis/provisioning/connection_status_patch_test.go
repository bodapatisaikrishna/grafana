package provisioning

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"

	provisioning "github.com/grafana/grafana/apps/provisioning/pkg/apis/provisioning/v0alpha1"
	"github.com/grafana/grafana/pkg/tests/apis/provisioning/common"
)

// Test that status patches still succeed even if the mutation would fail.
// Previously, we ran mutation and validation hooks on every update (both on the resource, or subresource - including)
// the status. If there was an error on the mutation hook, this would prevent status updates.
func TestIntegrationProvisioning_ConnectionStatusPatch_SurvivesInvalidSpecMutation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	helper := sharedHelper(t)

	const connName = "conn-status-patch-survives-bad-spec"
	connObj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "provisioning.grafana.app/v0alpha1",
		"kind":       "Connection",
		"metadata": map[string]any{
			"name":      connName,
			"namespace": "default",
		},
		"spec": map[string]any{
			"title": "Status patch survives bad spec",
			"type":  string(provisioning.GithubConnectionType),
			"github": map[string]any{
				"appID":          "123456",
				"installationID": "454545",
			},
		},
		"secure": map[string]any{
			"privateKey": map[string]any{
				"create": common.TestGithubPrivateKeyBase64(),
			},
		},
	}}

	_, err := helper.Connections.Resource.Create(t.Context(), connObj, metav1.CreateOptions{})
	require.NoError(t, err, "failed to create connection")

	// Sanity check: this exact appID value is rejected by admission when sent
	// as a plain spec update against the main resource (no subresource) - it's
	// the same class of validation failure the combined patch below carries
	// into the status subresource request.
	invalidAppIDPatch := mustMarshalJSONPatch(t, []map[string]any{
		{"op": "replace", "path": "/spec/github/appID", "value": ""},
	})
	_, err = helper.Connections.Resource.Patch(t.Context(), connName, types.JSONPatchType, invalidAppIDPatch, metav1.PatchOptions{})
	require.Error(t, err, "a spec update clearing the GitHub appID should be rejected by admission")
	require.Contains(t, err.Error(), "appID must be specified")

	// A single PATCH against the status subresource that carries both the same
	// invalid appID op AND a routine status write must still succeed in full:
	// the subresource guard means spec validation never runs for this request.
	health := provisioning.HealthStatus{
		Healthy: true,
		Checked: time.Now().UnixMilli(),
	}
	combinedPatch := mustMarshalJSONPatch(t, []map[string]any{
		{"op": "replace", "path": "/spec/github/appID", "value": ""},
		{"op": "replace", "path": "/status/health", "value": health},
	})

	_, err = helper.Connections.Resource.Patch(t.Context(), connName, types.JSONPatchType, combinedPatch, metav1.PatchOptions{}, "status")
	require.NoError(t, err, "status subresource patch should succeed even though it carries a spec change that would fail full validation")

	updated, err := helper.Connections.Resource.Get(t.Context(), connName, metav1.GetOptions{})
	require.NoError(t, err, "failed to get connection after status patch")
	conn := common.MustFromUnstructured[provisioning.Connection](t, updated)

	require.True(t, conn.Status.Health.Healthy, "status.health should have been written by the combined patch")
	require.Equal(t, health.Checked, conn.Status.Health.Checked)
}
