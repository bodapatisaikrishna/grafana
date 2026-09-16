package provisioning

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"

	provisioning "github.com/grafana/grafana/apps/provisioning/pkg/apis/provisioning/v0alpha1"
	"github.com/grafana/grafana/pkg/tests/apis"
	"github.com/grafana/grafana/pkg/tests/apis/provisioning/common"
)

// repositoryConditions and connectionConditions adapt Repository/Connection to the
// generic condition helpers below.
func repositoryConditions(r *provisioning.Repository) []metav1.Condition { return r.Status.Conditions }
func connectionConditions(c *provisioning.Connection) []metav1.Condition { return c.Status.Conditions }

// readConditions fetches the named resource of type T and returns its status
// conditions via getConditions. Used both directly and from
// waitForConditionTypes; kept error-returning (rather than require-based) so
// it's safe to call from retry callbacks and worker goroutines.
func readConditions[T any](ctx context.Context, resource *apis.K8sResourceClient, name string, getConditions func(*T) []metav1.Condition) ([]metav1.Condition, error) {
	obj, err := resource.Resource.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	item, err := common.FromUnstructured[T](obj)
	if err != nil {
		return nil, err
	}
	return getConditions(item), nil
}

// waitForConditionTypes blocks until every named condition type is present on
// the resource. Used to make sure a controller has written its initial
// conditions (and so the conditions array exists) before a test starts
// mutating status.conditions.
func waitForConditionTypes[T any](t *testing.T, resource *apis.K8sResourceClient, name string, getConditions func(*T) []metav1.Condition, conditionTypes ...string) {
	t.Helper()
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		cur, err := readConditions(t.Context(), resource, name, getConditions)
		if !assert.NoError(c, err) {
			return
		}
		have := conditionTypeSet(cur)
		for _, typ := range conditionTypes {
			assert.Contains(c, have, typ, "%q still missing condition %q", name, typ)
		}
	}, common.WaitTimeoutDefault, common.WaitIntervalDefault,
		"%q should have conditions %v", name, conditionTypes)
}

// Test that status patches still succeed even if the mutation would fail.
// Previously, we ran mutation and validation hooks on every update (both on the resource, or subresource - including)
// the status. If there was an error on the mutation hook, this would prevent status updates.
//
// This appends a unique condition type the connection controller never touches, rather than
// writing status.health directly - the controller owns that field and reconciles it
// concurrently (via its own health check against the connection), so asserting an exact
// value there would race the controller's own writes.
func TestIntegrationProvisioning_RepositoryStatusPatch_SurvivesInvalidSpecMutation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	helper := sharedHelper(t)

	const repoName = "git-status-patch-survives-bad-spec"
	repoObj := helper.RenderObject(t, common.TestdataPath("git.json.tmpl"), map[string]any{
		"Name":          repoName,
		"Branch":        "main",
		"WorkflowsJSON": `[]`,
	})
	_, err := helper.Repositories.Resource.Create(t.Context(), repoObj, metav1.CreateOptions{})
	require.NoError(t, err, "failed to create git repository")

	// Wait for the controller to populate the initial conditions so the
	// status.conditions array exists before we append to it below.
	waitForConditionTypes(t, helper.Repositories, repoName, repositoryConditions,
		provisioning.ConditionTypeReady,
		provisioning.ConditionTypeNamespaceQuota,
	)

	// Sanity check: this exact branch value is rejected by admission when sent
	// as a plain spec update against the main resource (no subresource) - it's
	// the same class of validation failure the combined patch below carries
	// into the status subresource request.
	invalidBranchPatch := mustMarshalJSONPatch(t, []map[string]any{
		{"op": "replace", "path": "/spec/git/branch", "value": "bad//branch"},
	})
	_, err = helper.Repositories.Resource.Patch(t.Context(), repoName, types.JSONPatchType, invalidBranchPatch, metav1.PatchOptions{})
	require.Error(t, err, "a spec update with an invalid git branch name should be rejected by admission")
	require.Contains(t, err.Error(), "invalid branch name")

	// A single PATCH against the status subresource that carries both the same
	// invalid branch op AND a routine status write must still succeed in full:
	// the subresource guard means spec validation never runs for this request.
	condition := metav1.Condition{
		Type:               "StatusPatchSurvivesBadSpecTest",
		Status:             metav1.ConditionTrue,
		Reason:             "Test",
		Message:            "status write should succeed even though this same patch carries an invalid spec change",
		LastTransitionTime: metav1.NewTime(time.Now()),
	}
	combinedPatch := mustMarshalJSONPatch(t, []map[string]any{
		{"op": "replace", "path": "/spec/git/branch", "value": "bad//branch"},
		{"op": "add", "path": "/status/conditions/-", "value": condition},
	})

	_, err = helper.Repositories.Resource.Patch(t.Context(), repoName, types.JSONPatchType, combinedPatch, metav1.PatchOptions{}, "status")
	require.NoError(t, err, "status subresource patch should succeed even though it carries a spec change that would fail full validation")

	updated, err := helper.Repositories.Resource.Get(t.Context(), repoName, metav1.GetOptions{})
	require.NoError(t, err, "failed to get repository after status patch")
	repo := common.MustFromUnstructured[provisioning.Repository](t, updated)

	found := common.FindCondition(repo.Status.Conditions, condition.Type)
	require.NotNil(t, found, "status condition should have been written by the combined patch")
	require.Equal(t, metav1.ConditionTrue, found.Status)
}

// Test that status patches still succeed even if the mutation would fail.
// Previously, we ran mutation and validation hooks on every update (both on the resource, or subresource - including)
// the status. If there was an error on the mutation hook, this would prevent status updates.
//
// This appends a unique condition type the connection controller never touches, rather than
// writing status.health directly - the controller owns that field and reconciles it
// concurrently (via its own health check against the connection), so asserting an exact
// value there would race the controller's own writes.
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

	// Wait for the controller to populate the initial Ready condition so the
	// status.conditions array exists before we append to it below.
	waitForConditionTypes(t, helper.Connections, connName, connectionConditions, provisioning.ConditionTypeReady)

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
	condition := metav1.Condition{
		Type:               "ConnectionStatusPatchSurvivesBadSpecTest",
		Status:             metav1.ConditionTrue,
		Reason:             "Test",
		Message:            "status write should succeed even though this same patch carries an invalid spec change",
		LastTransitionTime: metav1.NewTime(time.Now()),
	}
	combinedPatch := mustMarshalJSONPatch(t, []map[string]any{
		{"op": "replace", "path": "/spec/github/appID", "value": ""},
		{"op": "add", "path": "/status/conditions/-", "value": condition},
	})

	_, err = helper.Connections.Resource.Patch(t.Context(), connName, types.JSONPatchType, combinedPatch, metav1.PatchOptions{}, "status")
	require.NoError(t, err, "status subresource patch should succeed even though it carries a spec change that would fail full validation")

	updated, err := helper.Connections.Resource.Get(t.Context(), connName, metav1.GetOptions{})
	require.NoError(t, err, "failed to get connection after status patch")
	conn := common.MustFromUnstructured[provisioning.Connection](t, updated)

	found := common.FindCondition(conn.Status.Conditions, condition.Type)
	require.NotNil(t, found, "status condition should have been written by the combined patch")
	require.Equal(t, metav1.ConditionTrue, found.Status)
}
