package provisioning

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	provisioning "github.com/grafana/grafana/apps/provisioning/pkg/apis/provisioning/v0alpha1"
	"github.com/grafana/grafana/pkg/tests/apis/provisioning/common"
)

// Test that status patches still succeed even if the mutation would fail.
// Previously, we ran mutation and validation hooks on every update (both on the resource, or subresource - including)
// the status. If there was an error on the mutation hook, this would prevent status updates.
func TestIntegrationProvisioning_StatusPatch_SurvivesInvalidSpecMutation(t *testing.T) {
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
	waitForConditionTypes(t, helper, repoName,
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
