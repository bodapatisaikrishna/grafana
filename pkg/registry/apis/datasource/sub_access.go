package datasource

import (
	"context"
	"net/http"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/registry/rest"

	authlib "github.com/grafana/authlib/types"
	"github.com/grafana/grafana/pkg/apimachinery/identity"
	"github.com/grafana/grafana/pkg/apimachinery/utils"
	datasourceV0alpha1 "github.com/grafana/grafana/pkg/apis/datasource/v0alpha1"
	"github.com/grafana/grafana/pkg/services/accesscontrol"
	apirequest "github.com/grafana/grafana/pkg/services/apiserver/endpoints/request"
	"github.com/grafana/grafana/pkg/services/contexthandler"
	"github.com/grafana/grafana/pkg/services/datasources"
)

type subAccessREST struct {
	builder *DataSourceAPIBuilder
}

// subAccessAuthzREST is the multi-tenant form of the datasource access
// endpoint. The Grafana request context in an MT apiserver does not hydrate
// legacy RBAC permissions, so access metadata must be assembled from AuthZ
// checks instead.
type subAccessAuthzREST struct {
	builder *DataSourceAPIBuilder
}

var _ = rest.Connecter(&subAccessAuthzREST{})

type datasourceAccessCheck struct {
	correlationID string
	action        string
	verb          string
	subresource   string
}

// These are the datasource permissions consumed by the configuration UI. The
// correlation IDs are deliberately short because AuthZ validates their format.
var datasourceAccessChecks = []datasourceAccessCheck{
	{correlationID: "read", action: datasources.ActionRead, verb: utils.VerbGet},
	{correlationID: "write", action: datasources.ActionWrite, verb: utils.VerbUpdate},
	{correlationID: "delete", action: datasources.ActionDelete, verb: utils.VerbDelete},
	{correlationID: "query", action: datasources.ActionQuery, verb: utils.VerbCreate, subresource: "query"},
	{correlationID: "getperms", action: datasources.ActionPermissionsRead, verb: utils.VerbGetPermissions},
	{correlationID: "setperms", action: datasources.ActionPermissionsWrite, verb: utils.VerbSetPermissions},
}

func (r *subAccessAuthzREST) New() runtime.Object {
	return &datasourceV0alpha1.DatasourceAccessInfo{}
}

func (r *subAccessAuthzREST) Destroy() {}

func (r *subAccessAuthzREST) ConnectMethods() []string {
	return []string{"GET"}
}

func (r *subAccessAuthzREST) NewConnectOptions() (runtime.Object, bool, string) {
	return nil, false, ""
}

func (r *subAccessAuthzREST) Connect(ctx context.Context, name string, opts runtime.Object, responder rest.Responder) (http.Handler, error) {
	m := newConnectMetric("access", r.builder.pluginJSON.ID)
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		defer m.Record()
		access, err := r.getAccessInfo(ctx, name)
		if err != nil {
			m.SetError()
			responder.Error(err)
			return
		}
		responder.Object(http.StatusOK, access)
	}), nil
}

func (r *subAccessAuthzREST) getAccessInfo(ctx context.Context, name string) (*datasourceV0alpha1.DatasourceAccessInfo, error) {
	ns, err := apirequest.NamespaceInfoFrom(ctx, true)
	if err != nil {
		return nil, err
	}
	user, err := identity.GetRequester(ctx)
	if err != nil {
		return nil, err
	}

	group := r.builder.GetGroupVersion().Group
	checks := make([]authlib.BatchCheckItem, len(datasourceAccessChecks))
	for i, check := range datasourceAccessChecks {
		checks[i] = authlib.BatchCheckItem{
			CorrelationID: check.correlationID,
			Group:         group,
			Resource:      "datasources",
			Name:          name,
			Verb:          check.verb,
			Subresource:   check.subresource,
		}
	}

	response, err := r.builder.accessClient.BatchCheck(ctx, user, authlib.BatchCheckRequest{
		Namespace: ns.Value,
		Checks:    checks,
	})
	if err != nil {
		return nil, err
	}

	permissions := accesscontrol.Metadata{}
	for _, check := range datasourceAccessChecks {
		result := response.Results[check.correlationID]
		if result.Error != nil {
			return nil, result.Error
		}
		if result.Allowed {
			permissions[check.action] = true
		}
	}

	return &datasourceV0alpha1.DatasourceAccessInfo{Permissions: permissions}, nil
}

var _ = rest.Connecter(&subAccessREST{})

func (r *subAccessREST) New() runtime.Object {
	return &datasourceV0alpha1.DatasourceAccessInfo{}
}

func (r *subAccessREST) Destroy() {
	// no-op implementation needed for rest.Storage interface.
}

func (r *subAccessREST) ConnectMethods() []string {
	return []string{"GET"}
}

func (r *subAccessREST) NewConnectOptions() (runtime.Object, bool, string) {
	return nil, false, "" // true means you can use the trailing path as a variable
}

func (r *subAccessREST) Connect(ctx context.Context, name string, opts runtime.Object, responder rest.Responder) (http.Handler, error) {
	m := newConnectMetric("access", r.builder.pluginJSON.ID)

	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		defer m.Record()

		access, err := r.getAccessInfo(ctx, name)
		if err != nil {
			m.SetError()
			responder.Error(err)
		} else {
			responder.Object(200, access)
		}
	}), nil
}

func (r *subAccessREST) getAccessInfo(ctx context.Context, name string) (*datasourceV0alpha1.DatasourceAccessInfo, error) {
	reqContext := contexthandler.FromContext(ctx)
	resourceIDs := map[string]bool{name: true}
	access := accesscontrol.GetResourcesMetadata(reqContext.Req.Context(), reqContext.GetPermissions(), datasources.ScopePrefix, resourceIDs)
	return &datasourceV0alpha1.DatasourceAccessInfo{
		Permissions: access[name],
	}, nil
}
