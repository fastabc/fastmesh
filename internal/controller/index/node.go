package index

import (
	"encoding/json"
	"reflect"
	"time"

	"github.com/fastabc/fastmesh/model"
	"github.com/fastabc/fastmesh/store"
)

// Unknown records conservatively wake all tenants. Contact timestamps do not
// change execution policy, so they must not start another authorization cycle.
func nodeNotify(prev, next store.Object) (tenant string, notify bool) {
	var a, b model.Node
	if json.Unmarshal(next.Data, &b) != nil {
		return "", true
	}
	tenant = b.Tenant
	if tenant == "" {
		tenant = model.DefaultTenant
	}
	if prev.Data == nil || json.Unmarshal(prev.Data, &a) != nil {
		return tenant, true
	}
	a.Status.LastContact, b.Status.LastContact = time.Time{}, time.Time{}
	a.Status.ObservedAt, b.Status.ObservedAt = time.Time{}, time.Time{}
	return tenant, !reflect.DeepEqual(a, b)
}
