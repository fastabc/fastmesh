package expiry

import (
	"encoding/json"
	"net/netip"
	"testing"
	"time"

	"github.com/fastabc/fastmesh/model"
	"github.com/fastabc/fastmesh/store"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
)

func TestRenewalSupersedesExpiryAndFailedWorkCanRetry(t *testing.T) {
	now := time.Now()
	e := model.Endpoint{Tenant: "default", Service: "api", Port: "tcp", InstanceID: "one", Owner: spiffeid.RequireFromString("spiffe://test/owner"), Address: netip.MustParseAddrPort("127.0.0.1:80"), Generation: 1, LeaseExpiresAt: now.Add(time.Minute)}
	data, _ := json.Marshal(model.EndpointRecord{Endpoint: e})
	obj := store.Object{Key: model.EndpointKey(e.Tenant, e.Service, e.Owner, e.InstanceID), ResourceVersion: 1, Data: data}
	var s Schedule
	s.Reset([]store.Object{obj})
	e.LeaseExpiresAt = now.Add(2 * time.Minute)
	obj.Data, _ = json.Marshal(model.EndpointRecord{Endpoint: e})
	obj.ResourceVersion = 2
	s.Apply([]store.Event{{Type: store.EventPut, Object: obj}})
	if len(s.Due(now.Add(time.Minute))) != 0 {
		t.Fatal("renewed instance expired on old deadline")
	}
	due := s.Due(now.Add(2 * time.Minute))
	if len(due) != 1 || due[0].ResourceVersion != 2 {
		t.Fatal("renewed deadline lost")
	}
	s.Retry(due[0])
	if len(s.Due(time.Now().Add(time.Second))) != 1 {
		t.Fatal("failed task not retried")
	}
	s.Retry(obj)
	obj.ResourceVersion = 3
	s.Apply([]store.Event{{Type: store.EventDelete, Object: obj}})
	if len(s.Due(now.Add(3*time.Minute))) != 0 {
		t.Fatal("deleted task retried")
	}
}

func TestEphemeralNodesUseVersionedDeadlineHeap(t *testing.T) {
	now := time.Now()
	s := Schedule{NodeTTL: time.Minute}
	object := func(contact time.Time, version int64) store.Object {
		b, err := json.Marshal(model.Node{ID: "ephemeral", Ephemeral: true, Status: model.NodeStatus{LastContact: contact}})
		if err != nil {
			t.Fatal(err)
		}
		return store.Object{Key: model.NodeKey("ephemeral"), ResourceVersion: version, Data: b}
	}
	old := object(now.Add(-time.Minute), 1)
	newer := object(now, 2)
	s.Reset([]store.Object{old})
	s.Apply([]store.Event{{Object: newer}})
	if len(s.Due(now)) != 0 {
		t.Fatal("old expiry removed a live node")
	}
	if due := s.Due(now.Add(time.Minute)); len(due) != 1 || due[0].ResourceVersion != 2 {
		t.Fatalf("new node expiry: %+v", due)
	}
}
