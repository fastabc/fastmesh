package index

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/fastabc/fastmesh/model"
	"github.com/fastabc/fastmesh/store"
)

func TestNodeNotifications(t *testing.T) {
	for _, mode := range []string{"contact", "intent", "delete", "malformed", "default", "move"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			n := model.Node{ID: "n", Tenant: "acme", Intent: "active", Ephemeral: true}
			if mode == "default" {
				n.Tenant = ""
			}
			data, err := json.Marshal(n)
			if err != nil {
				t.Fatal(err)
			}
			old := store.Object{Key: model.NodeKey("n"), Data: data}
			i := New(nil, nil)
			i.reset(store.View{Revision: 1, Objects: map[store.Range][]store.Object{{Kind: model.NodeKind}: {old}}})
			channels := map[string]<-chan struct{}{}
			for _, tenant := range []string{"acme", "other", "default"} {
				channels[tenant], err = i.Subscribe(ctx, tenant)
				if err != nil {
					t.Fatal(err)
				}
			}
			n.Status.LastContact, n.Status.ObservedAt = time.Now(), time.Now()
			if mode != "contact" {
				n.Intent = "disabled"
			}
			if mode == "move" {
				n.Tenant = "other"
			}
			data, err = json.Marshal(n)
			if err != nil {
				t.Fatal(err)
			}
			if mode == "malformed" {
				data = []byte("{invalid")
			}
			e := store.Event{Type: store.EventPut, Object: store.Object{Key: old.Key, Data: data}}
			if mode == "delete" {
				e.Type, e.Object.Data = store.EventDelete, nil
			}
			i.apply(store.RevisionBatch{Revision: 2, Events: []store.Event{e}})
			for tenant, ch := range channels {
				want := mode == "malformed" || ((mode == "intent" || mode == "delete") && tenant == "acme") || (mode == "default" && tenant == "default") || (mode == "move" && (tenant == "acme" || tenant == "other"))
				got := false
				select {
				case <-ch:
					got = true
				default:
				}
				if got != want {
					t.Errorf("%s notified=%v want=%v", tenant, got, want)
				}
			}
		})
	}
}
