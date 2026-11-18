package etcd

import (
	"context"
	clientv3 "go.etcd.io/etcd/client/v3"
	"testing"

	"github.com/fastabc/fastmesh/store"
	"github.com/fastabc/fastmesh/store/storetest"
)

func TestViewConformance(t *testing.T) {
	storetest.RunViewConformance(t, func(t *testing.T) store.ObjectStore { return openEtcd(t) })
}

// Count logical Range requests issued to a real etcd backend.
type countingKV struct {
	clientv3.KV
	reads int
}

func (c *countingKV) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	c.reads++
	return c.KV.Get(ctx, key, opts...)
}

func TestViewReadsSharedPrefixOnce(t *testing.T) {
	s := openEtcd(t)
	ctx := context.Background()
	for _, tenant := range []string{"beta", "alpha"} {
		_, err := s.PutObject(ctx, store.Object{Key: store.ObjectKey{Tenant: tenant, Kind: "Service", Name: "one"}, Data: []byte("original"), Labels: map[string]string{"tag": "original"}})
		if err != nil {
			t.Fatal(err)
		}
	}
	counter := &countingKV{KV: s.client.KV}
	s.client.KV = counter
	ranges := []store.Range{{Kind: "Service"}, {Kind: "Access"}, {Kind: "Group"}, {Kind: "Bind"}, {Kind: "Endpoint"}, {Kind: "Node"}}
	v, err := s.View(ctx, ranges)
	if err != nil || counter.reads != 1 || len(v.Objects) != 6 {
		t.Fatalf("six ranges: reads=%d result=%d err=%v", counter.reads, len(v.Objects), err)
	}
	all := store.Range{}
	services := ranges[0]
	v, err = s.View(ctx, []store.Range{all, services, services})
	if err != nil {
		t.Fatal(err)
	}
	if objects := v.Objects[services]; len(objects) != 2 || objects[0].Key.Tenant != "alpha" || objects[1].Key.Tenant != "beta" {
		t.Fatalf("unexpected order or duplicate range: %v", objects)
	}
	v.Objects[all][0].Data[0] = 'X'
	v.Objects[all][0].Labels["tag"] = "changed"
	if obj := v.Objects[services][0]; string(obj.Data) != "original" || obj.Labels["tag"] != "original" {
		t.Fatal("overlapping ranges share object storage")
	}
}
