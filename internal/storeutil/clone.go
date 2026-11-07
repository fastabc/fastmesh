// Package storeutil contains pure copies shared by memory and file backends.
package storeutil

import (
	"github.com/fastabc/fastmesh/store"
)

func CloneObject(obj store.Object) store.Object {
	obj.Data = append([]byte(nil), obj.Data...)
	if obj.Labels != nil {
		obj.Labels = CloneMap(obj.Labels)
	}
	return obj
}

func CloneMap(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
