// Package catalogv1 contains the mesh catalog wire contract.
package catalogv1

//go:generate protoc -I ../../proto --go_out=../../.. --go_opt=module=github.com/fastabc/fastmesh --go-grpc_out=../../.. --go-grpc_opt=module=github.com/fastabc/fastmesh ../../proto/fastmesh/catalog/v1/catalog.proto
