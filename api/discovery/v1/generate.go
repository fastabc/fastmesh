// Package discoveryv1 is the shared, ADS and Bootstrap contract.
package discoveryv1

//go:generate protoc -I ../../proto -I ../../third_party --go_out=../../.. --go_opt=module=github.com/fastabc/fastmesh --go-grpc_out=../../.. --go-grpc_opt=module=github.com/fastabc/fastmesh ../../proto/fastmesh/discovery/v1/discovery.proto
