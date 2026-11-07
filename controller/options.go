package controller

import (
	"github.com/fastabc/fastmesh/controller/discovery"
	"google.golang.org/grpc"
)

// WithDiscovery configures the embedded Discovery service.
func WithDiscovery(cfg discovery.Config) Option { return func(c *Config) { c.discovery = cfg } }

// WithGRPCRegistrars adds host-owned services to the controller server.
func WithGRPCRegistrars(registrars ...func(*grpc.Server)) Option {
	return func(c *Config) { c.grpcRegistrars = append(c.grpcRegistrars, registrars...) }
}

// WithBuilder installs the product compiler/codec for non-mesh TypeURLs.
func WithBuilder(builder discovery.Builder) Option {
	return func(c *Config) { c.discovery.Builder = builder }
}
