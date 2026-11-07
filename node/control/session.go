package control

import (
	"context"

	discoveryv1 "github.com/fastabc/fastmesh/api/discovery/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
)

func (c *Client) Session(ctx context.Context, conn grpc.ClientConnInterface) error {
	if !c.loaded() {
		if err := c.bootstrapFull(ctx, conn); err != nil {
			return err
		}
	}
	err := c.deltaSession(ctx, conn)
	if isUnimplemented(err) {
		return c.fullSession(ctx, conn)
	}
	return err
}

func (c *Client) fullSession(ctx context.Context, conn grpc.ClientConnInterface) error {
	stream, err := discoveryv1.NewDiscoveryClient(conn).StreamAggregatedResources(ctx)
	if err != nil {
		return err
	}
	if err := c.sendFullRequest(stream); err != nil {
		return err
	}
	for {
		resp, err := stream.Recv()
		if err != nil {
			return err
		}
		if err := c.HandleFull(ctx, stream, resp); err != nil {
			return err
		}
	}
}

func (c *Client) bootstrapFull(ctx context.Context, conn grpc.ClientConnInterface) error {
	stream, err := discoveryv1.NewDiscoveryClient(conn).StreamAggregatedResources(ctx)
	if err != nil {
		return err
	}
	defer stream.CloseSend()
	if err := c.sendFullRequest(stream); err != nil {
		return err
	}
	for !c.loaded() {
		resp, err := stream.Recv()
		if err != nil {
			return err
		}
		if err := c.HandleFull(ctx, stream, resp); err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) sendFullRequest(stream grpc.BidiStreamingClient[discoveryv1.DiscoveryRequest, discoveryv1.DiscoveryResponse]) error {
	return stream.Send(&discoveryv1.DiscoveryRequest{
		Node:        c.cfg.Node(),
		TypeUrl:     c.cfg.TypeURL,
		VersionInfo: c.current(c.cfg.TypeURL),
	})
}

func (c *Client) deltaSession(ctx context.Context, conn grpc.ClientConnInterface) error {
	stream, err := discoveryv1.NewDiscoveryClient(conn).DeltaAggregatedResources(ctx)
	if err != nil {
		return err
	}
	initial := map[string]string{}
	if rev := c.current(c.cfg.TypeURL); rev != "" {
		initial["snapshot"] = string(rev)
	}
	if err := stream.Send(&discoveryv1.DeltaDiscoveryRequest{
		Node:                    c.cfg.Node(),
		TypeUrl:                 c.cfg.TypeURL,
		InitialResourceVersions: initial,
		ResourceNamesSubscribe:  []string{"snapshot"},
	}); err != nil {
		return err
	}
	for {
		resp, err := stream.Recv()
		if err != nil {
			return err
		}
		if err := c.HandleDelta(ctx, stream, resp); err != nil {
			return err
		}
	}
}

func isUnimplemented(err error) bool {
	return grpcstatus.Code(err) == codes.Unimplemented
}
