package control

import (
	"context"
	"errors"

	discoveryv1 "github.com/fastabc/fastmesh/api/discovery/v1"
	"github.com/fastabc/fastmesh/policy/apply"
	spb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
)

func (c *Client) applyFull(ctx context.Context, r *discoveryv1.DiscoveryResponse) error {
	typeURL := r.GetTypeUrl()
	if typeURL == "" {
		typeURL = c.cfg.TypeURL
	}
	handler, ok := c.cfg.Appliers[typeURL]
	if !ok {
		return errors.New("ads: unexpected response type")
	}
	return handler.Full(ctx, r)
}

func (c *Client) applyDelta(ctx context.Context, r *discoveryv1.DeltaDiscoveryResponse) error {
	typeURL := r.GetTypeUrl()
	if typeURL == "" {
		typeURL = c.cfg.TypeURL
	}
	handler, ok := c.cfg.Appliers[typeURL]
	if !ok {
		return errors.New("ads: unexpected response type")
	}
	return handler.Delta(ctx, r)
}

// HandleFull verifies and applies one pushed response, then ACK/NACKs it. The
// ACK carries the accepted revision; a NACK keeps the last-good version and
// attaches the failure detail so the controller stops re-pushing the bad one.
func (c *Client) HandleFull(ctx context.Context, stream grpc.BidiStreamingClient[discoveryv1.DiscoveryRequest, discoveryv1.DiscoveryResponse], resp *discoveryv1.DiscoveryResponse) error {
	applyErr := c.applyFull(ctx, resp)
	if errors.Is(applyErr, apply.ErrRecoveryRequired) {
		return applyErr
	}
	if c.cfg.AfterApply != nil {
		applyErr = c.cfg.AfterApply(ctx, resp.GetVersionInfo(), resp.GetNode(), applyErr)
	}
	if applyErr == nil {
		c.contacted.Store(true)
		if c.cfg.ControlContact != nil {
			c.cfg.ControlContact(c.address, nil)
		}
		if c.cfg.OnContact != nil {
			c.cfg.OnContact()
		}
	}

	req := &discoveryv1.DiscoveryRequest{
		Node:          c.cfg.Node(),
		TypeUrl:       c.responseType(resp.GetTypeUrl()),
		ResponseNonce: resp.GetNonce(),
	}
	if applyErr != nil {
		req.VersionInfo = c.current(c.responseType(resp.GetTypeUrl()))
		req.ErrorDetail = &spb.Status{Message: applyErr.Error()}
	} else {
		req.VersionInfo = resp.GetVersionInfo()
	}
	return stream.Send(req)
}

func (c *Client) HandleDelta(ctx context.Context, stream grpc.BidiStreamingClient[discoveryv1.DeltaDiscoveryRequest, discoveryv1.DeltaDiscoveryResponse], resp *discoveryv1.DeltaDiscoveryResponse) error {
	applyErr := c.applyDelta(ctx, resp)
	if errors.Is(applyErr, apply.ErrRecoveryRequired) {
		return applyErr
	}
	if c.cfg.AfterApply != nil {
		applyErr = c.cfg.AfterApply(ctx, resp.GetSystemVersionInfo(), resp.GetNode(), applyErr)
	}
	if applyErr == nil {
		c.contacted.Store(true)
		if c.cfg.ControlContact != nil {
			c.cfg.ControlContact(c.address, nil)
		}
		if c.cfg.OnContact != nil {
			c.cfg.OnContact()
		}
	}

	req := &discoveryv1.DeltaDiscoveryRequest{
		Node:          c.cfg.Node(),
		TypeUrl:       c.responseType(resp.GetTypeUrl()),
		ResponseNonce: resp.GetNonce(),
	}
	if applyErr != nil {
		req.ErrorDetail = &spb.Status{Message: applyErr.Error()}
	}
	if err := stream.Send(req); err != nil {
		return err
	}
	if errors.Is(applyErr, ErrUnsignedDelta) {
		return grpcstatus.Error(codes.Unimplemented, "ads: unsigned delta; use signed full stream")
	}
	return nil
}
