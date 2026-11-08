package discovery

import (
	"errors"
	"io"
	"time"

	discoveryv1 "github.com/fastabc/fastmesh/api/discovery/v1"
	"github.com/fastabc/fastmesh/model"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type streamCodec[Req, Resp any] struct {
	first    func(*Req) (*discoveryv1.Node, string, string)
	ack      func(*Req) (string, string, bool)
	response func(Artifact, string, string, *discoveryv1.Node) *Resp
	delta    bool
}

func fullCodec() streamCodec[discoveryv1.DiscoveryRequest, discoveryv1.DiscoveryResponse] {
	return streamCodec[discoveryv1.DiscoveryRequest, discoveryv1.DiscoveryResponse]{
		first: func(r *discoveryv1.DiscoveryRequest) (*discoveryv1.Node, string, string) {
			return r.GetNode(), r.GetTypeUrl(), ""
		},
		ack: func(r *discoveryv1.DiscoveryRequest) (string, string, bool) {
			return r.GetResponseNonce(), r.GetVersionInfo(), r.GetErrorDetail() != nil
		},
		response: func(a Artifact, nonce, typeURL string, node *discoveryv1.Node) *discoveryv1.DiscoveryResponse {
			return &discoveryv1.DiscoveryResponse{VersionInfo: a.Revision, Resources: a.Resources, Nonce: nonce, TypeUrl: typeURL, SnapshotSignature: a.Signature, Node: node}
		},
	}
}

func deltaCodec() streamCodec[discoveryv1.DeltaDiscoveryRequest, discoveryv1.DeltaDiscoveryResponse] {
	return streamCodec[discoveryv1.DeltaDiscoveryRequest, discoveryv1.DeltaDiscoveryResponse]{
		delta: true,
		first: func(r *discoveryv1.DeltaDiscoveryRequest) (*discoveryv1.Node, string, string) {
			return r.GetNode(), r.GetTypeUrl(), r.GetInitialResourceVersions()["snapshot"]
		},
		ack: func(r *discoveryv1.DeltaDiscoveryRequest) (string, string, bool) {
			return r.GetResponseNonce(), "", r.GetErrorDetail() != nil
		},
		response: func(a Artifact, nonce, typeURL string, node *discoveryv1.Node) *discoveryv1.DeltaDiscoveryResponse {
			return &discoveryv1.DeltaDiscoveryResponse{SystemVersionInfo: a.Revision, Resources: a.Resources, Nonce: nonce, TypeUrl: typeURL, SnapshotSignature: a.Signature, Node: node}
		},
	}
}

type sessionState struct {
	lastSent, acked, nacked       string
	pendingNonce, pendingRevision string
	pendingInput                  int64
	pendingSince                  time.Time
	dirty                         bool
	from                          string
}

func runSession[Req, Resp any](s *Server, stream grpc.BidiStreamingServer[Req, Resp], codec streamCodec[Req, Resp], mode string) (streamErr error) {
	ctx, started := stream.Context(), time.Now()
	defer func() {
		if s.observer != nil {
			s.observer.RecordDiscoveryStreamEnd(streamEndReason(streamErr), time.Since(started))
		}
	}()
	release, err := s.reserveStream()
	if err != nil {
		return err
	}
	defer release()
	defer s.streamOpened()()
	first, err := firstRequest(ctx, stream.Recv, s.cfg.FirstRequestTimeout)
	if err != nil {
		return err
	}
	node, typeURL, from := codec.first(first)
	if typeURL == "" {
		typeURL = s.cfg.DefaultTypeURL
	}
	initial, err := s.authorize(ctx, node.GetId(), "", typeURL)
	if err != nil {
		return err
	}
	releaseNode, err := s.reserveNode(initial.Principal)
	if err != nil {
		return err
	}
	defer releaseNode()
	tenant := initial.Principal.Tenant
	requests, recvErr := make(chan *Req, 8), make(chan error, 1)
	go recvLoop(stream, requests, recvErr)
	changes, err := s.cfg.Watch(ctx, initial, typeURL)
	if err != nil {
		return err
	}
	state := sessionState{from: from}
	ackTimer := time.NewTimer(s.cfg.AckTimeout)
	ackTimer.Stop()
	defer ackTimer.Stop()
	push := func(force bool) (pushErr error) {
		if state.pendingNonce != "" {
			state.dirty = true
			return nil
		}
		started := time.Now()
		defer func() { s.recordPush(mode, pushErr, started) }()
		session, err := s.authorize(ctx, node.GetId(), tenant, typeURL)
		if err != nil {
			return err
		}
		built, err := s.cfg.Builder.Build(ctx, BuildRequest{Session: session, TypeURL: typeURL, Delta: codec.delta, FromRevision: state.from})
		if err != nil {
			return err
		}
		version := built.Revision + ":" + nodeVersion(session.Node)
		skipped := (!force && version == state.lastSent) || version == state.nacked
		if codec.delta {
			skipped = !force && version == state.acked
		}
		if skipped {
			if s.observer != nil {
				s.observer.RecordDiscoveryPushSkipped(mode)
			}
			return nil
		}
		if err := built.sign(ctx); err != nil {
			return err
		}
		nonce := nonce()
		response := codec.response(built, nonce, typeURL, session.Node)
		if typeURL == model.PlanTypeURL && proto.Size(any(response).(proto.Message)) > model.MaxPlanEnvelopeBytes {
			return status.Error(codes.ResourceExhausted, model.ErrPlanTooLarge.Error())
		}
		if err := sendResponse(ctx, func() error { return stream.Send(response) }, s.cfg.AckTimeout); err != nil {
			return err
		}
		state.lastSent, state.pendingNonce, state.pendingRevision = version, nonce, built.Revision
		state.pendingInput, state.pendingSince = built.InputRevision, time.Now()
		ackTimer.Reset(s.cfg.AckTimeout)
		return nil
	}
	if err := push(false); err != nil {
		return err
	}
	heartbeat := time.NewTicker(s.cfg.HeartbeatInterval)
	defer heartbeat.Stop()
	coalesce := newCoalescer(s.cfg.CoalesceWindow)
	defer coalesce.stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ackTimer.C:
			s.recordAckWait("timeout", state.pendingSince)
			return status.Error(codes.DeadlineExceeded, "discovery: acknowledgement timeout")
		case err := <-recvErr:
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		case request := <-requests:
			nonce, version, nack := codec.ack(request)
			if state.pendingNonce == "" || nonce != state.pendingNonce || (!codec.delta && !nack && version != state.pendingRevision) {
				continue
			}
			ackTimer.Stop()
			result := "ack"
			if nack {
				result = "nack"
			}
			s.recordAckWait(result, state.pendingSince)
			s.recordAck(!nack)
			if nack {
				if codec.delta {
					return status.Error(codes.Unimplemented, "ads: delta rejected; use full stream")
				}
				state.nacked = state.lastSent
			} else {
				s.recordApplied(node.GetId(), state.pendingInput)
				state.acked = state.lastSent
				if codec.delta {
					state.from = state.pendingRevision
				}
			}
			state.pendingNonce, state.pendingRevision = "", ""
			if state.dirty {
				state.dirty = false
				if err := push(false); err != nil {
					return err
				}
			}
		case _, ok := <-changes:
			if !ok {
				return nil
			}
			coalesce.schedule()
		case <-coalesce.ch:
			coalesce.fired()
			if err := push(false); err != nil {
				return err
			}
		case <-heartbeat.C:
			// Reauthorize even while an unacknowledged response blocks pushes.
			if _, err := s.authorize(ctx, node.GetId(), tenant, typeURL); err != nil {
				return err
			}
			if err := push(true); err != nil {
				return err
			}
		}
	}
}

func recvLoop[Req, Resp any](stream grpc.BidiStreamingServer[Req, Resp], out chan<- *Req, errc chan<- error) {
	for {
		r, err := stream.Recv()
		if err != nil {
			errc <- err
			return
		}
		select {
		case out <- r:
		case <-stream.Context().Done():
			return
		}
	}
}
