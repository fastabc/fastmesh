// Package discovery owns the shared ADS delivery state machine.
package discovery

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"slices"
	"strconv"
	"sync"
	"time"

	discoveryv1 "github.com/fastabc/fastmesh/api/discovery/v1"
	"github.com/fastabc/fastmesh/model"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const defaultCoalesceWindow = 50 * time.Millisecond

type Session struct {
	Principal model.Session
	Node      *discoveryv1.Node
}
type BuildRequest struct {
	Session      Session
	TypeURL      string
	Delta        bool
	FromRevision string
}
type Artifact struct {
	InputRevision int64
	Revision      string
	Resources     []*discoveryv1.Resource
	Signature     *discoveryv1.SnapshotSignature
	// Sign defers signing until the stream decides this revision needs delivery.
	// Builders returning an existing Signature leave it nil.
	Sign         func(context.Context) (*discoveryv1.SnapshotSignature, error)
	FetchPayload []byte
}
type Builder interface {
	Build(context.Context, BuildRequest) (Artifact, error)
}
type BuilderFunc func(context.Context, BuildRequest) (Artifact, error)

func (f BuilderFunc) Build(ctx context.Context, r BuildRequest) (Artifact, error) { return f(ctx, r) }

type DiscoveryRecorder interface {
	DiscoveryStreamOpened() func()
	RecordDiscoveryPush(string, error, time.Duration)
	RecordDiscoveryAck(bool)
}

// StreamObserver reports bounded stream events. Implementations are optional
// and must be concurrency safe and return promptly.
type StreamObserver interface {
	RecordDiscoveryStreamEnd(string, time.Duration)
	RecordDiscoveryAckWait(string, time.Duration)
	RecordDiscoveryPushSkipped(string)
	RecordPlanApplied(string, int64)
}
type planObserver interface{ RecordPlanApplied(string, int64) }

type Config struct {
	Authorize                         func(context.Context, string, string) (Session, error)
	Watch                             func(context.Context, Session, string) (<-chan struct{}, error)
	Builder                           Builder
	DefaultTypeURL                    string
	HeartbeatInterval, CoalesceWindow time.Duration
	Metrics                           DiscoveryRecorder
	// FirstRequestTimeout and AckTimeout bound silent clients. Zero uses defaults.
	FirstRequestTimeout, AckTimeout time.Duration
	// MaxStreams bounds all streams, including those waiting for their first request.
	// MaxStreamsPerNode applies after authentication, across full and delta streams.
	MaxStreams, MaxStreamsPerNode int
}
type Server struct {
	discoveryv1.UnimplementedDiscoveryServer
	cfg      Config
	observer StreamObserver
	applied  planObserver
	mu       sync.Mutex
	streams  int
	nodes    map[model.Session]int
}

func New(cfg Config) *Server {
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = 30 * time.Second
	}
	if cfg.CoalesceWindow <= 0 {
		cfg.CoalesceWindow = defaultCoalesceWindow
	}
	if cfg.FirstRequestTimeout <= 0 {
		cfg.FirstRequestTimeout = 5 * time.Second
	}
	if cfg.AckTimeout <= 0 {
		cfg.AckTimeout = 15 * time.Second
	}
	if cfg.MaxStreams <= 0 {
		cfg.MaxStreams = 1024
	}
	if cfg.MaxStreamsPerNode <= 0 {
		cfg.MaxStreamsPerNode = 4
	}
	s := &Server{cfg: cfg, nodes: make(map[model.Session]int)}
	s.observer, _ = cfg.Metrics.(StreamObserver)
	s.applied, _ = cfg.Metrics.(planObserver)
	return s
}
func (s *Server) streamOpened() func() {
	if s.cfg.Metrics == nil {
		return func() {}
	}
	return s.cfg.Metrics.DiscoveryStreamOpened()
}
func (s *Server) recordPush(mode string, err error, started time.Time) {
	if s.cfg.Metrics != nil {
		s.cfg.Metrics.RecordDiscoveryPush(mode, err, time.Since(started))
	}
}
func (s *Server) recordAck(ok bool) {
	if s.cfg.Metrics != nil {
		s.cfg.Metrics.RecordDiscoveryAck(ok)
	}
}
func nodeVersion(n *discoveryv1.Node) string {
	return n.GetIntent() + ":" + strconv.FormatUint(n.GetGeneration(), 10) + ":" + n.GetObservedIntent() + ":" + strconv.FormatUint(n.GetObservedGeneration(), 10)
}
func nonce() string { var b [8]byte; _, _ = rand.Read(b[:]); return hex.EncodeToString(b[:]) }

func (s *Server) authorize(ctx context.Context, id, tenant, typeURL string) (Session, error) {
	session, err := s.cfg.Authorize(ctx, id, tenant)
	if err != nil {
		return session, err
	}
	capability := ""
	switch typeURL {
	case model.PlanTypeURL:
		capability = model.PlanCapability
	}
	if capability != "" && (session.Principal.Workload.IsZero() || !slices.Contains(session.Node.GetCapabilities(), capability)) {
		return session, status.Error(codes.PermissionDenied, "node has not enrolled "+capability+" capability")
	}
	return session, nil
}

// StreamAggregatedResources delivers full snapshots, retaining rejected versions.
func (s *Server) StreamAggregatedResources(stream grpc.BidiStreamingServer[discoveryv1.DiscoveryRequest, discoveryv1.DiscoveryResponse]) error {
	return runSession(s, stream, fullCodec(), "full")
}

// DeltaAggregatedResources advances the client's base only after acknowledgement.
func (s *Server) DeltaAggregatedResources(stream grpc.BidiStreamingServer[discoveryv1.DeltaDiscoveryRequest, discoveryv1.DeltaDiscoveryResponse]) error {
	return runSession(s, stream, deltaCodec(), "delta")
}

func (s *Server) FetchSnapshot(ctx context.Context, req *discoveryv1.FetchSnapshotRequest) (*discoveryv1.FetchSnapshotResponse, error) {
	typeURL := req.GetTypeUrl()
	if typeURL == "" {
		typeURL = s.cfg.DefaultTypeURL
	}
	session, err := s.authorize(ctx, req.GetNode().GetId(), req.GetTenant(), typeURL)
	if err != nil {
		return nil, err
	}
	built, err := s.cfg.Builder.Build(ctx, BuildRequest{Session: session, TypeURL: typeURL})
	if err != nil {
		return nil, err
	}
	if err := built.sign(ctx); err != nil {
		return nil, err
	}
	response := &discoveryv1.FetchSnapshotResponse{Tenant: session.Principal.Tenant, Revision: built.Revision, Snapshot: built.FetchPayload, SnapshotSignature: built.Signature, Node: session.Node, TypeUrl: typeURL}
	if typeURL == model.PlanTypeURL && proto.Size(response) > model.MaxPlanEnvelopeBytes {
		return nil, status.Error(codes.ResourceExhausted, model.ErrPlanTooLarge.Error())
	}
	return response, nil
}
