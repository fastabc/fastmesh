// Package observability demonstrates bounded, dependency-free Observer adapters.
// Copy the adapters into a host application; they are examples, not stable SDK APIs.
// Callbacks update fixed-size counters only. The host owns reporting and shutdown.
package observability

import (
	"net/netip"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fastabc/fastmesh/controller"
	"github.com/fastabc/fastmesh/node"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
)

var _ node.Observer = (*NodeObserver)(nil)
var _ controller.Observer = (*ControllerObserver)(nil)

type directional struct{ inbound, outbound, other atomic.Uint64 }

type Directions struct {
	Inbound  uint64 `json:"inbound"`
	Outbound uint64 `json:"outbound"`
	Other    uint64 `json:"other"`
}

func (d *directional) add(dir node.Direction) {
	switch dir {
	case node.Inbound:
		d.inbound.Add(1)
	case node.Outbound:
		d.outbound.Add(1)
	default:
		d.other.Add(1)
	}
}
func (d *directional) snapshot() Directions {
	return Directions{d.inbound.Load(), d.outbound.Load(), d.other.Load()}
}

type NodeObserver struct {
	applied, changed, dialFailures, contacts, contactErrors atomic.Uint64
	denied, revoked                                         directional
}

type NodeStats struct {
	PlanApplied     uint64     `json:"plan_applied"`
	TopologyChanged uint64     `json:"topology_changed"`
	Denied          Directions `json:"denied"`
	Revoked         Directions `json:"revoked"`
	DialFailures    uint64     `json:"dial_failures"`
	Contacts        uint64     `json:"control_contacts"`
	ContactErrors   uint64     `json:"control_contact_errors"`
}

func (o *NodeObserver) PlanApplied(_ int64, _ time.Time, changed bool) {
	o.applied.Add(1)
	if changed {
		o.changed.Add(1)
	}
}
func (o *NodeObserver) ConnDenied(dir node.Direction, _ node.ServiceRef, _ spiffeid.ID, _ string) {
	o.denied.add(dir)
}
func (o *NodeObserver) ConnRevoked(dir node.Direction, _ node.ServiceRef, _ spiffeid.ID) {
	o.revoked.add(dir)
}
func (o *NodeObserver) DialFailed(node.ServiceRef, netip.AddrPort, error) {
	o.dialFailures.Add(1)
}
func (o *NodeObserver) ControlContact(_ string, err error) {
	o.contacts.Add(1)
	if err != nil {
		o.contactErrors.Add(1)
	}
}

// Snapshot is safe during callbacks, but counters are not an atomic transaction.
func (o *NodeObserver) Snapshot() NodeStats {
	return NodeStats{o.applied.Load(), o.changed.Load(), o.denied.snapshot(), o.revoked.snapshot(), o.dialFailures.Load(), o.contacts.Load(), o.contactErrors.Load()}
}

type timing struct{ count, errors, nanos atomic.Uint64 }

type TimingStats struct {
	Count   uint64  `json:"count"`
	Errors  uint64  `json:"errors"`
	Seconds float64 `json:"seconds_sum"`
}

func (t *timing) record(d time.Duration, err error) {
	t.count.Add(1)
	if err != nil {
		t.errors.Add(1)
	}
	t.nanos.Add(uint64(max(d, 0)))
}
func (t *timing) snapshot() TimingStats {
	return TimingStats{t.count.Load(), t.errors.Load(), float64(t.nanos.Load()) / float64(time.Second)}
}

type ControllerObserver struct {
	streams                        atomic.Int64
	ack, nack, applied, skipped    atomic.Uint64
	compile, sign, push            timing
	streamEnds                     [8]atomic.Uint64
	ackWait, nackWait, timeoutWait timing
}

type ControllerStats struct {
	Streams                        int64             `json:"streams"`
	ACK                            uint64            `json:"ack"`
	NACK                           uint64            `json:"nack"`
	AppliedReports                 uint64            `json:"applied_reports"`
	PushSkipped                    uint64            `json:"push_skipped_unchanged"`
	Compile                        TimingStats       `json:"compile"`
	Sign                           TimingStats       `json:"sign"`
	Push                           TimingStats       `json:"push"`
	StreamEnds                     map[string]uint64 `json:"stream_ends"`
	AckWait, NackWait, TimeoutWait TimingStats
}

func (o *ControllerObserver) DiscoveryStreamOpened() func() {
	o.streams.Add(1)
	var once sync.Once
	return func() { once.Do(func() { o.streams.Add(-1) }) }
}
func (o *ControllerObserver) RecordDiscoveryPush(_ string, err error, d time.Duration) {
	o.push.record(d, err)
}
func (o *ControllerObserver) RecordDiscoveryAck(ok bool) {
	if ok {
		o.ack.Add(1)
	} else {
		o.nack.Add(1)
	}
}
func (o *ControllerObserver) RecordPlanCompile(d time.Duration, err error) {
	o.compile.record(d, err)
}
func (o *ControllerObserver) RecordPlanSignature(d time.Duration, err error) {
	o.sign.record(d, err)
}
func (o *ControllerObserver) RecordDiscoveryPushSkipped(_ string) { o.skipped.Add(1) }
func (o *ControllerObserver) RecordPlanApplied(_ string, _ int64) {
	o.applied.Add(1)
}
func (o *ControllerObserver) Snapshot() ControllerStats {
	ends := make(map[string]uint64, len(streamReasons))
	for i, reason := range streamReasons {
		ends[reason] = o.streamEnds[i].Load()
	}
	return ControllerStats{Streams: o.streams.Load(), ACK: o.ack.Load(), NACK: o.nack.Load(), AppliedReports: o.applied.Load(), PushSkipped: o.skipped.Load(), Compile: o.compile.snapshot(), Sign: o.sign.snapshot(), Push: o.push.snapshot(), StreamEnds: ends, AckWait: o.ackWait.snapshot(), NackWait: o.nackWait.snapshot(), TimeoutWait: o.timeoutWait.snapshot()}
}

type ServiceStatus struct {
	State                    string  `json:"state"`
	ValidityRemainingSeconds float64 `json:"validity_remaining_seconds"`
	SelectableEndpoints      int     `json:"selectable_endpoints"`
}

// Inspect reports local plan/selection state, not end-to-end service health.
// Absence from Dialable cannot distinguish missing authorization from no endpoints.
func Inspect(s node.Status, service node.ServiceRef, now time.Time) ServiceStatus {
	if s.TopologyHash == "" || s.NotAfter.IsZero() {
		return ServiceStatus{State: "no_plan"}
	}
	remaining := s.NotAfter.Sub(now).Seconds()
	if remaining <= 0 {
		return ServiceStatus{State: "expired_plan"}
	}
	result := ServiceStatus{State: "no_selectable_endpoint", ValidityRemainingSeconds: remaining, SelectableEndpoints: s.Dialable[service]}
	if result.SelectableEndpoints > 0 {
		result.State = "selectable"
	}
	return result
}

// The reason vocabulary is fixed; unknown future values share the error bucket.
var streamReasons = [...]string{"ok", "canceled", "first_request_timeout", "send_timeout", "ack_timeout", "stream_capacity", "node_capacity", "error"}

func (o *ControllerObserver) RecordDiscoveryStreamEnd(reason string, _ time.Duration) {
	for i, name := range streamReasons {
		if reason == name {
			o.streamEnds[i].Add(1)
			return
		}
	}
	o.streamEnds[len(streamReasons)-1].Add(1)
}
func (o *ControllerObserver) RecordDiscoveryAckWait(result string, elapsed time.Duration) {
	switch result {
	case "ack":
		o.ackWait.record(elapsed, nil)
	case "nack":
		o.nackWait.record(elapsed, nil)
	case "timeout":
		o.timeoutWait.record(elapsed, nil)
	}
}
