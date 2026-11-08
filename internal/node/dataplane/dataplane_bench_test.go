package dataplane

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/fastabc/fastmesh/model"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
)

func BenchmarkAuthorization(b *testing.B) {
	peer := spiffeid.RequireFromString("spiffe://test/allowed")
	other := spiffeid.RequireFromString("spiffe://test/other")
	for _, services := range []int{1, 100, 1000} {
		for _, rules := range []int{1, 100, 1000} {
			b.Run(fmt.Sprintf("services=%d/rules=%d", services, rules), func(b *testing.B) {
				p := &model.Plan{}
				for i := range services {
					in := model.InboundService{Service: fmt.Sprint("s", i), Port: model.ServicePort{Name: "api"}}
					for range rules {
						in.Allow = append(in.Allow, model.InboundAllow{Source: other})
					}
					in.Allow[rules-1].Source = peer
					p.Inbound = append(p.Inbound, in)
				}
				ref := ServiceRef{Service: fmt.Sprint("s", services-1), Port: "api"}
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					if !AllowedInbound(p, ref, peer, false, nil) {
						b.Fatal("denied")
					}
				}
			})
		}
	}
}
func BenchmarkEndpointSelection(b *testing.B) {
	for _, count := range []int{1, 10, 100} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			r := New(nil, time.Second)
			candidates := make([]dialCandidate, count)
			for i := range candidates {
				candidates[i].Endpoint.InstanceID = fmt.Sprint(i)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				r.selectCandidate(candidates)
			}
		})
	}
}

type revocationRecorder struct {
	Observer
	first, last time.Time
}

func (o *revocationRecorder) ConnRevoked(model.Direction, ServiceRef, spiffeid.ID) {
	if o.first.IsZero() {
		o.first = time.Now()
	}
	o.last = time.Now()
}
func BenchmarkRevocation(b *testing.B) {
	for _, count := range []int{1, 100, 1000} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			var total, decision, closeTime, notify time.Duration
			for range b.N {

				r := New(nil, time.Second)
				var peers []net.Conn
				for range count {
					a, c := net.Pipe()
					r.active[&Conn{conn: a, runtime: r, ready: true}] = struct{}{}
					peers = append(peers, c)
				}
				r.zero = make(chan struct{})
				o := &revocationRecorder{}
				r.SetObserver(o)
				d := time.Now()
				for c := range r.active {
					if c.allowed(nil) {
						b.Fatal("allowed")
					}
				}
				decision += time.Since(d)

				start := time.Now()
				if err := r.Reconcile(); err != nil {
					b.Fatal(err)
				}
				elapsed := time.Since(start)
				total += elapsed
				closeTime += o.first.Sub(start)
				notify += o.last.Sub(o.first)
				if elapsed > time.Second {
					b.Fatal("revocation budget exceeded")
				}

				for _, c := range peers {
					c.Close()
				}

			}
			b.ReportMetric(float64(total.Nanoseconds())/float64(b.N), "reconcile-ns/op")
			b.ReportMetric(float64(decision.Nanoseconds())/float64(b.N), "decision-probe-ns/op")
			b.ReportMetric(float64(closeTime.Nanoseconds())/float64(b.N), "through-close-ns/op")
			b.ReportMetric(float64(notify.Nanoseconds())/float64(b.N), "notify-ns/op")
		})
	}
}

func BenchmarkRuntimeAuthorization(b *testing.B) {
	peer := spiffeid.RequireFromString("spiffe://test/allowed")
	other := spiffeid.RequireFromString("spiffe://test/other")
	for _, services := range []int{1, 100, 1000} {
		for _, rules := range []int{1, 100, 1000} {
			b.Run(fmt.Sprintf("services=%d/rules=%d", services, rules), func(b *testing.B) {
				p := &model.Plan{}
				for i := range services {
					in := model.InboundService{Service: fmt.Sprint("s", i), Port: model.ServicePort{Name: "api"}}
					for range rules {
						in.Allow = append(in.Allow, model.InboundAllow{Source: other})
					}
					in.Allow[rules-1].Source = peer
					p.Inbound = append(p.Inbound, in)
				}
				ref := ServiceRef{Service: fmt.Sprint("s", services-1), Port: "api"}
				r := New(nil, time.Second)
				r.plan.Store(p)
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					if !r.AllowedInbound(p, ref, peer, false, nil) {
						b.Fatal("denied")
					}
				}
			})
		}
	}
}
