// Demo emits synthetic Observer events; it does not start a mesh or exporter.
package main

import (
	"errors"
	"log/slog"
	"net/netip"
	"os"
	"time"

	"github.com/fastabc/fastmesh/examples/observability"
	"github.com/fastabc/fastmesh/node"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	var n observability.NodeObserver
	var c observability.ControllerObserver
	service := node.ServiceRef{Service: "payments", Port: "api"}
	deadline := time.Now().Add(time.Minute)
	n.PlanApplied(7, deadline, true)
	n.DialFailed(service, netip.MustParseAddrPort("127.0.0.1:8443"), errors.New("example dial failure"))
	n.ControlContact("controller:9443", nil)
	closed := c.DiscoveryStreamOpened()
	c.RecordPlanCompile(2*time.Millisecond, nil)
	c.RecordPlanSignature(time.Millisecond, nil)
	c.RecordDiscoveryAck(true)
	c.RecordPlanApplied("example-node", 7)
	logger.Info("synthetic fastmesh observations", "node", n.Snapshot(), "controller", c.Snapshot(),
		"service", observability.Inspect(node.Status{TopologyHash: "example", NotAfter: deadline}, service, time.Now()))
	closed()
	logger.Info("stream closed", "controller", c.Snapshot())
}
