# Observer adapters

This example uses fixed-size atomic counters and no extra dependencies. Node and
Controller adapters implement the existing public Observer contracts. Callbacks
perform no I/O and retain no node IDs, endpoint addresses or error strings.

Run a synthetic event demonstration:

```sh
GOWORK=off go run ./examples/observability/cmd/demo
```

Copy the adapters into your host, attach them at construction, and report their
snapshots from a host-owned goroutine. The sample is not a stable public metrics
API or an OTLP/Prometheus exporter. See [integration and diagnosis](../../docs/integration.md)
for event meanings, request-layer diagnostics and lifecycle ownership.
