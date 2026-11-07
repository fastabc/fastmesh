# Orders and payments outside the framework repository

Copy `main.go` into a new directory outside both repositories. Build an independent
module with Go 1.27.3 and `GOWORK=off`. The only local replacement is fastmesh:

```sh
mkdir /tmp/orders-payments
cp examples/orders-payments/main.go /tmp/orders-payments/
cd /tmp/orders-payments
go mod init example.org/orders-payments
go mod edit -require=github.com/fastabc/fastmesh@v0.0.0-20261001152545-2923fc02694b
go mod edit -replace=github.com/fastabc/fastmesh=/absolute/path/to/fastmesh
GOWORK=off go mod tidy
GOWORK=off go build -o demo .
./demo -role setup
```

Run three separate processes from that directory, in this order:

```sh
./demo -role controller
./demo -role payments
./demo -role orders
```

Wait for `controller-ready` and `payments-ready` before starting the next process.
Orders prints `paid`. The controller serves Bootstrap, Catalog and Discovery on
one mTLS port. Payments binds an authenticated HTTP service with an exact
`GET /pay` allow for orders; applications import only the public SDK.

After one successful call, stop the controller and immediately run orders again.
Its persisted node and unexpired signed plan permit a new direct call. Endpoint
leases expire after 30 seconds and plan authorization after one minute; after
those limits, new calls fail closed. Restarting the controller reestablishes ADS
and lease renewal. Payments preserves its instance ID and increments its durable
generation when it must republish after an expired lease.

The generated credentials last 24 hours and are confined to the demo directory.
The file identity source reloads credentials on each handshake; production
applications can instead pass the SPIFFE Workload API source.
