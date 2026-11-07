package node

import (
	"context"
	"net"

	"github.com/fastabc/fastmesh/internal/node/dataplane"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
)

func PeerID(conn net.Conn) (spiffeid.ID, bool) { return dataplane.PeerID(conn) }

func PeerFromContext(ctx context.Context) (spiffeid.ID, bool) { return dataplane.PeerFromContext(ctx) }
