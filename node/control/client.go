package control

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	discoveryv1 "github.com/fastabc/fastmesh/api/discovery/v1"
	"google.golang.org/grpc"
)

var ErrUnsignedDelta = errors.New("ads: unsigned delta cannot refresh policy")

// Applier owns verification and materialization for one registered TypeURL.
// Callbacks are serialized within its subscription.
type Applier struct {
	Loaded  func() bool
	Current func() string
	Full    func(context.Context, *discoveryv1.DiscoveryResponse) error
	Delta   func(context.Context, *discoveryv1.DeltaDiscoveryResponse) error
}

type Config struct {
	TypeURL        string
	Appliers       map[string]Applier
	Node           func() *discoveryv1.Node
	DialOptions    func(context.Context) ([]grpc.DialOption, error)
	Backoff        time.Duration
	OnContact      func()
	ControlContact func(string, error)
	AfterApply     func(context.Context, string, *discoveryv1.Node, error) error
	// StopOnError lets a caller handle a terminal subscription negotiation error.
	StopOnError func(error) bool
}

type Client struct {
	cfg       Config
	contacted atomic.Bool
	address   string
}

func New(cfg Config) *Client {
	if cfg.Backoff <= 0 {
		cfg.Backoff = 500 * time.Millisecond
	}
	return &Client{cfg: cfg}
}

func (c *Client) loaded() bool { return c.cfg.Appliers[c.cfg.TypeURL].Loaded() }

func (c *Client) current(typeURL string) string {
	if handler, ok := c.cfg.Appliers[typeURL]; ok {
		return handler.Current()
	}
	return ""
}

func (c *Client) responseType(typeURL string) string {
	if typeURL == "" {
		return c.cfg.TypeURL
	}
	return typeURL
}
