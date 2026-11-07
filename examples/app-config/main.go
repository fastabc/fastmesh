// app-config synchronizes business configuration using fastmesh's public APIs.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/fastabc/fastmesh/controller"
	"github.com/fastabc/fastmesh/controller/enrollment"
	"github.com/fastabc/fastmesh/model"
	"github.com/fastabc/fastmesh/node"
	"github.com/fastabc/fastmesh/store/file"
)

type options struct {
	role, name, dir, controller, listen, advertise, http, config string
	poll                                                         time.Duration
}

func main() {
	var o options
	flag.StringVar(&o.role, "role", "app", "setup, controller, or app")
	flag.StringVar(&o.name, "name", "app-1", "app-1 or app-2 (app identity)")
	flag.StringVar(&o.dir, "dir", "config-demo", "credentials and local state directory")
	flag.StringVar(&o.controller, "controller", "127.0.0.1:19190", "controller dial address; also listen address in controller mode")
	flag.StringVar(&o.listen, "config-listen", "127.0.0.1:19191", "controller's mesh config service bind address")
	flag.StringVar(&o.advertise, "advertise", "127.0.0.1:19191", "config service IP:port reachable by all apps")
	flag.StringVar(&o.http, "http", "127.0.0.1:8081", "app's local HTTP demo/status address")
	flag.StringVar(&o.config, "config", "", "controller source file (default: dir/app.json)")
	flag.DurationVar(&o.poll, "poll", 2*time.Second, "app config polling interval")
	flag.Parse()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx, o); err != nil && !errors.Is(err, context.Canceled) {
		log.Print(err)
		os.Exit(1)
	}
}

func run(ctx context.Context, o options) error {
	switch o.role {
	case "setup":
		return setup(o.dir)
	case "controller":
		return runController(ctx, o)
	case "app":
		if o.name != "app-1" && o.name != "app-2" || o.poll <= 0 {
			return errors.New("app requires name app-1/app-2 and a positive poll interval")
		}
		return runApp(ctx, o)
	default:
		return errors.New("role must be setup, controller or app")
	}
}

func newNode(o options, name, token string) (*node.Node, error) {
	return node.New(node.Config{
		Identity: fileIdentity{dir: o.dir, role: name}, Controllers: []string{o.controller},
		AllowedControllerIDs: []string{id("controller").String()}, TrustDomain: domain,
		Tenant: "default", JoinToken: token, StateDir: filepath.Join(o.dir, name+"-state"),
		ListenAddress: o.listen, AdvertiseAddress: o.advertise,
		ConnectTimeout: 2 * time.Second, DrainTimeout: time.Second,
	})
}

func runController(ctx context.Context, o options) error {
	if o.config == "" {
		o.config = filepath.Join(o.dir, "app.json")
	}
	backing, err := file.Open(filepath.Join(o.dir, "controller-store.json"))
	if err != nil {
		return err
	}
	defer backing.Close()
	c, err := controller.New(controller.Config{ID: "app-config-demo", Objects: backing, Leases: backing, Identity: fileIdentity{o.dir, "controller"}, TrustDomain: domain})
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", o.controller)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	var serveErr error
	go func() { serveErr = c.Serve(ctx, listener); close(done) }()
	defer func() { cancel(); <-done }()
	for !c.Active() {
		select {
		case <-done:
			return serveErr
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
	if err := c.PutService(ctx, model.Service{Tenant: "default", Name: "app-config", RequiresL7: true, Ports: []model.ServicePort{{Name: "http", Protocol: "http"}}}); err != nil {
		return err
	}
	if err := c.PutBindGrant(ctx, "config-provider", model.BindGrant{Tenant: "default", Service: "app-config", Workload: model.Principal{ID: id("controller")}}); err != nil {
		return err
	}
	for _, name := range []string{"app-1", "app-2"} {
		if err := c.PutServiceAccess(ctx, name, model.ServiceAccess{Tenant: "default", Service: "app-config", Port: "http", Source: model.Principal{ID: id(name)}, L7: &model.HTTPMatch{Method: "GET", Path: "/config"}}); err != nil {
			return err
		}
	}
	// This short-lived demo token is shared by the three enrolled nodes.
	token, err := c.CreateJoinToken(ctx, enrollment.CreateRequest{Tenant: "default", TTL: time.Hour})
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(o.dir, "join-token"), []byte(token.Raw), 0600); err != nil {
		return err
	}
	provider, err := newNode(o, "controller", token.Raw)
	if err != nil {
		return err
	}
	defer provider.Close()
	if err := provider.Start(ctx); err != nil {
		return err
	}
	bound, err := provider.HTTPHandler(ctx, node.ServiceRef{Service: "app-config", Port: "http"}, configHandler(o.config))
	if err != nil {
		return err
	}
	defer bound.Close()
	log.Printf("controller-ready address=%s config-service=%s source=%s", o.controller, o.advertise, o.config)
	select {
	case <-done:
		return serveErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

func runApp(ctx context.Context, o options) error {
	token, err := os.ReadFile(filepath.Join(o.dir, "join-token"))
	if err != nil {
		return err
	}
	// Apps only dial the mesh config service; they publish no mesh listener.
	n, err := newNode(o, o.name, strings.TrimSpace(string(token)))
	if err != nil {
		return err
	}
	defer n.Close()
	if err := n.Start(ctx); err != nil {
		return err
	}
	client := n.HTTPClient(node.ServiceRef{Service: "app-config", Port: "http"})
	client.Timeout = 3 * time.Second
	defer client.CloseIdleConnections()
	state := &configState{}
	listener, err := net.Listen("tcp", o.http)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	pollDone := make(chan struct{})
	go func() {
		defer close(pollDone)
		ticker := time.NewTicker(o.poll)
		defer ticker.Stop()
		for {
			changed, err := state.refresh(ctx, client)
			if err != nil && ctx.Err() == nil {
				log.Printf("config-sync-failed instance=%s error=%v (retaining last valid config)", o.name, err)
			} else if changed {
				log.Printf("config-applied instance=%s revision=%d", o.name, state.current.Load().Revision)
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	defer func() { cancel(); <-pollDone }()
	server := &http.Server{Handler: state.handler(o.name), ReadHeaderTimeout: 3 * time.Second}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	log.Printf("app-listening instance=%s address=%s (503 until first config)", o.name, listener.Addr())
	select {
	case err := <-done:
		return fmt.Errorf("app HTTP server: %w", err)
	case <-ctx.Done():
		stop, release := context.WithTimeout(context.Background(), time.Second)
		defer release()
		err := server.Shutdown(stop)
		if err != nil {
			_ = server.Close()
		}
		<-done
		return err
	}
}
