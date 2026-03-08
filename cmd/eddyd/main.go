package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/eddy/internal/circulation"
	"github.com/eddy/internal/gossip"
	"github.com/eddy/internal/graph"
	"github.com/eddy/internal/lnd"
	"github.com/eddy/internal/p2p"
	"github.com/eddy/internal/shortid"
	"gopkg.in/yaml.v3"
)

type config struct {
	LogLevel string `yaml:"log_level"`
	DataDir  string `yaml:"data_dir"`
	Solver   struct {
		MaxCycleLength int   `yaml:"max_cycle_length"`
		StaleThreshold int   `yaml:"stale_threshold"`
		MinPushAmount  int64 `yaml:"min_push_amount"`
	} `yaml:"solver"`
	Circulation circulation.Config `yaml:"circulation"`
	Gossip      struct {
		Interval time.Duration `yaml:"interval"`
	} `yaml:"gossip"`
}

func parseLogLevel(s string) slog.Level {
	switch s {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func expandHome(path string) string {
	if strings.HasPrefix(path, "~/") {
		if home, err := os.UserHomeDir(); err == nil {
			return filepath.Join(home, path[2:])
		}
	}
	return path
}

func loadConfig(path string, explicit bool) config {
	var cfg config
	cfg.LogLevel = "info"
	cfg.Solver.MaxCycleLength = 5
	cfg.Solver.StaleThreshold = 60
	cfg.Solver.MinPushAmount = 100000
	cfg.Circulation.Timeout = 30 * time.Second
	cfg.Circulation.PaymentTimeout = 60 * time.Second
	cfg.Gossip.Interval = 30 * time.Second

	data, err := os.ReadFile(path)
	if err != nil {
		if !os.IsNotExist(err) {
			slog.Error("read config", "error", err)
			os.Exit(1)
		}
		if explicit {
			slog.Error("config file not found", "path", path)
			os.Exit(1)
		}
		slog.Warn("no config file found, using defaults", "path", path)
		return cfg
	}
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		slog.Error("parse config", "error", err)
		os.Exit(1)
	}
	return cfg
}

// eddyd is the top-level daemon state, wiring together the LND client,
// P2P transport, local graph, gossip service, and circulation manager.
type eddyd struct {
	client    *lnd.Client
	transport *p2p.Transport
	graph     *graph.LocalGraph
	selfID    string
	cfg       config

	gossip      *gossip.Service
	circ        *circulation.Manager
	interceptor *circulation.Interceptor
}

func main() {
	host := flag.String("lnd-host", "localhost:10001", "LND gRPC host:port")
	cert := flag.String("lnd-cert", "", "path to LND TLS cert")
	macaroon := flag.String("lnd-macaroon", "", "path to LND admin macaroon")
	configPath := flag.String("config", "", "path to config file")
	flag.Parse()

	configExplicit := *configPath != ""
	if !configExplicit {
		*configPath = "config.yaml"
	}

	if *cert == "" || *macaroon == "" {
		slog.Error("both --lnd-cert and --lnd-macaroon are required")
		os.Exit(1)
	}

	cfg := loadConfig(*configPath, configExplicit)

	if cfg.DataDir == "" {
		cfg.DataDir = "~/.eddy"
	}
	cfg.DataDir = expandHome(cfg.DataDir)
	if err := os.MkdirAll(cfg.DataDir, 0700); err != nil {
		slog.Error("create data dir", "error", err)
		os.Exit(1)
	}
	logFile, err := os.OpenFile(filepath.Join(cfg.DataDir, "eddy.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		slog.Error("open log file", "error", err)
		os.Exit(1)
	}
	defer logFile.Close()

	slog.SetDefault(slog.New(slog.NewTextHandler(logFile, &slog.HandlerOptions{
		Level: parseLogLevel(cfg.LogLevel),
	})))

	rootCtx, rootCancel := context.WithCancel(context.Background())
	defer rootCancel()
	// Separate context for the solver so we can stop proposing new
	// circulations on shutdown while keeping existing ones running.
	solverCtx, solverCancel := context.WithCancel(rootCtx)
	defer solverCancel()
	ctx := rootCtx

	client, err := lnd.NewClient(*host, *cert, *macaroon)
	if err != nil {
		slog.Error("connect to lnd", "error", err)
		os.Exit(1)
	}
	defer client.Close()

	info, err := client.GetInfo(ctx)
	if err != nil {
		slog.Error("getinfo", "error", err)
		os.Exit(1)
	}
	slog.Info("node started", "alias", info.Alias, "pubkey", shortid.Short(info.IdentityPubkey))

	g := graph.NewLocalGraph()
	transport := p2p.NewTransport(client)
	interceptor := circulation.NewInterceptor(client)

	d := &eddyd{
		client:      client,
		transport:   transport,
		graph:       g,
		selfID:      info.IdentityPubkey,
		cfg:         cfg,
		interceptor: interceptor,
	}

	d.gossip = gossip.New(transport, g, client, info.IdentityPubkey,
		cfg.Solver.MaxCycleLength, cfg.Solver.StaleThreshold, cfg.Gossip.Interval)

	d.circ = circulation.New(client, transport, g, info.IdentityPubkey, interceptor,
		circulation.Config{
			Timeout:        cfg.Circulation.Timeout,
			PaymentTimeout: cfg.Circulation.PaymentTimeout,
		})

	channels, err := client.ListChannels(ctx)
	if err != nil {
		slog.Error("listchannels", "error", err)
		os.Exit(1)
	}
	if len(channels) == 0 {
		slog.Info("no channels found")
	} else {
		slog.Info(fmt.Sprintf("%-20s %-68s %12s %12s %12s %12s",
			"CHANNEL_ID", "PEER", "CAPACITY", "LOCAL", "REMOTE", "DELTA"))
		for _, ch := range channels {
			mid := ch.Capacity / 2
			delta := ch.LocalBalance - mid
			slog.Info(fmt.Sprintf("%-20d %-68s %12d %12d %12d %+12d",
				ch.ChanId, ch.RemotePubkey,
				ch.Capacity, ch.LocalBalance, ch.RemoteBalance, delta))
		}
	}

	// Initial gossip (populates graph)
	d.gossip.RefreshAndGossip(ctx, channels)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		slog.Info("shutting down, stopping solver")
		d.circ.SetDraining(true)
		solverCancel()

		if d.circ.WaitDrain(d.cfg.Circulation.Timeout * 3) {
			slog.Info("drain complete")
		} else {
			slog.Warn("drain deadline exceeded, forcing shutdown")
		}
		rootCancel()
	}()

	incoming, err := d.transport.Subscribe(ctx)
	if err != nil {
		slog.Error("subscribe custom messages", "error", err)
		os.Exit(1)
	}

	go d.channelEventLoop(ctx)
	go d.htlcEventLoop(ctx)
	go d.gossip.Heartbeat(ctx)
	go d.solverLoop(solverCtx)
	go func() {
		if err := interceptor.Run(ctx); err != nil && ctx.Err() == nil {
			slog.Error("interceptor stopped", "error", err)
		}
	}()

	for {
		select {
		case msg, ok := <-incoming:
			if !ok {
				return
			}
			d.handleMessage(ctx, msg)
		case <-ctx.Done():
			return
		}
	}
}
