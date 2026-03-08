package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	tea "github.com/charmbracelet/bubbletea"
)

func main() {
	networkDir := flag.String("network", "", "path to extracted polar network directory")
	eddydBin := flag.String("eddyd", "", "path to eddyd binary (empty = skip eddyd)")
	noTraffic := flag.Bool("no-traffic", false, "disable payment traffic generator")
	flag.Parse()

	if *networkDir == "" {
		// Try default location
		home, _ := os.UserHomeDir()
		*networkDir = filepath.Join(home, "eddy-demo", "network")
	}

	configs, err := discoverNodes(*networkDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to discover nodes: %v\n", err)
		fmt.Fprintf(os.Stderr, "Make sure the Polar network is running and the path is correct.\n")
		os.Exit(1)
	}

	fmt.Fprintf(os.Stderr, "Connecting to %d nodes...\n", len(configs))

	mc, err := NewMultiClient(configs)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to connect: %v\n", err)
		os.Exit(1)
	}
	defer mc.Close()

	for _, n := range mc.Nodes {
		fmt.Fprintf(os.Stderr, "  Connected: %s (%s...)\n", n.info.Alias, n.info.Pubkey[:16])
	}

	// Ensure all nodes are peered, then test custom messages.
	if len(mc.Nodes) >= 2 {
		for i, a := range mc.Nodes {
			for j, b := range mc.Nodes {
				if i >= j {
					continue
				}
				if err := a.EnsurePeerConnected(context.Background(), b.info.Pubkey); err != nil {
					fmt.Fprintf(os.Stderr, "  Peer connect %s->%s: %v\n", a.info.Alias, b.info.Alias, err)
				}
			}
		}
		err := mc.Nodes[0].SendCustomMessage(context.Background(), mc.Nodes[1].info.Pubkey, 55001, []byte("test"))
		if err != nil {
			fmt.Fprintf(os.Stderr, "  Custom message test FAILED: %v\n", err)
		} else {
			fmt.Fprintf(os.Stderr, "  Custom message test OK\n")
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	monitor := NewMonitor(mc)

	// Initial poll
	if err := monitor.Poll(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "Initial poll failed: %v\n", err)
		os.Exit(1)
	}

	// Start balance polling
	go monitor.Run(ctx, 2*time.Second)

	// Start eddyd processes
	var eddydMgr *EddydManager
	if *eddydBin != "" {
		eddydMgr = NewEddydManager(monitor)
		if err := eddydMgr.Start(configs, *eddydBin); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to start eddyd: %v\n", err)
		} else {
			fmt.Fprintf(os.Stderr, "  Started %d eddyd instances\n", len(configs))
		}
	}

	// Start traffic generator
	traffic := NewTrafficGenerator(mc, monitor)
	if !*noTraffic {
		go traffic.Run(ctx, 1*time.Second, 1*time.Second)
	}

	// Handle signals
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		cancel()
		if eddydMgr != nil {
			eddydMgr.Stop()
		}
	}()

	// Launch TUI
	p := tea.NewProgram(newModel(monitor, traffic), tea.WithAltScreen())
	if _, err := p.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "TUI error: %v\n", err)
	}

	cancel()
	if eddydMgr != nil {
		eddydMgr.Stop()

		fmt.Fprintf(os.Stderr, "Delete logs? [Y/n] ")
		var answer string
		fmt.Scanln(&answer)
		answer = strings.TrimSpace(strings.ToLower(answer))
		if answer == "" || answer == "y" || answer == "yes" {
			eddydMgr.Cleanup()
			fmt.Fprintf(os.Stderr, "Cleaned up eddyd data.\n")
		}
	}
}

// discoverNodes finds LND node credentials in a Polar network directory.
func discoverNodes(networkDir string) ([]NodeConfig, error) {
	// Polar stores volumes under .polar/networks/<id>/volumes/lnd/
	// The directory structure varies, so we search for tls.cert and admin.macaroon files

	type candidate struct {
		alias    string
		certPath string
		macPath  string
		port     string
	}

	// Standard Polar node names and ports
	nodeNames := []struct {
		name string
		port string
	}{
		{"alice", "10001"},
		{"bob", "10002"},
		{"carol", "10003"},
		{"dave", "10004"},
	}

	var configs []NodeConfig
	for _, nn := range nodeNames {
		// Try common Polar volume paths
		var certPath, macPath string
		for _, base := range []string{
			filepath.Join(networkDir, "volumes", "lnd", nn.name),
			filepath.Join(networkDir, nn.name),
		} {
			cert := filepath.Join(base, "tls.cert")
			mac := filepath.Join(base, "data", "chain", "bitcoin", "regtest", "admin.macaroon")
			if _, err := os.Stat(cert); err == nil {
				if _, err := os.Stat(mac); err == nil {
					certPath = cert
					macPath = mac
					break
				}
			}
		}
		if certPath == "" {
			// Try to find by globbing
			matches, _ := filepath.Glob(filepath.Join(networkDir, "**", nn.name, "tls.cert"))
			if len(matches) == 0 {
				matches, _ = filepath.Glob(filepath.Join(networkDir, "*", "*", nn.name, "tls.cert"))
			}
			if len(matches) > 0 {
				certPath = matches[0]
				dir := filepath.Dir(certPath)
				macPath = filepath.Join(dir, "data", "chain", "bitcoin", "regtest", "admin.macaroon")
			}
		}

		if certPath == "" || macPath == "" {
			continue
		}

		configs = append(configs, NodeConfig{
			Alias:        nn.name,
			Host:         "localhost:" + nn.port,
			CertPath:     certPath,
			MacaroonPath: macPath,
		})
	}

	if len(configs) == 0 {
		// Fall back to manual specification via env vars
		for _, nn := range nodeNames {
			envPrefix := strings.ToUpper(nn.name)
			cert := os.Getenv(envPrefix + "_CERT")
			mac := os.Getenv(envPrefix + "_MACAROON")
			host := os.Getenv(envPrefix + "_HOST")
			if cert != "" && mac != "" {
				if host == "" {
					host = "localhost:" + nn.port
				}
				configs = append(configs, NodeConfig{
					Alias:        nn.name,
					Host:         host,
					CertPath:     cert,
					MacaroonPath: mac,
				})
			}
		}
	}

	if len(configs) == 0 {
		return nil, fmt.Errorf("no LND nodes found in %s (also checked env vars ALICE_CERT etc.)", networkDir)
	}

	return configs, nil
}
