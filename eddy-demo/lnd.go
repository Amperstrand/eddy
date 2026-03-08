package main

import (
	"context"
	"crypto/x509"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/lightningnetwork/lnd/lnrpc"
	"github.com/lightningnetwork/lnd/lnrpc/routerrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type NodeInfo struct {
	Alias  string
	Pubkey string
}

type LNDClient struct {
	conn      *grpc.ClientConn
	lightning lnrpc.LightningClient
	router    routerrpc.RouterClient
	info      NodeInfo
}

func NewLNDClient(host, certPath, macaroonPath string) (*LNDClient, error) {
	certBytes, err := os.ReadFile(certPath)
	if err != nil {
		return nil, fmt.Errorf("read tls cert: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(certBytes) {
		return nil, fmt.Errorf("failed to parse tls cert")
	}
	tlsCreds := credentials.NewClientTLSFromCert(pool, "")

	macBytes, err := os.ReadFile(macaroonPath)
	if err != nil {
		return nil, fmt.Errorf("read macaroon: %w", err)
	}
	macHex := hex.EncodeToString(macBytes)

	conn, err := grpc.NewClient(host,
		grpc.WithTransportCredentials(tlsCreds),
		grpc.WithPerRPCCredentials(&macaroonCred{macaroon: macHex}),
	)
	if err != nil {
		return nil, fmt.Errorf("grpc dial: %w", err)
	}

	return &LNDClient{
		conn:      conn,
		lightning: lnrpc.NewLightningClient(conn),
		router:    routerrpc.NewRouterClient(conn),
	}, nil
}

func (c *LNDClient) Close() error { return c.conn.Close() }

func (c *LNDClient) GetInfo(ctx context.Context) (*NodeInfo, error) {
	resp, err := c.lightning.GetInfo(ctx, &lnrpc.GetInfoRequest{})
	if err != nil {
		return nil, err
	}
	c.info = NodeInfo{Alias: resp.Alias, Pubkey: resp.IdentityPubkey}
	return &c.info, nil
}

func (c *LNDClient) ListChannels(ctx context.Context) ([]*lnrpc.Channel, error) {
	resp, err := c.lightning.ListChannels(ctx, &lnrpc.ListChannelsRequest{})
	if err != nil {
		return nil, err
	}
	return resp.Channels, nil
}

func (c *LNDClient) AddInvoice(ctx context.Context, amtSat int64) (string, error) {
	resp, err := c.lightning.AddInvoice(ctx, &lnrpc.Invoice{Value: amtSat})
	if err != nil {
		return "", err
	}
	return resp.PaymentRequest, nil
}

func (c *LNDClient) ConnectPeer(ctx context.Context, pubkey, host string) error {
	_, err := c.lightning.ConnectPeer(ctx, &lnrpc.ConnectPeerRequest{
		Addr: &lnrpc.LightningAddress{Pubkey: pubkey, Host: host},
	})
	if err != nil && !strings.Contains(err.Error(), "already connected") {
		return err
	}
	return nil
}

// EnsurePeerConnected looks up the target node's advertised addresses in the
// graph and attempts to connect. Ignores "already connected" errors.
func (c *LNDClient) EnsurePeerConnected(ctx context.Context, pubkey string) error {
	nodeInfo, err := c.lightning.GetNodeInfo(ctx, &lnrpc.NodeInfoRequest{PubKey: pubkey})
	if err != nil {
		return fmt.Errorf("get node info for %s: %w", pubkey[:16], err)
	}
	for _, addr := range nodeInfo.Node.Addresses {
		if err := c.ConnectPeer(ctx, pubkey, addr.Addr); err == nil {
			return nil
		}
	}
	// May already be connected — verify via ListPeers.
	peers, err := c.lightning.ListPeers(ctx, &lnrpc.ListPeersRequest{})
	if err != nil {
		return err
	}
	for _, p := range peers.Peers {
		if p.PubKey == pubkey {
			return nil
		}
	}
	return fmt.Errorf("could not connect to peer %s", pubkey[:16])
}

func (c *LNDClient) SendCustomMessage(ctx context.Context, peerPubkey string, msgType uint32, data []byte) error {
	peerBytes, err := hex.DecodeString(peerPubkey)
	if err != nil {
		return fmt.Errorf("decode peer pubkey: %w", err)
	}
	_, err = c.lightning.SendCustomMessage(ctx, &lnrpc.SendCustomMessageRequest{
		Peer: peerBytes,
		Type: msgType,
		Data: data,
	})
	return err
}

func (c *LNDClient) SendPaymentSync(ctx context.Context, payReq string) error {
	_, err := c.lightning.SendPaymentSync(ctx, &lnrpc.SendRequest{
		PaymentRequest: payReq,
	})
	return err
}

type macaroonCred struct {
	macaroon string
}

func (m *macaroonCred) GetRequestMetadata(_ context.Context, _ ...string) (map[string]string, error) {
	return map[string]string{"macaroon": m.macaroon}, nil
}

func (m *macaroonCred) RequireTransportSecurity() bool { return true }

// MultiClient manages connections to all 4 LND nodes.
type MultiClient struct {
	Nodes   []*LNDClient
	Aliases map[string]string // pubkey -> alias
	mu      sync.RWMutex
}

func NewMultiClient(configs []NodeConfig) (*MultiClient, error) {
	mc := &MultiClient{
		Aliases: make(map[string]string),
	}
	for _, cfg := range configs {
		c, err := NewLNDClient(cfg.Host, cfg.CertPath, cfg.MacaroonPath)
		if err != nil {
			return nil, fmt.Errorf("connect %s: %w", cfg.Alias, err)
		}
		info, err := c.GetInfo(context.Background())
		if err != nil {
			return nil, fmt.Errorf("getinfo %s: %w", cfg.Alias, err)
		}
		if cfg.Alias != "" {
			c.info.Alias = cfg.Alias
			info.Alias = cfg.Alias
		}
		mc.Nodes = append(mc.Nodes, c)
		mc.Aliases[info.Pubkey] = info.Alias
	}
	return mc, nil
}

func (mc *MultiClient) Close() {
	for _, n := range mc.Nodes {
		n.Close()
	}
}

func (mc *MultiClient) AliasFor(pubkey string) string {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	if a, ok := mc.Aliases[pubkey]; ok {
		return a
	}
	if len(pubkey) > 8 {
		return pubkey[:8]
	}
	return pubkey
}

type NodeConfig struct {
	Alias        string
	Host         string
	CertPath     string
	MacaroonPath string
}
