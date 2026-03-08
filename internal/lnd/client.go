package lnd

import (
	"context"
	"crypto/x509"
	"encoding/hex"
	"fmt"
	"os"

	"github.com/lightningnetwork/lnd/lnrpc"
	"github.com/lightningnetwork/lnd/lnrpc/routerrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// Client wraps the LND Lightning and Router gRPC services.
type Client struct {
	conn      *grpc.ClientConn
	lightning lnrpc.LightningClient
	router    routerrpc.RouterClient
}

func NewClient(host, certPath, macaroonPath string) (*Client, error) {
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

	return &Client{
		conn:      conn,
		lightning: lnrpc.NewLightningClient(conn),
		router:    routerrpc.NewRouterClient(conn),
	}, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) GetInfo(ctx context.Context) (*lnrpc.GetInfoResponse, error) {
	return c.lightning.GetInfo(ctx, &lnrpc.GetInfoRequest{})
}

func (c *Client) ListChannels(ctx context.Context) ([]*lnrpc.Channel, error) {
	resp, err := c.lightning.ListChannels(ctx, &lnrpc.ListChannelsRequest{})
	if err != nil {
		return nil, err
	}
	return resp.Channels, nil
}

func (c *Client) SubscribeChannelEvents(ctx context.Context) (lnrpc.Lightning_SubscribeChannelEventsClient, error) {
	return c.lightning.SubscribeChannelEvents(ctx, &lnrpc.ChannelEventSubscription{})
}

func (c *Client) SubscribeHtlcEvents(ctx context.Context) (routerrpc.Router_SubscribeHtlcEventsClient, error) {
	return c.router.SubscribeHtlcEvents(ctx, &routerrpc.SubscribeHtlcEventsRequest{})
}

func (c *Client) AddInvoice(ctx context.Context, amtSat int64) (paymentHash, paymentAddr []byte, err error) {
	resp, err := c.lightning.AddInvoice(ctx, &lnrpc.Invoice{
		Value: amtSat,
	})
	if err != nil {
		return nil, nil, err
	}
	decoded, err := c.lightning.DecodePayReq(ctx, &lnrpc.PayReqString{PayReq: resp.PaymentRequest})
	if err != nil {
		return nil, nil, fmt.Errorf("decode pay req: %w", err)
	}
	return resp.RHash, decoded.PaymentAddr, nil
}

func (c *Client) GetChanInfo(ctx context.Context, chanID uint64) (*lnrpc.ChannelEdge, error) {
	return c.lightning.GetChanInfo(ctx, &lnrpc.ChanInfoRequest{ChanId: chanID})
}

func (c *Client) HtlcInterceptor(ctx context.Context) (routerrpc.Router_HtlcInterceptorClient, error) {
	return c.router.HtlcInterceptor(ctx)
}

func (c *Client) SendToRouteV2(ctx context.Context, paymentHash []byte, route *lnrpc.Route) (*lnrpc.HTLCAttempt, error) {
	return c.router.SendToRouteV2(ctx, &routerrpc.SendToRouteRequest{
		PaymentHash: paymentHash,
		Route:       route,
	})
}

func (c *Client) TrackPaymentV2(ctx context.Context, paymentHash []byte) (*lnrpc.Payment, error) {
	stream, err := c.router.TrackPaymentV2(ctx, &routerrpc.TrackPaymentRequest{
		PaymentHash:       paymentHash,
		NoInflightUpdates: true,
	})
	if err != nil {
		return nil, err
	}
	// With NoInflightUpdates, the first Recv returns the terminal state.
	return stream.Recv()
}

func (c *Client) SignMessage(ctx context.Context, msg []byte) (string, error) {
	resp, err := c.lightning.SignMessage(ctx, &lnrpc.SignMessageRequest{Msg: msg})
	if err != nil {
		return "", err
	}
	return resp.Signature, nil
}

func (c *Client) VerifyMessage(ctx context.Context, msg []byte, signature string) (bool, string, error) {
	resp, err := c.lightning.VerifyMessage(ctx, &lnrpc.VerifyMessageRequest{Msg: msg, Signature: signature})
	if err != nil {
		return false, "", err
	}
	return resp.Valid, resp.Pubkey, nil
}

func (c *Client) SendCustomMessage(ctx context.Context, peerPubkey []byte, msgType uint32, data []byte) error {
	_, err := c.lightning.SendCustomMessage(ctx, &lnrpc.SendCustomMessageRequest{
		Peer: peerPubkey,
		Type: msgType,
		Data: data,
	})
	return err
}

func (c *Client) SubscribeCustomMessages(ctx context.Context) (lnrpc.Lightning_SubscribeCustomMessagesClient, error) {
	return c.lightning.SubscribeCustomMessages(ctx, &lnrpc.SubscribeCustomMessagesRequest{})
}

// macaroonCred implements grpc.PerRPCCredentials, injecting the macaroon
// auth token into every gRPC request's metadata.
type macaroonCred struct {
	macaroon string
}

func (m *macaroonCred) GetRequestMetadata(_ context.Context, _ ...string) (map[string]string, error) {
	return map[string]string{"macaroon": m.macaroon}, nil
}

func (m *macaroonCred) RequireTransportSecurity() bool {
	return true
}
