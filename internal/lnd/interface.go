package lnd

import (
	"context"

	"github.com/lightningnetwork/lnd/lnrpc"
	"github.com/lightningnetwork/lnd/lnrpc/routerrpc"
)

// LNDClient abstracts the LND gRPC methods used across the codebase,
// enabling test doubles and decoupling from the concrete Client.
type LNDClient interface {
	GetInfo(ctx context.Context) (*lnrpc.GetInfoResponse, error)
	ListChannels(ctx context.Context) ([]*lnrpc.Channel, error)
	SubscribeChannelEvents(ctx context.Context) (lnrpc.Lightning_SubscribeChannelEventsClient, error)
	SubscribeHtlcEvents(ctx context.Context) (routerrpc.Router_SubscribeHtlcEventsClient, error)
	AddInvoice(ctx context.Context, amtSat int64) (paymentHash, paymentAddr []byte, err error)
	GetChanInfo(ctx context.Context, chanID uint64) (*lnrpc.ChannelEdge, error)
	HtlcInterceptor(ctx context.Context) (routerrpc.Router_HtlcInterceptorClient, error)
	SendToRouteV2(ctx context.Context, paymentHash []byte, route *lnrpc.Route) (*lnrpc.HTLCAttempt, error)
	TrackPaymentV2(ctx context.Context, paymentHash []byte) (*lnrpc.Payment, error)
	SignMessage(ctx context.Context, msg []byte) (string, error)
	VerifyMessage(ctx context.Context, msg []byte, signature string) (valid bool, pubkey string, err error)
	SendCustomMessage(ctx context.Context, peerPubkey []byte, msgType uint32, data []byte) error
	SubscribeCustomMessages(ctx context.Context) (lnrpc.Lightning_SubscribeCustomMessagesClient, error)
}

// Verify Client implements LNDClient at compile time.
var _ LNDClient = (*Client)(nil)
