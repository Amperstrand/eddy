package circulation

import (
	"context"
	"encoding/hex"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/lightningnetwork/lnd/lnrpc"
	"github.com/lightningnetwork/lnd/lnrpc/routerrpc"

	"github.com/eddy/internal/lnd"
)

// Interceptor manages the HTLC interceptor stream, waiving fees for
// expected circulation payment hashes via RESUME_MODIFIED.
type Interceptor struct {
	client lnd.LNDClient
	pubkey string // our node's identity pubkey, set during Run

	mu        sync.Mutex
	expected  map[string]bool // payment hash hex → true
	onForward func(paymentHash string)
}

// NewInterceptor creates a new HTLC interceptor.
func NewInterceptor(client lnd.LNDClient) *Interceptor {
	return &Interceptor{
		client:   client,
		expected: make(map[string]bool),
	}
}

// SetOnForward sets a callback invoked after a circulation HTLC is
// successfully forwarded via RESUME_MODIFIED. The callback receives the
// payment hash hex so the caller can clear related state.
func (ic *Interceptor) SetOnForward(fn func(paymentHash string)) {
	ic.mu.Lock()
	ic.onForward = fn
	ic.mu.Unlock()
}

// Expect registers a payment hash so that when an HTLC with this hash
// arrives, the interceptor will use RESUME_MODIFIED to waive fees.
func (ic *Interceptor) Expect(paymentHash string) {
	ic.mu.Lock()
	ic.expected[paymentHash] = true
	ic.mu.Unlock()
}

// Remove removes a payment hash from the expected set.
func (ic *Interceptor) Remove(paymentHash string) {
	ic.mu.Lock()
	delete(ic.expected, paymentHash)
	ic.mu.Unlock()
}

// isExpected checks if a hash is in the expected set.
func (ic *Interceptor) isExpected(paymentHash string) bool {
	ic.mu.Lock()
	defer ic.mu.Unlock()
	return ic.expected[paymentHash]
}

// Run starts the HTLC interceptor loop. Reconnects on stream failure.
// Blocks until ctx is cancelled.
func (ic *Interceptor) Run(ctx context.Context) error {
	backoff := time.Second
	info, err := ic.client.GetInfo(ctx)
	if err != nil {
		return fmt.Errorf("interceptor: GetInfo: %w", err)
	}
	ic.pubkey = info.IdentityPubkey

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		stream, err := ic.client.HtlcInterceptor(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			slog.Error("interceptor: connect failed, retrying", "error", err)
			select {
			case <-time.After(backoff):
				backoff = min(backoff*2, 30*time.Second)
			case <-ctx.Done():
				return ctx.Err()
			}
			continue
		}
		backoff = time.Second
		slog.Info("interceptor: stream connected")

		if err := ic.recvLoop(ctx, stream); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			slog.Error("interceptor: stream broke, reconnecting", "error", err)
		}
	}
}

// outgoingFeePolicy returns (baseMsat, ratePPM) for our node's direction on the given channel.
func (ic *Interceptor) outgoingFeePolicy(ctx context.Context, chanID uint64) (int64, int64, error) {
	edge, err := ic.client.GetChanInfo(ctx, chanID)
	if err != nil {
		return 0, 0, err
	}
	var pol *lnrpc.RoutingPolicy
	if edge.Node1Pub == ic.pubkey {
		pol = edge.Node1Policy
	} else {
		pol = edge.Node2Policy
	}
	if pol == nil {
		return 0, 0, fmt.Errorf("no policy for %s on channel %d", ic.pubkey, chanID)
	}
	return pol.FeeBaseMsat, pol.FeeRateMilliMsat, nil
}

func (ic *Interceptor) recvLoop(ctx context.Context, stream routerrpc.Router_HtlcInterceptorClient) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}

		hashHex := hex.EncodeToString(req.PaymentHash)

		if ic.isExpected(hashHex) {
			// Waive our forwarding fee: forward the full incoming amount.
			outAmt := req.IncomingAmountMsat

			// LND validates: InAmountMsat - OutAmountMsat >= required_fee(OutAmountMsat)
			// Compute required_fee using our actual fee policy on the outgoing channel.
			baseMsat, ratePPM, err := ic.outgoingFeePolicy(ctx, req.OutgoingRequestedChanId)
			if err != nil {
				slog.Error("interceptor: fee policy lookup failed, falling back to RESUME",
					"hash", hashHex[:16], "error", err)
				if err := stream.Send(&routerrpc.ForwardHtlcInterceptResponse{
					IncomingCircuitKey: req.IncomingCircuitKey,
					Action:             routerrpc.ResolveHoldForwardAction_RESUME,
				}); err != nil {
					return fmt.Errorf("send RESUME: %w", err)
				}
				continue
			}
			requiredFee := hopFee(baseMsat, ratePPM, int64(outAmt))
			inAmt := outAmt + uint64(requiredFee)
			slog.Info("interceptor: RESUME_MODIFIED (waiving fees)", "hash", hashHex[:16],
				"in", req.IncomingAmountMsat, "out", outAmt, "faked_in", inAmt)
			if err := stream.Send(&routerrpc.ForwardHtlcInterceptResponse{
				IncomingCircuitKey: req.IncomingCircuitKey,
				Action:             routerrpc.ResolveHoldForwardAction_RESUME_MODIFIED,
				InAmountMsat:       inAmt,
				OutAmountMsat:      outAmt,
			}); err != nil {
				return fmt.Errorf("send RESUME_MODIFIED: %w", err)
			}
			// Only remove after successful send — if the stream broke before
			// Send completed, the HTLC will reappear on the new stream and
			// we need to waive fees again.
			ic.Remove(hashHex)
			ic.mu.Lock()
			fn := ic.onForward
			ic.mu.Unlock()
			if fn != nil {
				fn(hashHex)
			}
		} else {
			if err := stream.Send(&routerrpc.ForwardHtlcInterceptResponse{
				IncomingCircuitKey: req.IncomingCircuitKey,
				Action:             routerrpc.ResolveHoldForwardAction_RESUME,
			}); err != nil {
				return fmt.Errorf("send RESUME: %w", err)
			}
		}
	}
}
