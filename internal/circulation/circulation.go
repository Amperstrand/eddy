package circulation

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/eddy/internal/graph"
	"github.com/eddy/internal/lnd"
	"github.com/eddy/internal/p2p"
	"github.com/eddy/internal/shortid"
	"github.com/lightningnetwork/lnd/lnrpc"
)

// circSpec holds the immutable identity and topology of a circulation.
// Set once at creation and safe to read without holding the Manager lock.
type circSpec struct {
	cycleID     string
	initiator   string
	cycleNodes  []string
	cycleChans  []uint64
	amountSat   int64
	paymentHash string
	isInitiator bool
	myIndex     int
}

// prevNodeIndex returns the previous node index in the cycle, wrapping around.
func (c *circSpec) prevNodeIndex() int {
	numNodes := len(c.cycleNodes) - 1
	if c.myIndex == 0 {
		return numNodes - 1
	}
	return c.myIndex - 1
}

// nextNodeIndex returns the next node index in the cycle, wrapping around.
func (c *circSpec) nextNodeIndex() int {
	numNodes := len(c.cycleNodes) - 1
	if c.myIndex+1 >= numNodes {
		return 0
	}
	return c.myIndex + 1
}

// activeCirc extends circSpec with mutable, per-attempt state (route, timing).
// Only the initiator populates route and paymentHashBytes.
type activeCirc struct {
	circSpec // immutable after creation

	route            *lnrpc.Route
	paymentHashBytes []byte
	startTime        time.Time
}

// Config holds circulation timing configuration.
type Config struct {
	Timeout        time.Duration `yaml:"timeout"`
	PaymentTimeout time.Duration `yaml:"payment_timeout"`
}

// proposeTimeout is the worst-case time for a propose message to propagate
// hop-by-hop around the ring (numNodes hops).
func (c Config) proposeTimeout(numNodes int) time.Duration {
	return c.Timeout * time.Duration(numNodes)
}

// LifecycleTimeout is the total worst-case lifetime of a circulation:
// propose propagation + payment execution.
func (c Config) LifecycleTimeout(numNodes int) time.Duration {
	return c.proposeTimeout(numNodes) + c.PaymentTimeout
}

// Manager owns the circulation protocol state.
type Manager struct {
	client      lnd.LNDClient
	transport   *p2p.Transport
	graphRef    *graph.LocalGraph
	selfID      string
	interceptor *Interceptor
	cfg         Config

	circMu     sync.Mutex
	circActive *activeCirc
	circWg     sync.WaitGroup

	draining atomic.Bool
}

// New creates a new circulation Manager.
func New(client lnd.LNDClient, transport *p2p.Transport, g *graph.LocalGraph, selfID string, interceptor *Interceptor, cfg Config) *Manager {
	mgr := &Manager{
		client:      client,
		transport:   transport,
		graphRef:    g,
		selfID:      selfID,
		interceptor: interceptor,
		cfg:         cfg,
	}
	if interceptor != nil {
		interceptor.SetOnForward(mgr.ClearByPaymentHash)
	}
	return mgr
}

// Draining returns whether the manager is draining.
func (m *Manager) Draining() bool { return m.draining.Load() }

// SetDraining sets the draining flag.
func (m *Manager) SetDraining(v bool) { m.draining.Store(v) }

// WaitDrain waits for all in-flight circulations to complete, up to timeout.
func (m *Manager) WaitDrain(timeout time.Duration) bool {
	done := make(chan struct{})
	go func() { m.circWg.Wait(); close(done) }()
	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false
	}
}

// IsBusy returns true if there's an active circulation, along with its start time.
func (m *Manager) IsBusy() (busy bool, startTime time.Time, cycleID string) {
	m.circMu.Lock()
	defer m.circMu.Unlock()
	if m.circActive != nil {
		return true, m.circActive.startTime, m.circActive.cycleID
	}
	return false, time.Time{}, ""
}

// ClearStale clears an active circulation if it matches the given cycleID.
// The payment hash is set at proposal time (before any HTLC is dispatched),
// so its presence alone doesn't indicate an in-flight HTLC. The only time
// an HTLC is actually in flight is inside executePayment, which clears the
// circulation itself on completion.
func (m *Manager) ClearStale(cycleID string) {
	m.circMu.Lock()
	if m.circActive != nil && m.circActive.cycleID == cycleID {
		slog.Warn("solver: clearing stale circulation", "cycle_id", shortid.Short(cycleID))
		m.clearCirculation()
	}
	m.circMu.Unlock()
}

// ClearByPaymentHash clears the active circulation if it matches the given
// payment hash and this node is not the initiator. Called by the interceptor's
// onForward callback when an HTLC is successfully forwarded, so intermediate
// nodes don't have to wait for the watchdog timer.
func (m *Manager) ClearByPaymentHash(paymentHash string) {
	m.circMu.Lock()
	defer m.circMu.Unlock()
	if m.circActive == nil || m.circActive.paymentHash != paymentHash {
		return
	}
	if m.circActive.isInitiator {
		return // initiator clears via executePayment
	}
	slog.Info("circulation: cleared by HTLC forward", "cycle_id", shortid.Short(m.circActive.cycleID))
	m.circActive = nil
}

// GenerateCycleID generates a random cycle ID.
func GenerateCycleID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

// StartProposal creates a new circulation as initiator and sends the propose message.
// It builds the route and creates the invoice upfront so the payment hash can travel
// with the propose message, eliminating the need for a separate prepare phase.
func (m *Manager) StartProposal(ctx context.Context, cycleNodes []string, chanIDs []uint64, pushAmount int64, cycleID string) error {
	m.circMu.Lock()
	if m.circActive != nil {
		m.circMu.Unlock()
		return fmt.Errorf("already active")
	}
	m.circMu.Unlock()

	// Build route and create invoice before acquiring the slot, so we don't
	// hold the circulation lock during potentially slow RPCs.
	route, invoiceAmtSat, err := BuildRoute(ctx, m.client, m.selfID, cycleNodes, chanIDs, pushAmount)
	if err != nil {
		return fmt.Errorf("build route: %w", err)
	}

	paymentHash, paymentAddr, err := m.client.AddInvoice(ctx, invoiceAmtSat)
	if err != nil {
		return fmt.Errorf("add invoice: %w", err)
	}
	paymentHashHex := hex.EncodeToString(paymentHash)

	// MPP record on the last hop is required by LND even for single-path
	// payments — it carries the payment address that proves invoice ownership.
	lastHop := route.Hops[len(route.Hops)-1]
	lastHop.MppRecord = &lnrpc.MPPRecord{
		PaymentAddr:  paymentAddr,
		TotalAmtMsat: invoiceAmtSat * 1000,
	}

	m.circMu.Lock()
	if m.circActive != nil {
		m.circMu.Unlock()
		return fmt.Errorf("already active")
	}
	m.circActive = &activeCirc{
		circSpec: circSpec{
			cycleID:     cycleID,
			initiator:   m.selfID,
			cycleNodes:  cycleNodes,
			cycleChans:  chanIDs,
			amountSat:   pushAmount,
			paymentHash: paymentHashHex,
			isInitiator: true,
			myIndex:     0,
		},
		route:            route,
		paymentHashBytes: paymentHash,
		startTime:        time.Now(),
	}
	m.circMu.Unlock()

	// Arm the interceptor so that when the HTLC circles back to us,
	// it gets RESUME_MODIFIED (fee waiver) instead of normal forwarding.
	if m.interceptor != nil {
		m.interceptor.Expect(paymentHashHex)
	}

	nextHop := cycleNodes[1]
	propose := p2p.CircPropose{
		CycleID:       cycleID,
		Initiator:     m.selfID,
		CycleNodes:    cycleNodes,
		CycleChannels: chanIDs,
		AmountSat:     pushAmount,
		PaymentHash:   paymentHashHex,
	}
	slog.Info("solver: sending circ_propose", "cycle_id", shortid.Short(cycleID), "next_hop", shortid.Short(nextHop), "invoice_amt", invoiceAmtSat)
	if err := m.transport.Send(ctx, nextHop, "circ_propose", propose); err != nil {
		m.circMu.Lock()
		m.clearCirculation()
		m.circMu.Unlock()
		return err
	}
	return nil
}

func (m *Manager) getActiveCirc(cycleID string) *activeCirc {
	if m.circActive != nil && m.circActive.cycleID == cycleID {
		return m.circActive
	}
	return nil
}

func (m *Manager) clearCirculation() *activeCirc {
	circ := m.circActive
	if circ == nil {
		return nil
	}
	if circ.paymentHash != "" && m.interceptor != nil {
		m.interceptor.Remove(circ.paymentHash)
	}
	m.circActive = nil
	return circ
}

func (m *Manager) abortCirculation(ctx context.Context, circ *activeCirc, reason string) {
	m.circMu.Lock()
	if m.getActiveCirc(circ.cycleID) == nil {
		m.circMu.Unlock()
		return
	}
	slog.Error("circulation: ABORT", "cycle_id", shortid.Short(circ.cycleID), "reason", reason)
	m.clearCirculation()
	m.circMu.Unlock()
	m.propagateAbort(ctx, circ, "", p2p.CircAbort{CycleID: circ.cycleID, Reason: reason})
}

// propagateAbort sends an abort message to both ring neighbors (prev and next),
// excluding the peer that sent us the abort (to avoid echo loops).
func (m *Manager) propagateAbort(ctx context.Context, circ *activeCirc, excludePeer string, abort p2p.CircAbort) {
	prev := circ.cycleNodes[circ.prevNodeIndex()]
	next := circ.cycleNodes[circ.nextNodeIndex()]
	if prev != excludePeer && prev != m.selfID {
		m.transport.Send(ctx, prev, "circ_abort", abort)
	}
	if next != excludePeer && next != m.selfID && next != prev {
		m.transport.Send(ctx, next, "circ_abort", abort)
	}
}

func (m *Manager) HandleCircPropose(ctx context.Context, sender string, cp p2p.CircPropose) {
	if len(cp.CycleNodes) < 2 || len(cp.CycleChannels) != len(cp.CycleNodes)-1 {
		slog.Warn("circ_propose: invalid cycle dimensions", "nodes", len(cp.CycleNodes), "chans", len(cp.CycleChannels))
		return
	}
	if m.draining.Load() && cp.Initiator != m.selfID {
		slog.Warn("circ_propose: draining, rejecting", "cycle_id", shortid.Short(cp.CycleID))
		m.transport.Send(ctx, sender, "circ_busy", p2p.CircBusy{CycleID: cp.CycleID})
		return
	}
	m.circMu.Lock()

	if cp.Initiator == m.selfID {
		circ := m.getActiveCirc(cp.CycleID)
		if circ == nil {
			m.circMu.Unlock()
			slog.Warn("circ_propose: stale return", "cycle_id", shortid.Short(cp.CycleID))
			return
		}
		m.circMu.Unlock()
		slog.Info("circ_propose: ring complete, executing payment", "cycle_id", shortid.Short(cp.CycleID))
		m.executePayment(ctx, circ)
		return
	}

	myIdx := -1
	for i, n := range cp.CycleNodes {
		if n == m.selfID && i < len(cp.CycleNodes)-1 {
			myIdx = i
			break
		}
	}
	if myIdx < 0 {
		m.circMu.Unlock()
		slog.Warn("circ_propose: not in cycle", "cycle_id", shortid.Short(cp.CycleID))
		return
	}

	prevIdx := myIdx - 1
	if prevIdx < 0 {
		prevIdx = len(cp.CycleNodes) - 2
	}
	if sender != cp.CycleNodes[prevIdx] {
		m.circMu.Unlock()
		slog.Warn("circ_propose: sender is not previous node in ring", "cycle_id", shortid.Short(cp.CycleID), "sender", sender, "expected", cp.CycleNodes[prevIdx])
		return
	}

	if m.circActive != nil {
		if m.circActive.paymentHash != "" {
			// Payment hash is set, meaning the interceptor is armed and an
			// HTLC may be in flight. Never preempt in this state.
			m.circMu.Unlock()
			slog.Warn("circ_propose: busy (interceptor armed), rejecting", "cycle_id", shortid.Short(cp.CycleID))
			m.transport.Send(ctx, sender, "circ_busy", p2p.CircBusy{CycleID: cp.CycleID})
			return
		}
		if cp.CycleID < m.circActive.cycleID {
			slog.Info("circ_propose: preempting", "old_cycle", shortid.Short(m.circActive.cycleID), "new_cycle", shortid.Short(cp.CycleID))
			m.clearCirculation()
		} else {
			m.circMu.Unlock()
			slog.Warn("circ_propose: busy, rejecting", "cycle_id", shortid.Short(cp.CycleID))
			m.transport.Send(ctx, sender, "circ_busy", p2p.CircBusy{CycleID: cp.CycleID})
			return
		}
	}

	outChanID := cp.CycleChannels[myIdx]
	edge := m.graphRef.GetEdge(outChanID, m.selfID)
	if edge == nil {
		m.circMu.Unlock()
		slog.Warn("circ_propose: unknown channel", "cycle_id", shortid.Short(cp.CycleID), "channel", outChanID)
		m.transport.Send(ctx, sender, "circ_abort", p2p.CircAbort{CycleID: cp.CycleID, Reason: "unknown channel"})
		return
	}
	delta := edge.Delta
	expectedNext := cp.CycleNodes[myIdx+1]
	if edge.To != expectedNext {
		m.circMu.Unlock()
		slog.Warn("circ_propose: channel peer mismatch", "cycle_id", shortid.Short(cp.CycleID), "channel", outChanID)
		m.transport.Send(ctx, sender, "circ_abort", p2p.CircAbort{CycleID: cp.CycleID, Reason: "channel peer mismatch"})
		return
	}
	if delta <= 0 || cp.AmountSat > delta {
		m.circMu.Unlock()
		slog.Warn("circ_propose: insufficient delta", "cycle_id", shortid.Short(cp.CycleID), "delta", delta, "amount", cp.AmountSat)
		m.transport.Send(ctx, sender, "circ_abort", p2p.CircAbort{CycleID: cp.CycleID, Reason: "insufficient delta"})
		return
	}

	m.circActive = &activeCirc{
		circSpec: circSpec{
			cycleID:     cp.CycleID,
			initiator:   cp.Initiator,
			cycleNodes:  cp.CycleNodes,
			cycleChans:  cp.CycleChannels,
			amountSat:   cp.AmountSat,
			paymentHash: cp.PaymentHash,
			isInitiator: false,
			myIndex:     myIdx,
		},
		startTime: time.Now(),
	}
	m.circMu.Unlock()

	// Arm interceptor with the payment hash from the propose message
	if m.interceptor != nil && cp.PaymentHash != "" {
		m.interceptor.Expect(cp.PaymentHash)
	}

	numNodes := len(cp.CycleNodes) - 1
	time.AfterFunc(m.cfg.proposeTimeout(numNodes), func() {
		m.circMu.Lock()
		if m.getActiveCirc(cp.CycleID) == nil {
			m.circMu.Unlock()
			return
		}
		slog.Warn("circ_propose: watchdog clearing stale circulation", "cycle_id", shortid.Short(cp.CycleID))
		m.clearCirculation()
		m.circMu.Unlock()
	})

	nextHop := cp.CycleNodes[myIdx+1]
	slog.Info("circ_propose: accepted, forwarding", "cycle_id", shortid.Short(cp.CycleID), "next_hop", shortid.Short(nextHop))
	if err := m.transport.Send(ctx, nextHop, "circ_propose", cp); err != nil {
		slog.Error("circ_propose: forward failed", "error", err)
		m.circMu.Lock()
		m.clearCirculation()
		m.circMu.Unlock()
	}
}

// executePayment runs the payment as initiator after propose completes the ring.
func (m *Manager) executePayment(ctx context.Context, circ *activeCirc) {
	route := circ.route
	paymentHash := circ.paymentHashBytes
	if route == nil || paymentHash == nil {
		slog.Error("circulation: no route/hash on active circ", "cycle_id", shortid.Short(circ.cycleID))
		m.abortCirculation(ctx, circ, "no route or payment hash")
		return
	}

	m.circWg.Add(1)
	go func() {
		defer m.circWg.Done()

		slog.Info("circulation: sending payment", "cycle_id", shortid.Short(circ.cycleID), "amount_sats", circ.amountSat)

		attempt, err := m.client.SendToRouteV2(ctx, paymentHash, route)
		if err != nil {
			// RPC error doesn't mean the HTLC wasn't dispatched. Ask LND
			// for the actual payment state before deciding.
			slog.Warn("circulation: SendToRouteV2 RPC error, tracking payment", "cycle_id", shortid.Short(circ.cycleID), "error", err)
			attempt, err = m.resolvePayment(ctx, circ, paymentHash)
			if err != nil {
				slog.Error("circulation: unable to resolve payment state, will keep polling", "cycle_id", shortid.Short(circ.cycleID), "error", err)
				go m.pollPaymentUntilResolved(circ, paymentHash)
				return
			}
		}

		if attempt.Status == lnrpc.HTLCAttempt_FAILED {
			slog.Error("circulation: payment FAILED", "cycle_id", shortid.Short(circ.cycleID), "failure", attempt.Failure)
			m.circMu.Lock()
			m.clearCirculation()
			m.circMu.Unlock()
			return
		}

		slog.Info("circulation: COMPLETE", "cycle_id", shortid.Short(circ.cycleID), "amount_sats", circ.amountSat)

		m.circMu.Lock()
		var edges []graph.CycleEdge
		for i, cid := range circ.cycleChans {
			edges = append(edges, graph.CycleEdge{
				ChannelID: cid,
				From:      circ.cycleNodes[i],
				To:        circ.cycleNodes[i+1],
			})
		}
		m.clearCirculation()
		m.circMu.Unlock()
		m.graphRef.InvalidateEdges(edges)
	}()
}

func (m *Manager) HandleCircAbort(ctx context.Context, sender string, ca p2p.CircAbort) {
	m.circMu.Lock()
	circ := m.getActiveCirc(ca.CycleID)
	if circ == nil {
		m.circMu.Unlock()
		return
	}
	// Abort can come from either neighbor in the ring.
	prev := circ.cycleNodes[circ.prevNodeIndex()]
	next := circ.cycleNodes[circ.nextNodeIndex()]
	if sender != prev && sender != next {
		m.circMu.Unlock()
		slog.Warn("circ_abort: unexpected sender", "cycle_id", shortid.Short(ca.CycleID), "sender", shortid.Short(sender))
		return
	}
	slog.Error("circulation: ABORT", "cycle_id", shortid.Short(ca.CycleID), "from", shortid.Short(sender), "reason", ca.Reason)
	circ = m.clearCirculation()
	m.circMu.Unlock()
	m.propagateAbort(ctx, circ, sender, ca)
}

func (m *Manager) HandleCircBusy(ctx context.Context, sender string, cb p2p.CircBusy) {
	m.circMu.Lock()
	circ := m.getActiveCirc(cb.CycleID)
	if circ == nil {
		m.circMu.Unlock()
		return
	}
	// Busy comes from the next hop that rejected the propose.
	expected := circ.cycleNodes[circ.nextNodeIndex()]
	if sender != expected {
		m.circMu.Unlock()
		slog.Warn("circ_busy: unexpected sender", "cycle_id", shortid.Short(cb.CycleID), "sender", shortid.Short(sender), "expected", shortid.Short(expected))
		return
	}
	slog.Warn("circulation: peer busy", "cycle_id", shortid.Short(cb.CycleID))
	m.clearCirculation()
	m.circMu.Unlock()
}

// resolvePayment queries LND for the terminal state of a payment when
// SendToRouteV2 returned an RPC error. Converts the Payment-level response
// into an HTLCAttempt so the caller can use the same status check.
func (m *Manager) resolvePayment(ctx context.Context, circ *activeCirc, paymentHash []byte) (*lnrpc.HTLCAttempt, error) {
	payment, err := m.client.TrackPaymentV2(ctx, paymentHash)
	if err != nil {
		return nil, fmt.Errorf("TrackPaymentV2: %w", err)
	}
	switch payment.Status {
	case lnrpc.Payment_SUCCEEDED:
		slog.Info("circulation: payment confirmed SUCCEEDED after RPC error", "cycle_id", shortid.Short(circ.cycleID))
		return &lnrpc.HTLCAttempt{Status: lnrpc.HTLCAttempt_SUCCEEDED}, nil
	case lnrpc.Payment_FAILED:
		slog.Info("circulation: payment confirmed FAILED after RPC error", "cycle_id", shortid.Short(circ.cycleID))
		return &lnrpc.HTLCAttempt{Status: lnrpc.HTLCAttempt_FAILED}, nil
	default:
		// IN_FLIGHT or UNKNOWN — can't clear safely.
		return nil, fmt.Errorf("payment still in state %s", payment.Status)
	}
}

// pollPaymentUntilResolved keeps querying TrackPaymentV2 until the payment
// reaches a terminal state, then clears the circulation. This runs when the
// initial resolvePayment call couldn't determine the state (e.g. IN_FLIGHT).
func (m *Manager) pollPaymentUntilResolved(circ *activeCirc, paymentHash []byte) {
	m.circWg.Add(1)
	defer m.circWg.Done()

	backoff := 5 * time.Second
	for {
		time.Sleep(backoff)
		backoff = min(backoff*2, 30*time.Second)

		payment, err := m.client.TrackPaymentV2(context.Background(), paymentHash)
		if err != nil {
			slog.Warn("circulation: poll TrackPaymentV2 error, retrying", "cycle_id", shortid.Short(circ.cycleID), "error", err)
			continue
		}

		switch payment.Status {
		case lnrpc.Payment_SUCCEEDED:
			slog.Info("circulation: polled payment SUCCEEDED", "cycle_id", shortid.Short(circ.cycleID))
			m.circMu.Lock()
			var edges []graph.CycleEdge
			for i, cid := range circ.cycleChans {
				edges = append(edges, graph.CycleEdge{
					ChannelID: cid,
					From:      circ.cycleNodes[i],
					To:        circ.cycleNodes[i+1],
				})
			}
			m.clearCirculation()
			m.circMu.Unlock()
			m.graphRef.InvalidateEdges(edges)
			return
		case lnrpc.Payment_FAILED:
			slog.Info("circulation: polled payment FAILED", "cycle_id", shortid.Short(circ.cycleID))
			m.circMu.Lock()
			m.clearCirculation()
			m.circMu.Unlock()
			return
		default:
			slog.Debug("circulation: payment still in-flight, polling again", "cycle_id", shortid.Short(circ.cycleID))
		}
	}
}
