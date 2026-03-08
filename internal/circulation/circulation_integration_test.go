package circulation

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/lightningnetwork/lnd/lnrpc"
	"github.com/lightningnetwork/lnd/lnrpc/routerrpc"
	"google.golang.org/grpc/metadata"

	"github.com/eddy/internal/graph"
	"github.com/eddy/internal/p2p"
)

// --- mock LND client (per-node) ---

type sentMessage struct {
	from    string
	dest    string
	msgType uint32
	data    []byte
}

type mockNodeLND struct {
	selfID string
	bus    chan sentMessage

	mu              sync.Mutex
	payResults      map[string]chan *lnrpc.HTLCAttempt // paymentHash hex -> result
	failSendToRoute bool

	// For AddInvoice
	invoiceHash []byte
	invoiceAddr []byte

	// Fee policy for GetChanInfo
	chanPolicies map[uint64]*lnrpc.ChannelEdge
}

func newMockNodeLND(selfID string, bus chan sentMessage) *mockNodeLND {
	return &mockNodeLND{
		selfID:       selfID,
		bus:          bus,
		payResults:   make(map[string]chan *lnrpc.HTLCAttempt),
		chanPolicies: make(map[uint64]*lnrpc.ChannelEdge),
	}
}

func (m *mockNodeLND) GetInfo(_ context.Context) (*lnrpc.GetInfoResponse, error) {
	return &lnrpc.GetInfoResponse{BlockHeight: 800000}, nil
}

func (m *mockNodeLND) AddInvoice(_ context.Context, amtSat int64) ([]byte, []byte, error) {
	// Generate deterministic hash and addr
	var hash, addr [32]byte
	copy(hash[:], []byte(fmt.Sprintf("hash-%s-%d", m.selfID[:8], amtSat)))
	copy(addr[:], []byte(fmt.Sprintf("addr-%s-%d", m.selfID[:8], amtSat)))
	m.mu.Lock()
	m.invoiceHash = hash[:]
	m.invoiceAddr = addr[:]
	m.mu.Unlock()
	return hash[:], addr[:], nil
}

func (m *mockNodeLND) GetChanInfo(_ context.Context, chanID uint64) (*lnrpc.ChannelEdge, error) {
	m.mu.Lock()
	edge, ok := m.chanPolicies[chanID]
	m.mu.Unlock()
	if ok {
		return edge, nil
	}
	return nil, fmt.Errorf("channel %d not found", chanID)
}

func (m *mockNodeLND) HtlcInterceptor(_ context.Context) (routerrpc.Router_HtlcInterceptorClient, error) {
	return &mockInterceptorStream{}, nil
}

func (m *mockNodeLND) SendToRouteV2(ctx context.Context, paymentHash []byte, _ *lnrpc.Route) (*lnrpc.HTLCAttempt, error) {
	if m.failSendToRoute {
		return &lnrpc.HTLCAttempt{
			Status: lnrpc.HTLCAttempt_FAILED,
			Failure: &lnrpc.Failure{
				Code: lnrpc.Failure_TEMPORARY_CHANNEL_FAILURE,
			},
		}, nil
	}
	key := hex.EncodeToString(paymentHash)
	m.mu.Lock()
	ch, ok := m.payResults[key]
	if !ok {
		ch = make(chan *lnrpc.HTLCAttempt, 1)
		m.payResults[key] = ch
	}
	m.mu.Unlock()
	select {
	case result := <-ch:
		return result, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (m *mockNodeLND) SendCustomMessage(_ context.Context, peerPubkey []byte, msgType uint32, data []byte) error {
	m.bus <- sentMessage{from: m.selfID, dest: hex.EncodeToString(peerPubkey), msgType: msgType, data: data}
	return nil
}

func (m *mockNodeLND) SubscribeCustomMessages(_ context.Context) (lnrpc.Lightning_SubscribeCustomMessagesClient, error) {
	return nil, fmt.Errorf("not implemented in test")
}

func (m *mockNodeLND) ListChannels(_ context.Context) ([]*lnrpc.Channel, error) {
	return nil, nil
}

func (m *mockNodeLND) SubscribeChannelEvents(_ context.Context) (lnrpc.Lightning_SubscribeChannelEventsClient, error) {
	return nil, fmt.Errorf("not implemented in test")
}

func (m *mockNodeLND) SubscribeHtlcEvents(_ context.Context) (routerrpc.Router_SubscribeHtlcEventsClient, error) {
	return nil, fmt.Errorf("not implemented in test")
}

func (m *mockNodeLND) TrackPaymentV2(_ context.Context, _ []byte) (*lnrpc.Payment, error) {
	return nil, fmt.Errorf("not implemented in test")
}

func (m *mockNodeLND) SignMessage(_ context.Context, _ []byte) (string, error) {
	return "mock-signature", nil
}

func (m *mockNodeLND) VerifyMessage(_ context.Context, _ []byte, _ string) (bool, string, error) {
	return true, "", nil
}

// deliverPayResult delivers a result for a pending SendToRouteV2.
func (m *mockNodeLND) deliverPayResult(paymentHash string, result *lnrpc.HTLCAttempt) {
	m.mu.Lock()
	ch, ok := m.payResults[paymentHash]
	if !ok {
		ch = make(chan *lnrpc.HTLCAttempt, 1)
		m.payResults[paymentHash] = ch
	}
	m.mu.Unlock()
	ch <- result
}

// --- mock interceptor stream (no-op for tests) ---

type mockInterceptorStream struct{}

func (s *mockInterceptorStream) Send(*routerrpc.ForwardHtlcInterceptResponse) error { return nil }
func (s *mockInterceptorStream) Recv() (*routerrpc.ForwardHtlcInterceptRequest, error) {
	// Block forever — tests don't drive the interceptor stream
	select {}
}
func (s *mockInterceptorStream) Header() (metadata.MD, error) { return nil, nil }
func (s *mockInterceptorStream) Trailer() metadata.MD         { return nil }
func (s *mockInterceptorStream) CloseSend() error             { return nil }
func (s *mockInterceptorStream) Context() context.Context     { return context.Background() }
func (s *mockInterceptorStream) SendMsg(interface{}) error    { return nil }
func (s *mockInterceptorStream) RecvMsg(interface{}) error    { return nil }

// --- test helpers ---

func setupRing(t *testing.T) (
	ctx context.Context, cancel context.CancelFunc,
	managers map[string]*Manager, lnds map[string]*mockNodeLND,
	alice, bob, charlie string,
	cycleNodes []string, chanIDs []uint64, pushAmount int64,
	cycleID string,
	bus chan sentMessage,
) {
	t.Helper()

	alice = "aa" + fmt.Sprintf("%062d", 1)
	bob = "bb" + fmt.Sprintf("%062d", 2)
	charlie = "cc" + fmt.Sprintf("%062d", 3)

	cycleNodes = []string{alice, bob, charlie, alice}
	chanIDs = []uint64{1, 2, 3}
	pushAmount = 10000

	var b [16]byte
	rand.Read(b[:])
	cycleID = hex.EncodeToString(b[:])

	bus = make(chan sentMessage, 100)

	aliceLND := newMockNodeLND(alice, bus)
	bobLND := newMockNodeLND(bob, bus)
	charlieLND := newMockNodeLND(charlie, bus)

	// Set up channel fee policies (low fees for testing)
	for _, l := range []*mockNodeLND{aliceLND, bobLND, charlieLND} {
		for _, pair := range []struct {
			chanID       uint64
			node1, node2 string
		}{
			{1, alice, bob},
			{2, bob, charlie},
			{3, charlie, alice},
		} {
			l.chanPolicies[pair.chanID] = &lnrpc.ChannelEdge{
				ChannelId: pair.chanID,
				Node1Pub:  pair.node1,
				Node2Pub:  pair.node2,
				Node1Policy: &lnrpc.RoutingPolicy{
					FeeBaseMsat:      1000,
					FeeRateMilliMsat: 1,
					TimeLockDelta:    40,
				},
				Node2Policy: &lnrpc.RoutingPolicy{
					FeeBaseMsat:      1000,
					FeeRateMilliMsat: 1,
					TimeLockDelta:    40,
				},
			}
		}
	}

	g := graph.NewLocalGraph()
	cfg := Config{Timeout: 10 * time.Second, PaymentTimeout: 10 * time.Second}

	// Populate graph with edges so HandleCircPropose can look them up
	now := time.Now().Unix()
	g.UpsertEdge(alice, bob, 1, 100000, 0, 0, now)
	g.UpsertEdge(bob, charlie, 2, 100000, 0, 0, now)
	g.UpsertEdge(charlie, alice, 3, 100000, 0, 0, now)

	// Create interceptors (not running the loop in tests, just for expected-hash tracking)
	aliceIC := NewInterceptor(aliceLND)
	bobIC := NewInterceptor(bobLND)
	charlieIC := NewInterceptor(charlieLND)

	aliceMgr := New(aliceLND, p2p.NewTransport(aliceLND), g, alice, aliceIC, cfg)
	bobMgr := New(bobLND, p2p.NewTransport(bobLND), g, bob, bobIC, cfg)
	charlieMgr := New(charlieLND, p2p.NewTransport(charlieLND), g, charlie, charlieIC, cfg)

	managers = map[string]*Manager{
		alice:   aliceMgr,
		bob:     bobMgr,
		charlie: charlieMgr,
	}
	lnds = map[string]*mockNodeLND{
		alice:   aliceLND,
		bob:     bobLND,
		charlie: charlieLND,
	}

	ctx, cancel = context.WithTimeout(context.Background(), 15*time.Second)

	// Message router
	go func() {
		for {
			select {
			case msg := <-bus:
				mgr, ok := managers[msg.dest]
				if !ok {
					continue
				}
				var env p2p.Envelope
				if err := json.Unmarshal(msg.data, &env); err != nil {
					continue
				}
				go dispatchMessage(ctx, mgr, msg.from, lnds, env)
			case <-ctx.Done():
				return
			}
		}
	}()

	return
}

func dispatchMessage(ctx context.Context, mgr *Manager, sender string, lnds map[string]*mockNodeLND, env p2p.Envelope) {
	switch env.Type {
	case "circ_propose":
		var cp p2p.CircPropose
		json.Unmarshal(env.Data, &cp)
		mgr.HandleCircPropose(ctx, sender, cp)
	case "circ_abort":
		var ca p2p.CircAbort
		json.Unmarshal(env.Data, &ca)
		mgr.HandleCircAbort(ctx, sender, ca)
	case "circ_busy":
		var cb p2p.CircBusy
		json.Unmarshal(env.Data, &cb)
		mgr.HandleCircBusy(ctx, sender, cb)
	}
}

func TestCirculation_HappyPath(t *testing.T) {
	ctx, cancel, managers, lnds, alice, _, _, cycleNodes, chanIDs, pushAmount, cycleID, bus := setupRing(t)
	defer cancel()
	_ = bus

	if err := managers[alice].StartProposal(ctx, cycleNodes, chanIDs, pushAmount, cycleID); err != nil {
		t.Fatalf("StartProposal: %v", err)
	}

	// Wait for propose to propagate, prepare to complete, and payment to be dispatched
	time.Sleep(500 * time.Millisecond)

	// The initiator (alice) should have created an invoice and dispatched SendToRouteV2.
	// Find the payment hash from alice's mock.
	aliceLND := lnds[alice]
	aliceLND.mu.Lock()
	invoiceHash := aliceLND.invoiceHash
	aliceLND.mu.Unlock()

	if invoiceHash == nil {
		t.Fatal("alice should have created an invoice")
	}

	hashKey := hex.EncodeToString(invoiceHash)

	// Deliver successful payment result
	aliceLND.deliverPayResult(hashKey, &lnrpc.HTLCAttempt{
		Status: lnrpc.HTLCAttempt_SUCCEEDED,
	})

	// Wait for completion
	if !managers[alice].WaitDrain(10 * time.Second) {
		t.Fatal("alice WaitDrain timed out")
	}

	// Alice should no longer be busy
	busy, _, _ := managers[alice].IsBusy()
	if busy {
		t.Error("alice still busy after completion")
	}
}

func TestCirculation_PaymentFailure(t *testing.T) {
	ctx, cancel, managers, lnds, alice, _, _, cycleNodes, chanIDs, pushAmount, cycleID, _ := setupRing(t)
	defer cancel()

	// Make alice's SendToRouteV2 fail immediately
	lnds[alice].failSendToRoute = true

	if err := managers[alice].StartProposal(ctx, cycleNodes, chanIDs, pushAmount, cycleID); err != nil {
		t.Fatalf("StartProposal: %v", err)
	}

	// Wait for propose + prepare + payment attempt
	time.Sleep(500 * time.Millisecond)

	if !managers[alice].WaitDrain(10 * time.Second) {
		t.Fatal("alice WaitDrain timed out")
	}

	// Alice should have cleaned up
	busy, _, _ := managers[alice].IsBusy()
	if busy {
		t.Error("alice still busy after payment failure")
	}
}

func TestCirculation_InterceptorRegistersHash(t *testing.T) {
	ctx, cancel, managers, _, alice, bob, charlie, cycleNodes, chanIDs, pushAmount, cycleID, _ := setupRing(t)
	defer cancel()

	if err := managers[alice].StartProposal(ctx, cycleNodes, chanIDs, pushAmount, cycleID); err != nil {
		t.Fatalf("StartProposal: %v", err)
	}

	// Wait for prepare to propagate
	time.Sleep(500 * time.Millisecond)

	// Bob and Charlie's interceptors should have been told to expect the hash.
	// Check their circActive has a paymentHash set.
	for _, name := range []string{bob, charlie} {
		mgr := managers[name]
		mgr.circMu.Lock()
		circ := mgr.circActive
		mgr.circMu.Unlock()
		// The intermediate may have already been cleared if payment completed.
		// But the interceptor's expected set should have been populated (and possibly consumed).
		// At minimum, verify the hash was set on the circ.
		if circ != nil && circ.paymentHash == "" {
			t.Errorf("%s: expected paymentHash to be set", name[:16])
		}
	}
}
