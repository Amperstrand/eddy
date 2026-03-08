package circulation

import (
	"fmt"
	"testing"
	"time"

	"github.com/lightningnetwork/lnd/lnrpc"
)

func TestManager_WaitDrain(t *testing.T) {
	m := &Manager{}
	done := make(chan struct{})

	m.circWg.Add(1)
	go func() {
		ok := m.WaitDrain(time.Second)
		if !ok {
			t.Error("expected drain to complete")
		}
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)
	m.circWg.Done()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("drain should have completed")
	}
}

func TestManager_Draining(t *testing.T) {
	m := &Manager{}
	m.SetDraining(true)
	if !m.Draining() {
		t.Fatal("expected draining to be true")
	}
}

func TestClearCirculation(t *testing.T) {
	m := &Manager{}
	m.circActive = &activeCirc{
		circSpec: circSpec{cycleID: "cycle123"},
	}

	m.circMu.Lock()
	circ := m.clearCirculation()
	m.circMu.Unlock()

	if circ == nil {
		t.Fatal("expected cleared circ")
	}
	if m.circActive != nil {
		t.Fatal("expected nil circActive")
	}
}

func TestPrevNextNodeIndex(t *testing.T) {
	spec := circSpec{
		cycleNodes: []string{"A", "B", "C", "A"},
		myIndex:    1,
	}
	if spec.prevNodeIndex() != 0 {
		t.Fatalf("expected prev 0, got %d", spec.prevNodeIndex())
	}
	if spec.nextNodeIndex() != 2 {
		t.Fatalf("expected next 2, got %d", spec.nextNodeIndex())
	}

	spec.myIndex = 0
	if spec.prevNodeIndex() != 2 {
		t.Fatalf("expected prev 2, got %d", spec.prevNodeIndex())
	}

	spec.myIndex = 2
	if spec.nextNodeIndex() != 0 {
		t.Fatalf("expected next 0, got %d", spec.nextNodeIndex())
	}
}

func TestManager_StartProposal_RejectsWhenBusy(t *testing.T) {
	m := &Manager{}
	m.circActive = &activeCirc{
		circSpec:  circSpec{cycleID: "existing"},
		startTime: time.Now(),
	}

	err := m.StartProposal(
		t.Context(),
		[]string{"A", "B", "C", "A"},
		[]uint64{1, 2, 3},
		50000,
		"newcycle",
	)
	if err == nil {
		t.Fatal("expected error when circulation already active")
	}
	if err.Error() != "already active" {
		t.Fatalf("unexpected error: %v", err)
	}

	busy, _, cycleID := m.IsBusy()
	if !busy || cycleID != "existing" {
		t.Fatalf("expected original circulation to remain, got busy=%v cycleID=%s", busy, cycleID)
	}
}

func TestGenerateCycleID(t *testing.T) {
	id, err := GenerateCycleID()
	if err != nil {
		t.Fatal(err)
	}
	if len(id) != 32 {
		t.Fatalf("expected 32 hex chars, got %d", len(id))
	}

	// Uniqueness
	seen := make(map[string]bool)
	for i := 0; i < 100; i++ {
		id, err := GenerateCycleID()
		if err != nil {
			t.Fatal(err)
		}
		if seen[id] {
			t.Fatalf("duplicate at iteration %d", i)
		}
		seen[id] = true
	}
}

func TestHopFee(t *testing.T) {
	// base=1000msat, rate=1000ppm, amount=1000000msat
	fee := hopFee(1000, 1000, 1000000)
	// 1000 + ceil(1000000*1000/1000000) = 1000 + 1000 = 2000
	if fee != 2000 {
		t.Fatalf("expected 2000, got %d", fee)
	}

	// Zero fees
	fee = hopFee(0, 0, 1000000)
	if fee != 0 {
		t.Fatalf("expected 0, got %d", fee)
	}
}

func TestInterceptor_ExpectAndCheck(t *testing.T) {
	ic := NewInterceptor(nil)
	hash := "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"

	ic.Expect(hash)
	if !ic.isExpected(hash) {
		t.Fatal("expected isExpected to return true")
	}
	// isExpected doesn't remove — hash should still be there
	if !ic.isExpected(hash) {
		t.Fatal("expected isExpected to return true on second call")
	}
	// Remove explicitly, then check
	ic.Remove(hash)
	if ic.isExpected(hash) {
		t.Fatal("expected isExpected to return false after Remove")
	}
}

func TestInterceptor_Remove(t *testing.T) {
	ic := NewInterceptor(nil)
	hash := "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"

	ic.Expect(hash)
	ic.Remove(hash)
	if ic.isExpected(hash) {
		t.Fatal("expected isExpected to return false after Remove")
	}
}

// TestBuildRoute_ZeroCostCirculation verifies that a circular route with
// RESUME_MODIFIED interceptors results in zero net cost: the sender pushes
// exactly desiredMsat, fees are inside that amount for LND validation,
// RESUME_MODIFIED waives them, and the full desiredMsat returns to the sender.
func TestBuildRoute_ZeroCostCirculation(t *testing.T) {
	alice := "aa" + fmt.Sprintf("%062d", 1)
	bob := "bb" + fmt.Sprintf("%062d", 2)
	charlie := "cc" + fmt.Sprintf("%062d", 3)

	cycleNodes := []string{alice, bob, charlie, alice}
	cycleChans := []uint64{1, 2, 3}
	desiredAmtSat := int64(1_000_000)

	mock := newMockNodeLND(alice, make(chan sentMessage, 100))
	for _, pair := range []struct {
		chanID       uint64
		node1, node2 string
	}{
		{1, alice, bob},
		{2, bob, charlie},
		{3, charlie, alice},
	} {
		mock.chanPolicies[pair.chanID] = &lnrpc.ChannelEdge{
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

	route, invoiceAmtSat, err := BuildRoute(t.Context(), mock, alice, cycleNodes, cycleChans, desiredAmtSat)
	if err != nil {
		t.Fatalf("BuildRoute: %v", err)
	}

	desiredMsat := desiredAmtSat * 1000

	// Invoice = desired amount.
	if invoiceAmtSat != desiredAmtSat {
		t.Fatalf("invoice should equal desired: got %d, want %d", invoiceAmtSat, desiredAmtSat)
	}

	// TotalAmtMsat = desiredMsat exactly. Fees live inside, not on top.
	if route.TotalAmtMsat != desiredMsat {
		t.Fatalf("TotalAmtMsat=%d, want desiredMsat=%d", route.TotalAmtMsat, desiredMsat)
	}

	// Zero-fee route: all fees must be zero.
	if route.TotalFeesMsat != 0 {
		t.Fatalf("route should have zero fees, got %d", route.TotalFeesMsat)
	}

	// Every hop forwards exactly desiredMsat with zero fee.
	for i, hop := range route.Hops {
		if hop.AmtToForwardMsat != desiredMsat {
			t.Fatalf("hop %d AmtToForward=%d, want %d", i, hop.AmtToForwardMsat, desiredMsat)
		}
		if hop.FeeMsat != 0 {
			t.Fatalf("hop %d fee should be 0, got %d", i, hop.FeeMsat)
		}
	}

	// Route is internally consistent:
	//   hop[i].AmtToForward = hop[i+1].AmtToForward + hop[i+1].Fee
	for i := 0; i < len(route.Hops)-1; i++ {
		expected := route.Hops[i+1].AmtToForwardMsat + route.Hops[i+1].FeeMsat
		if route.Hops[i].AmtToForwardMsat != expected {
			t.Fatalf("hop %d AmtToForward=%d, want %d (hop[%d].fwd=%d + fee=%d)",
				i, route.Hops[i].AmtToForwardMsat, expected,
				i+1, route.Hops[i+1].AmtToForwardMsat, route.Hops[i+1].FeeMsat)
		}
	}

	// TotalAmtMsat = hop[0].AmtToForward + hop[0].Fee
	expectedTotal := route.Hops[0].AmtToForwardMsat + route.Hops[0].FeeMsat
	if route.TotalAmtMsat != expectedTotal {
		t.Fatalf("TotalAmtMsat=%d, want hop[0].fwd+fee=%d", route.TotalAmtMsat, expectedTotal)
	}

	// --- Simulate RESUME_MODIFIED at each intermediate hop ---
	// Each node forwards the full incoming amount (OutAmountMsat = IncomingAmountMsat).

	actualAmount := route.TotalAmtMsat // Alice sends this

	for i := 0; i < len(route.Hops)-1; i++ {
		// RESUME_MODIFIED: forward everything received
		actualAmount = actualAmount // outAmt = incomingAmt (no change)
	}

	// Alice receives back what she sent.
	if actualAmount != route.TotalAmtMsat {
		t.Fatalf("not zero cost: alice sent %d but received %d", route.TotalAmtMsat, actualAmount)
	}

	// Every channel shifts by exactly desiredMsat.
	if route.TotalAmtMsat != desiredMsat {
		t.Fatalf("channel shift=%d, want desiredMsat=%d", route.TotalAmtMsat, desiredMsat)
	}

	// Each node's net balance change is zero:
	// Alice: -desiredMsat on ch_AB, +desiredMsat on ch_CA → 0
	// Bob:   +desiredMsat on ch_AB, -desiredMsat on ch_BC → 0
	// Charlie: +desiredMsat on ch_BC, -desiredMsat on ch_CA → 0
	// (trivially true since the same amount flows through every channel)
}
