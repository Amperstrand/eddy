package gossip

import (
	"testing"
	"time"

	"github.com/eddy/internal/graph"
	"github.com/lightningnetwork/lnd/lnrpc"
)

func TestChanStateFromChannel(t *testing.T) {
	ch := &lnrpc.Channel{
		Capacity:     1000000,
		LocalBalance: 700000,
		RemotePubkey: "peer123",
		LocalConstraints: &lnrpc.ChannelConstraints{
			MinHtlcMsat:       1000,
			MaxPendingAmtMsat: 500000000,
		},
	}

	st := ChanStateFromChannel(ch)
	if st.Delta != 200000 {
		t.Fatalf("expected delta 200000, got %d", st.Delta)
	}
	if st.MinHtlc != 1000 {
		t.Fatalf("expected minHtlc 1000, got %d", st.MinHtlc)
	}
	if st.MaxHtlc != 500000000 {
		t.Fatalf("expected maxHtlc 500000000, got %d", st.MaxHtlc)
	}
	if st.PeerID != "peer123" {
		t.Fatalf("expected peer123, got %s", st.PeerID)
	}
}

func TestCheckDedup(t *testing.T) {
	s := &Service{
		dedup: make(map[gossipKey]int64),
		peers: make(map[string]bool),
	}

	if !s.CheckDedup("origin1", 1, 100) {
		t.Fatal("expected first message to be accepted")
	}
	if s.CheckDedup("origin1", 1, 100) {
		t.Fatal("expected same timestamp to be rejected")
	}
	if s.CheckDedup("origin1", 1, 50) {
		t.Fatal("expected older to be rejected")
	}
	if !s.CheckDedup("origin1", 1, 200) {
		t.Fatal("expected newer to be accepted")
	}
	if !s.CheckDedup("origin1", 2, 50) {
		t.Fatal("expected different channel to be accepted")
	}
}

func TestCleanupDedup(t *testing.T) {
	s := &Service{
		dedup:          make(map[gossipKey]int64),
		peers:          make(map[string]bool),
		staleThreshold: 60,
	}

	now := time.Now().Unix()
	s.dedup[gossipKey{origin: "a", channelID: 1}] = now
	s.dedup[gossipKey{origin: "b", channelID: 2}] = now - 120

	s.CleanupDedup()

	if len(s.dedup) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(s.dedup))
	}
	if _, ok := s.dedup[gossipKey{origin: "a", channelID: 1}]; !ok {
		t.Fatal("fresh entry should remain")
	}
}

func TestHandleGossipState(t *testing.T) {
	g := graph.NewLocalGraph()
	s := &Service{
		graph:  g,
		selfID: "self",
		dedup:  make(map[gossipKey]int64),
		peers:  make(map[string]bool),
	}

	now := time.Now().Unix()
	if !s.CheckDedup("nodeA", 42, now) {
		t.Fatal("should accept")
	}
	g.UpsertEdge("nodeA", "nodeB", 42, 500, 1000, 5000000, now)

	cycle := g.FindCycles("nodeA", 5, 60)
	if cycle != nil {
		t.Fatal("no cycle expected")
	}
}
