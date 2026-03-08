package p2p

import (
	"encoding/json"
	"testing"
)

func TestEnvelopeMarshal(t *testing.T) {
	data, _ := json.Marshal(map[string]string{"foo": "bar"})
	env := Envelope{Version: ProtocolVersion, Type: "test", Data: data}

	b, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var got Envelope
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Type != "test" {
		t.Fatalf("type mismatch: %s", got.Type)
	}
	if string(got.Data) != string(env.Data) {
		t.Fatalf("data mismatch: %s vs %s", got.Data, env.Data)
	}
}

func TestGossipStateJSON(t *testing.T) {
	gs := GossipState{
		OriginNodeID: "abc123",
		ChannelID:    42,
		PeerNodeID:   "def456",
		Delta:        -500,
		MinHtlc:      1000,
		MaxHtlc:      5000000,
		Timestamp:    1700000000,
		TTL:          5,
	}
	b, err := json.Marshal(gs)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got GossipState
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got != gs {
		t.Fatalf("round-trip mismatch: %+v vs %+v", got, gs)
	}
}

func TestGossipCloseJSON(t *testing.T) {
	gc := GossipClose{
		OriginNodeID: "abc",
		ChannelID:    99,
		PeerNodeID:   "def",
		TTL:          3,
		Timestamp:    1700000000,
	}
	b, err := json.Marshal(gc)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got GossipClose
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got != gc {
		t.Fatalf("mismatch: %+v vs %+v", got, gc)
	}
}

func TestCircProposeJSON(t *testing.T) {
	cp := CircPropose{
		CycleID:       "cycle001",
		Initiator:     "nodeA",
		CycleNodes:    []string{"A", "B", "C", "A"},
		CycleChannels: []uint64{1, 2, 3},
		AmountSat:     50000,
	}
	b, err := json.Marshal(cp)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got CircPropose
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.CycleID != cp.CycleID || got.AmountSat != cp.AmountSat {
		t.Fatalf("mismatch")
	}
	if len(got.CycleNodes) != 4 || len(got.CycleChannels) != 3 {
		t.Fatalf("slice lengths wrong")
	}
}

func TestEnvelopeVersionField(t *testing.T) {
	// Version 0 (omitted) decodes to zero value
	raw := `{"type":"test","data":{}}`
	var env Envelope
	if err := json.Unmarshal([]byte(raw), &env); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if env.Version != 0 {
		t.Fatalf("expected version 0 for missing field, got %d", env.Version)
	}

	// Explicit version round-trips
	env2 := Envelope{Version: ProtocolVersion, Type: "test", Data: json.RawMessage(`{}`)}
	b, err := json.Marshal(env2)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got Envelope
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Version != ProtocolVersion {
		t.Fatalf("version mismatch: got %d want %d", got.Version, ProtocolVersion)
	}
}

func TestEnvelopeUnknownType(t *testing.T) {
	raw := `{"type":"unknown_future_type","data":{"x":1}}`
	var env Envelope
	if err := json.Unmarshal([]byte(raw), &env); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if env.Type != "unknown_future_type" {
		t.Fatalf("type: %s", env.Type)
	}
	// Data should be preserved as raw JSON
	if string(env.Data) != `{"x":1}` {
		t.Fatalf("data not preserved: %s", env.Data)
	}
}
