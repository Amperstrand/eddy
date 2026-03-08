package p2p

import (
	"encoding/json"
	"fmt"
)

// Custom message type ID used for all P2P messages via LND.
// We use a single type ID and multiplex via the envelope Type field.
const CustomMsgType uint32 = 55001

// ProtocolVersion is the current protocol version. Peers with different
// major versions are incompatible and their messages are dropped.
const ProtocolVersion int = 1

// Envelope is the wire format for all P2P messages.
type Envelope struct {
	Version int             `json:"version,omitempty"`
	Type    string          `json:"type"`
	Data    json.RawMessage `json:"data"`
}

// --- Gossip messages ---

// GossipState announces a channel's current state.
type GossipState struct {
	OriginNodeID string `json:"origin_node_id"`
	ChannelID    uint64 `json:"channel_id"`
	PeerNodeID   string `json:"peer_node_id"`
	Delta        int64  `json:"delta"`
	MinHtlc      int64  `json:"min_htlc"`
	MaxHtlc      int64  `json:"max_htlc"`
	Timestamp    int64  `json:"timestamp"`
	TTL          int    `json:"ttl"`
	Signature    string `json:"signature,omitempty"`
}

// SignableBytes returns the canonical bytes to sign/verify. TTL is excluded
// because each relay decrements it, so including it would invalidate the
// original signature. Signature itself is obviously also excluded.
func (gs *GossipState) SignableBytes() []byte {
	return []byte(fmt.Sprintf("gossip_state:%s:%d:%s:%d:%d:%d:%d",
		gs.OriginNodeID, gs.ChannelID, gs.PeerNodeID,
		gs.Delta, gs.MinHtlc, gs.MaxHtlc, gs.Timestamp))
}

// GossipClose announces a channel closure.
type GossipClose struct {
	OriginNodeID string `json:"origin_node_id"`
	ChannelID    uint64 `json:"channel_id"`
	PeerNodeID   string `json:"peer_node_id"`
	TTL          int    `json:"ttl"`
	Timestamp    int64  `json:"timestamp"`
	Signature    string `json:"signature,omitempty"`
}

// SignableBytes returns the canonical bytes to sign/verify.
func (gc *GossipClose) SignableBytes() []byte {
	return []byte(fmt.Sprintf("gossip_close:%s:%d:%s:%d",
		gc.OriginNodeID, gc.ChannelID, gc.PeerNodeID, gc.Timestamp))
}

// --- Circulation messages (hop-by-hop around the cycle ring) ---

// CircPropose travels forward around the ring. Each node validates and forwards.
// Includes the payment hash so intermediates can arm their interceptors in a single pass.
type CircPropose struct {
	CycleID       string   `json:"cycle_id"`
	Initiator     string   `json:"initiator"`
	CycleNodes    []string `json:"cycle_nodes"`    // [A, B, C, A] — first==last
	CycleChannels []uint64 `json:"cycle_channels"` // [ch_AB, ch_BC, ch_CA]
	AmountSat     int64    `json:"amount_sat"`
	PaymentHash   string   `json:"payment_hash"`
}

// CircAbort can travel either direction. Cancels the circulation.
type CircAbort struct {
	CycleID string `json:"cycle_id"`
	Reason  string `json:"reason"`
}

// CircBusy is sent back when a node is already in a circulation.
type CircBusy struct {
	CycleID string `json:"cycle_id"`
}
