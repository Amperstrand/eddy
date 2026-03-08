package gossip

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/eddy/internal/graph"
	"github.com/eddy/internal/lnd"
	"github.com/eddy/internal/p2p"
	"github.com/lightningnetwork/lnd/lnrpc"
)

// ChanState holds the derived state of a local channel.
type ChanState struct {
	Delta   int64
	MinHtlc int64
	MaxHtlc int64
	PeerID  string
}

// ChanStateFromChannel derives ChanState from an LND channel.
func ChanStateFromChannel(ch *lnrpc.Channel) ChanState {
	mid := ch.Capacity / 2
	return ChanState{
		Delta:   ch.LocalBalance - mid,
		MinHtlc: int64(ch.LocalConstraints.MinHtlcMsat),
		MaxHtlc: int64(ch.LocalConstraints.MaxPendingAmtMsat),
		PeerID:  ch.RemotePubkey,
	}
}

// GossipStateFromChannel builds a GossipState message from a channel.
func GossipStateFromChannel(selfID string, ch *lnrpc.Channel, ts int64, ttl int) p2p.GossipState {
	st := ChanStateFromChannel(ch)
	return p2p.GossipState{
		OriginNodeID: selfID,
		ChannelID:    ch.ChanId,
		PeerNodeID:   ch.RemotePubkey,
		Delta:        st.Delta,
		MinHtlc:      st.MinHtlc,
		MaxHtlc:      st.MaxHtlc,
		Timestamp:    ts,
		TTL:          ttl,
	}
}

type gossipKey struct {
	origin    string
	channelID uint64
}

// Service owns gossip relay, peer tracking, and dedup state.
type Service struct {
	transport *p2p.Transport
	graph     *graph.LocalGraph
	client    lnd.LNDClient
	selfID    string

	maxTTL         int
	staleThreshold int
	interval       time.Duration

	peerMu sync.RWMutex
	peers  map[string]bool

	dedupMu sync.Mutex
	dedup   map[gossipKey]int64
}

// New creates a new gossip Service.
func New(transport *p2p.Transport, graph *graph.LocalGraph, client lnd.LNDClient, selfID string, maxTTL, staleThreshold int, interval time.Duration) *Service {
	return &Service{
		transport:      transport,
		graph:          graph,
		client:         client,
		selfID:         selfID,
		maxTTL:         maxTTL,
		staleThreshold: staleThreshold,
		interval:       interval,
		peers:          make(map[string]bool),
		dedup:          make(map[gossipKey]int64),
	}
}

// SignState signs a GossipState with the local node's key.
func (s *Service) SignState(ctx context.Context, gs *p2p.GossipState) error {
	sig, err := s.client.SignMessage(ctx, gs.SignableBytes())
	if err != nil {
		return err
	}
	gs.Signature = sig
	return nil
}

// SignClose signs a GossipClose with the local node's key.
func (s *Service) SignClose(ctx context.Context, gc *p2p.GossipClose) error {
	sig, err := s.client.SignMessage(ctx, gc.SignableBytes())
	if err != nil {
		return err
	}
	gc.Signature = sig
	return nil
}

// verifyGossipSig verifies a gossip signature and checks the recovered pubkey
// matches the claimed origin.
func (s *Service) verifyGossipSig(ctx context.Context, origin string, sig string, msg []byte) bool {
	if sig == "" {
		slog.Warn("gossip: dropping unsigned message", "origin", origin[:16])
		return false
	}
	valid, pubkey, err := s.client.VerifyMessage(ctx, msg, sig)
	if err != nil {
		slog.Warn("gossip: signature verification error", "origin", origin[:16], "error", err)
		return false
	}
	if !valid || pubkey != origin {
		slog.Warn("gossip: invalid signature", "origin", origin[:16], "recovered", pubkey[:16])
		return false
	}
	return true
}

// RelayGossip sends a gossip message to all peers except the excluded one.
func (s *Service) RelayGossip(ctx context.Context, exclude string, msgType string, msg any) {
	s.peerMu.RLock()
	defer s.peerMu.RUnlock()
	for peer := range s.peers {
		if peer != exclude {
			s.transport.Send(ctx, peer, msgType, msg)
		}
	}
}

// CheckDedup returns true if this gossip message is new (should be processed).
func (s *Service) CheckDedup(origin string, chanID uint64, ts int64) bool {
	s.dedupMu.Lock()
	defer s.dedupMu.Unlock()
	key := gossipKey{origin: origin, channelID: chanID}
	if prev, ok := s.dedup[key]; ok && prev >= ts {
		return false
	}
	s.dedup[key] = ts
	return true
}

// CleanupDedup removes dedup entries older than the stale threshold.
func (s *Service) CleanupDedup() {
	cutoff := time.Now().Unix() - int64(s.staleThreshold)
	s.dedupMu.Lock()
	for k, ts := range s.dedup {
		if ts < cutoff {
			delete(s.dedup, k)
		}
	}
	s.dedupMu.Unlock()
}

// AddPeers adds peer pubkeys to the known peers set.
func (s *Service) AddPeers(pubkeys ...string) {
	s.peerMu.Lock()
	for _, pk := range pubkeys {
		s.peers[pk] = true
	}
	s.peerMu.Unlock()
}

// RefreshAndGossip updates local state and gossips all channels to peers.
func (s *Service) RefreshAndGossip(ctx context.Context, channels []*lnrpc.Channel) {
	now := time.Now().Unix()

	peerKeys := make([]string, 0, len(channels))
	for _, ch := range channels {
		peerKeys = append(peerKeys, ch.RemotePubkey)
	}
	s.AddPeers(peerKeys...)

	for _, ch := range channels {
		st := ChanStateFromChannel(ch)
		s.graph.UpsertEdge(s.selfID, ch.RemotePubkey, ch.ChanId, st.Delta, st.MinHtlc, st.MaxHtlc, now)
	}

	for _, ch := range channels {
		gs := GossipStateFromChannel(s.selfID, ch, now, s.maxTTL)
		if err := s.SignState(ctx, &gs); err != nil {
			slog.Error("gossip: sign state", "error", err)
			continue
		}
		s.RelayGossip(ctx, ch.RemotePubkey, "gossip_state", gs)
	}
}

// Heartbeat periodically re-gossips all local channel state and cleans up dedup.
func (s *Service) Heartbeat(ctx context.Context) {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return
		}
		chs, err := s.client.ListChannels(ctx)
		if err != nil {
			slog.Error("gossip heartbeat: listchannels", "error", err)
			continue
		}
		s.RefreshAndGossip(ctx, chs)
		s.CleanupDedup()
	}
}

// HandleGossipState processes an incoming gossip_state message.
func (s *Service) HandleGossipState(ctx context.Context, sender string, gs p2p.GossipState) {
	if gs.TTL <= 0 || !s.CheckDedup(gs.OriginNodeID, gs.ChannelID, gs.Timestamp) {
		return
	}
	if !s.verifyGossipSig(ctx, gs.OriginNodeID, gs.Signature, gs.SignableBytes()) {
		return
	}
	s.graph.UpsertEdge(gs.OriginNodeID, gs.PeerNodeID, gs.ChannelID, gs.Delta, gs.MinHtlc, gs.MaxHtlc, gs.Timestamp)
	if gs.TTL > 1 {
		gs.TTL--
		s.RelayGossip(ctx, sender, "gossip_state", gs)
	}
}

// HandleGossipClose processes an incoming gossip_close message.
func (s *Service) HandleGossipClose(ctx context.Context, sender string, gc p2p.GossipClose) {
	if gc.TTL <= 0 || !s.CheckDedup(gc.OriginNodeID, gc.ChannelID, gc.Timestamp) {
		return
	}
	if !s.verifyGossipSig(ctx, gc.OriginNodeID, gc.Signature, gc.SignableBytes()) {
		return
	}
	s.graph.DeleteEdge(gc.ChannelID)
	if gc.TTL > 1 {
		gc.TTL--
		s.RelayGossip(ctx, sender, "gossip_close", gc)
	}
}
