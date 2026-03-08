package main

import (
	"context"
	"log/slog"
	"time"

	"github.com/eddy/internal/gossip"
	"github.com/eddy/internal/p2p"
	"github.com/lightningnetwork/lnd/lnrpc"
	"github.com/lightningnetwork/lnd/lnrpc/routerrpc"
)

// channelEventLoop watches for channel close events, removes the channel
// from the local graph, and gossips the closure to peers.
func (d *eddyd) channelEventLoop(ctx context.Context) {
	backoff := time.Second
	for {
		if ctx.Err() != nil {
			return
		}
		stream, err := d.client.SubscribeChannelEvents(ctx)
		if err != nil {
			slog.Error("subscribe channel events", "error", err)
			select {
			case <-time.After(backoff):
				backoff = min(backoff*2, 30*time.Second)
			case <-ctx.Done():
				return
			}
			continue
		}
		backoff = time.Second
		for {
			ev, err := stream.Recv()
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				slog.Error("channel events stream ended, reconnecting", "error", err)
				break
			}
			closed, ok := ev.Channel.(*lnrpc.ChannelEventUpdate_ClosedChannel)
			if !ok {
				continue
			}
			chanID := closed.ClosedChannel.ChanId
			peerNodeID := closed.ClosedChannel.RemotePubkey
			d.graph.DeleteEdge(chanID)

			gc := p2p.GossipClose{
				OriginNodeID: d.selfID,
				ChannelID:    chanID,
				PeerNodeID:   peerNodeID,
				TTL:          d.cfg.Solver.MaxCycleLength,
				Timestamp:    time.Now().Unix(),
			}
			if err := d.gossip.SignClose(ctx, &gc); err != nil {
				slog.Error("gossip: sign close", "error", err)
				continue
			}
			d.gossip.RelayGossip(ctx, "", "gossip_close", gc)
			slog.Info("channel closed", "channel", chanID)
		}
	}
}

// htlcEventLoop watches HTLC settle/forward events and refreshes local
// channel balances in the graph. Events are debounced: after the first
// event arrives, we wait 50ms to collect more, then batch-refresh all
// affected channels in a single ListChannels call.
func (d *eddyd) htlcEventLoop(ctx context.Context) {
	backoff := time.Second
	for {
		if ctx.Err() != nil {
			return
		}
		stream, err := d.client.SubscribeHtlcEvents(ctx)
		if err != nil {
			slog.Error("subscribe htlc events", "error", err)
			select {
			case <-time.After(backoff):
				backoff = min(backoff*2, 30*time.Second)
			case <-ctx.Done():
				return
			}
			continue
		}
		backoff = time.Second

		type htlcResult struct {
			ev  *routerrpc.HtlcEvent
			err error
		}
		evCh := make(chan htlcResult, 64)
		go func() {
			for {
				ev, err := stream.Recv()
				select {
				case evCh <- htlcResult{ev, err}:
				case <-ctx.Done():
					return
				}
				if err != nil {
					return
				}
			}
		}()

		dirty := make(map[uint64]bool)
		var debounce <-chan time.Time
		reconnect := false
		for !reconnect {
			select {
			case res := <-evCh:
				if res.err != nil {
					if ctx.Err() != nil {
						return
					}
					slog.Error("htlc events stream ended, reconnecting", "error", res.err)
					reconnect = true
					break
				}
				switch res.ev.Event.(type) {
				case *routerrpc.HtlcEvent_SettleEvent, *routerrpc.HtlcEvent_ForwardEvent:
				default:
					continue
				}
				if res.ev.IncomingChannelId != 0 {
					dirty[res.ev.IncomingChannelId] = true
				}
				if res.ev.OutgoingChannelId != 0 {
					dirty[res.ev.OutgoingChannelId] = true
				}
				if debounce == nil {
					debounce = time.After(50 * time.Millisecond)
				}
			case <-debounce:
				d.refreshDirtyChannels(ctx, dirty)
				dirty = make(map[uint64]bool)
				debounce = nil
			case <-ctx.Done():
				return
			}
		}
		if len(dirty) > 0 {
			d.refreshDirtyChannels(ctx, dirty)
		}
	}
}

// refreshDirtyChannels fetches current channel state from LND, updates the
// local graph for channels whose balance changed, and gossips the new state.
func (d *eddyd) refreshDirtyChannels(ctx context.Context, dirty map[uint64]bool) {
	chs, err := d.client.ListChannels(ctx)
	if err != nil {
		slog.Error("refresh channels", "error", err)
		return
	}
	now := time.Now().Unix()

	var changed []*lnrpc.Channel
	for _, ch := range chs {
		if !dirty[ch.ChanId] {
			continue
		}
		cur := gossip.ChanStateFromChannel(ch)
		prev := d.graph.GetEdge(ch.ChanId, d.selfID)
		if prev != nil && prev.Delta == cur.Delta && prev.To == ch.RemotePubkey {
			continue
		}
		d.graph.UpsertEdge(d.selfID, ch.RemotePubkey, ch.ChanId, cur.Delta, cur.MinHtlc, cur.MaxHtlc, now)
		changed = append(changed, ch)
	}

	for _, ch := range changed {
		gs := gossip.GossipStateFromChannel(d.selfID, ch, now, d.cfg.Solver.MaxCycleLength)
		if err := d.gossip.SignState(ctx, &gs); err != nil {
			slog.Error("gossip: sign state", "error", err)
			continue
		}
		d.gossip.RelayGossip(ctx, ch.RemotePubkey, "gossip_state", gs)
	}
}
