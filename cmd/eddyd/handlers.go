package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/eddy/internal/p2p"
	"github.com/eddy/internal/shortid"
)

// unmarshalMsg deserializes a JSON message into the given type, logging on failure.
func unmarshalMsg[T any](data json.RawMessage) (T, bool) {
	var v T
	if err := json.Unmarshal(data, &v); err != nil {
		slog.Debug("unmarshal error", "type", fmt.Sprintf("%T", v), "error", err)
		return v, false
	}
	return v, true
}

func (d *eddyd) handleMessage(ctx context.Context, msg p2p.IncomingMessage) {
	switch msg.Envelope.Type {
	case "gossip_state":
		if gs, ok := unmarshalMsg[p2p.GossipState](msg.Envelope.Data); ok {
			d.gossip.HandleGossipState(ctx, msg.PeerPubkey, gs)
		}
	case "gossip_close":
		if gc, ok := unmarshalMsg[p2p.GossipClose](msg.Envelope.Data); ok {
			d.gossip.HandleGossipClose(ctx, msg.PeerPubkey, gc)
		}
	case "circ_propose":
		if cp, ok := unmarshalMsg[p2p.CircPropose](msg.Envelope.Data); ok {
			go d.circ.HandleCircPropose(ctx, msg.PeerPubkey, cp)
		}
	case "circ_abort":
		if ca, ok := unmarshalMsg[p2p.CircAbort](msg.Envelope.Data); ok {
			go d.circ.HandleCircAbort(ctx, msg.PeerPubkey, ca)
		}
	case "circ_busy":
		if cb, ok := unmarshalMsg[p2p.CircBusy](msg.Envelope.Data); ok {
			go d.circ.HandleCircBusy(ctx, msg.PeerPubkey, cb)
		}
	default:
		slog.Debug("unknown p2p message type", "type", msg.Envelope.Type, "peer", shortid.Short(msg.PeerPubkey))
	}
}
