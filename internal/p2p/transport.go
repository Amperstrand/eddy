package p2p

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/eddy/internal/lnd"
)

// IncomingMessage is a deserialized message from a peer.
type IncomingMessage struct {
	PeerPubkey string
	Envelope   Envelope
}

// Transport wraps LND custom messages for structured P2P communication.
// Messages are serialized as JSON envelopes with type-based dispatch.
type Transport struct {
	client lnd.LNDClient
}

func NewTransport(client lnd.LNDClient) *Transport {
	return &Transport{client: client}
}

// Send serializes an envelope and sends it to a peer via LND custom message.
func (t *Transport) Send(ctx context.Context, peerPubkey string, msgType string, data any) error {
	raw, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("marshal data: %w", err)
	}
	env := Envelope{Version: ProtocolVersion, Type: msgType, Data: raw}
	payload, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("marshal envelope: %w", err)
	}
	peerBytes, err := hex.DecodeString(peerPubkey)
	if err != nil {
		return fmt.Errorf("decode peer pubkey: %w", err)
	}
	if err := t.client.SendCustomMessage(ctx, peerBytes, CustomMsgType, payload); err != nil {
		return fmt.Errorf("send custom msg to %s: %w", peerPubkey[:16], err)
	}
	return nil
}

// Subscribe returns a channel of incoming P2P messages.
func (t *Transport) Subscribe(ctx context.Context) (<-chan IncomingMessage, error) {
	stream, err := t.client.SubscribeCustomMessages(ctx)
	if err != nil {
		return nil, fmt.Errorf("subscribe custom messages: %w", err)
	}
	ch := make(chan IncomingMessage, 64)
	go func() {
		defer close(ch)
		backoff := time.Second
		for {
			if stream == nil {
				if ctx.Err() != nil {
					return
				}
				var err error
				stream, err = t.client.SubscribeCustomMessages(ctx)
				if err != nil {
					slog.Error("resubscribe custom messages", "error", err)
					// Jitter avoids thundering herd when multiple peers reconnect simultaneously.
					jitter := time.Duration(rand.Int64N(int64(backoff)))
					select {
					case <-time.After(backoff/2 + jitter/2):
						backoff = min(backoff*2, 30*time.Second)
					case <-ctx.Done():
						return
					}
					continue
				}
				backoff = time.Second
			}
			msg, err := stream.Recv()
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				slog.Error("custom message stream ended, reconnecting", "error", err)
				stream = nil
				continue
			}
			if msg.Type != CustomMsgType {
				continue
			}
			var env Envelope
			if err := json.Unmarshal(msg.Data, &env); err != nil {
				slog.Error("unmarshal custom message", "peer", fmt.Sprintf("%x", msg.Peer), "error", err)
				continue
			}
			if env.Version != ProtocolVersion {
				slog.Warn("dropping message with incompatible protocol version",
					"peer", fmt.Sprintf("%x", msg.Peer),
					"got", env.Version, "want", ProtocolVersion)
				continue
			}
			select {
			case ch <- IncomingMessage{
				PeerPubkey: hex.EncodeToString(msg.Peer),
				Envelope:   env,
			}:
			case <-ctx.Done():
				return
			}
		}
	}()
	return ch, nil
}
