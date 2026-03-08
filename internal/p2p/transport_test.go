package p2p

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"io"
	"testing"

	"github.com/lightningnetwork/lnd/lnrpc"
	"github.com/lightningnetwork/lnd/lnrpc/invoicesrpc"
	"github.com/lightningnetwork/lnd/lnrpc/routerrpc"
	"google.golang.org/grpc/metadata"

	"github.com/eddy/internal/lnd"
)

// mockLND implements lnd.LNDClient for testing.
type mockLND struct {
	lnd.LNDClient // embed for unimplemented methods

	sentMessages []sentMsg
	sendErr      error

	customMsgStream *mockCustomMsgStream
	subscribeErr    error
}

type sentMsg struct {
	Peer    []byte
	MsgType uint32
	Data    []byte
}

func (m *mockLND) SendCustomMessage(_ context.Context, peer []byte, msgType uint32, data []byte) error {
	if m.sendErr != nil {
		return m.sendErr
	}
	m.sentMessages = append(m.sentMessages, sentMsg{Peer: peer, MsgType: msgType, Data: data})
	return nil
}

func (m *mockLND) SubscribeCustomMessages(_ context.Context) (lnrpc.Lightning_SubscribeCustomMessagesClient, error) {
	if m.subscribeErr != nil {
		return nil, m.subscribeErr
	}
	return m.customMsgStream, nil
}

// mockCustomMsgStream implements lnrpc.Lightning_SubscribeCustomMessagesClient.
type mockCustomMsgStream struct {
	msgs []*lnrpc.CustomMessage
	idx  int
}

func (s *mockCustomMsgStream) Recv() (*lnrpc.CustomMessage, error) {
	if s.idx >= len(s.msgs) {
		return nil, io.EOF
	}
	msg := s.msgs[s.idx]
	s.idx++
	return msg, nil
}

func (s *mockCustomMsgStream) Header() (metadata.MD, error) { return nil, nil }
func (s *mockCustomMsgStream) Trailer() metadata.MD         { return nil }
func (s *mockCustomMsgStream) CloseSend() error             { return nil }
func (s *mockCustomMsgStream) Context() context.Context     { return context.Background() }
func (s *mockCustomMsgStream) SendMsg(any) error            { return nil }
func (s *mockCustomMsgStream) RecvMsg(any) error            { return nil }

// Stub out remaining interface methods
func (m *mockLND) GetInfo(context.Context) (*lnrpc.GetInfoResponse, error) { return nil, nil }
func (m *mockLND) ListChannels(context.Context) ([]*lnrpc.Channel, error)  { return nil, nil }
func (m *mockLND) SubscribeChannelEvents(context.Context) (lnrpc.Lightning_SubscribeChannelEventsClient, error) {
	return nil, nil
}
func (m *mockLND) SubscribeHtlcEvents(context.Context) (routerrpc.Router_SubscribeHtlcEventsClient, error) {
	return nil, nil
}
func (m *mockLND) AddHoldInvoice(context.Context, []byte, int64) ([]byte, error) { return nil, nil }
func (m *mockLND) SettleInvoice(context.Context, []byte) error                   { return nil }
func (m *mockLND) SubscribeSingleInvoice(context.Context, []byte) (invoicesrpc.Invoices_SubscribeSingleInvoiceClient, error) {
	return nil, nil
}
func (m *mockLND) CancelInvoice(context.Context, []byte) error { return nil }
func (m *mockLND) SendToRouteV2(context.Context, []byte, *lnrpc.Route) (*lnrpc.HTLCAttempt, error) {
	return nil, nil
}

func TestTransportSend(t *testing.T) {
	mock := &mockLND{}
	tr := NewTransport(mock)

	pubkey := "0011223344556677889900112233445566778899001122334455667788990011"
	err := tr.Send(context.Background(), pubkey, "gossip_state", GossipState{ChannelID: 42})
	if err != nil {
		t.Fatalf("send: %v", err)
	}

	if len(mock.sentMessages) != 1 {
		t.Fatalf("expected 1 sent message, got %d", len(mock.sentMessages))
	}
	msg := mock.sentMessages[0]
	if msg.MsgType != CustomMsgType {
		t.Fatalf("wrong msg type: %d", msg.MsgType)
	}
	expectedPeer, _ := hex.DecodeString(pubkey)
	if hex.EncodeToString(msg.Peer) != hex.EncodeToString(expectedPeer) {
		t.Fatal("peer mismatch")
	}

	// Verify payload is valid envelope
	var env Envelope
	if err := json.Unmarshal(msg.Data, &env); err != nil {
		t.Fatalf("unmarshal envelope: %v", err)
	}
	if env.Type != "gossip_state" {
		t.Fatalf("wrong envelope type: %s", env.Type)
	}
}

func TestTransportSend_InvalidPubkey(t *testing.T) {
	mock := &mockLND{}
	tr := NewTransport(mock)

	err := tr.Send(context.Background(), "not-hex!", "test", nil)
	if err == nil {
		t.Fatal("expected error for invalid pubkey")
	}
}

func TestTransportSubscribe(t *testing.T) {
	peerBytes, _ := hex.DecodeString("aabbccdd")
	envData, _ := json.Marshal(Envelope{
		Version: ProtocolVersion,
		Type:    "gossip_state",
		Data:    json.RawMessage(`{"channel_id":1}`),
	})

	mock := &mockLND{
		customMsgStream: &mockCustomMsgStream{
			msgs: []*lnrpc.CustomMessage{
				{Peer: peerBytes, Type: CustomMsgType, Data: envData},
				{Peer: peerBytes, Type: 99999, Data: []byte("ignored")}, // wrong type
			},
		},
	}
	tr := NewTransport(mock)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch, err := tr.Subscribe(ctx)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	msg, ok := <-ch
	if !ok {
		t.Fatal("channel closed early")
	}
	if msg.Envelope.Type != "gossip_state" {
		t.Fatalf("wrong type: %s", msg.Envelope.Type)
	}
	if msg.PeerPubkey != "aabbccdd" {
		t.Fatalf("wrong peer: %s", msg.PeerPubkey)
	}

	// Cancel context so the retry loop exits after EOF
	cancel()
	_, ok = <-ch
	if ok {
		t.Fatal("expected channel to close")
	}
}
