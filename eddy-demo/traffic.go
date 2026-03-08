package main

import (
	"context"
	"fmt"
	"math/rand"
	"time"
)

// TrafficGenerator sends payments that deliberately imbalance the network,
// creating opportunities for eddy to rebalance.
type TrafficGenerator struct {
	mc      *MultiClient
	monitor *Monitor
	paused  bool
}

func NewTrafficGenerator(mc *MultiClient, monitor *Monitor) *TrafficGenerator {
	return &TrafficGenerator{mc: mc, monitor: monitor}
}

func (t *TrafficGenerator) TogglePause() {
	t.paused = !t.paused
}

func (t *TrafficGenerator) IsPaused() bool {
	return t.paused
}

func (t *TrafficGenerator) Run(ctx context.Context, minInterval, maxInterval time.Duration) {
	for {
		if t.paused {
			select {
			case <-time.After(1 * time.Second):
				continue
			case <-ctx.Done():
				return
			}
		}

		spread := int64(maxInterval - minInterval)
		delay := minInterval
		if spread > 0 {
			delay += time.Duration(rand.Int63n(spread))
		}
		select {
		case <-time.After(delay):
			t.sendImbalancingPayment(ctx)
		case <-ctx.Done():
			return
		}
	}
}

// sendImbalancingPayment sends payments that always flow in one direction:
// alice→bob, alice→carol, bob→dave, carol→dave
// This drains alice and carol's outbound liquidity toward bob and dave,
// creating a clear imbalance pattern that eddy can fix with cycles like
// dave→carol→alice→bob→dave.
func (t *TrafficGenerator) sendImbalancingPayment(ctx context.Context) {
	if len(t.mc.Nodes) < 2 {
		return
	}

	// Pick a random sender, then pick a random direct channel and drain it
	senderIdx := rand.Intn(len(t.mc.Nodes))
	sender := t.mc.Nodes[senderIdx]

	channels, err := sender.ListChannels(ctx)
	if err != nil {
		t.monitor.AddEvent(fmt.Sprintf("traffic: list channels error: %v", err))
		return
	}
	if len(channels) == 0 {
		return
	}

	ch := channels[rand.Intn(len(channels))]
	amount := ch.LocalBalance * 90 / 100

	// Find the receiver node
	var receiver *LNDClient
	for _, n := range t.mc.Nodes {
		if n.info.Pubkey == ch.RemotePubkey {
			receiver = n
			break
		}
	}
	if receiver == nil {
		return
	}

	if amount < 10_000 {
		return // channel already drained
	}

	payReq, err := receiver.AddInvoice(ctx, amount)
	if err != nil {
		t.monitor.AddEvent(fmt.Sprintf("traffic: invoice error: %v", err))
		return
	}

	err = sender.SendPaymentSync(ctx, payReq)
	if err != nil {
		t.monitor.AddEvent(fmt.Sprintf("%s->%s %s FAILED",
			sender.info.Alias, receiver.info.Alias, formatSats(amount)))
		return
	}

	t.monitor.IncrPayment()
	t.monitor.AddEvent(fmt.Sprintf("%s->%s payment %s sats",
		sender.info.Alias, receiver.info.Alias, formatSats(amount)))
}

func formatSats(sats int64) string {
	if sats >= 1_000_000 {
		return fmt.Sprintf("%.1fM", float64(sats)/1_000_000)
	}
	if sats >= 1_000 {
		return fmt.Sprintf("%dk", sats/1000)
	}
	return fmt.Sprintf("%d", sats)
}
