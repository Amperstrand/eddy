package main

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"
)

// ChannelBalance represents one channel's balance state.
type ChannelBalance struct {
	ChanID       uint64
	NodeA, NodeB string // aliases, sorted alphabetically
	PubA, PubB   string // pubkeys matching NodeA, NodeB
	Capacity     int64
	LocalA       int64 // balance from NodeA's perspective
}

// Monitor polls all nodes for channel balances.
type Monitor struct {
	mc       *MultiClient
	mu       sync.RWMutex
	channels []ChannelBalance
	counters Counters

	eventLog []LogEntry
	logMu    sync.RWMutex
}

type Counters struct {
	Rebalances int
	SatsMoved  int64
	FeesSaved  int64
	Payments   int
}

type LogEntry struct {
	Time time.Time
	Text string
}

func NewMonitor(mc *MultiClient) *Monitor {
	return &Monitor{mc: mc}
}

func (m *Monitor) Channels() []ChannelBalance {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]ChannelBalance, len(m.channels))
	copy(out, m.channels)
	return out
}

func (m *Monitor) Counters() Counters {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.counters
}

func (m *Monitor) Events() []LogEntry {
	m.logMu.RLock()
	defer m.logMu.RUnlock()
	out := make([]LogEntry, len(m.eventLog))
	copy(out, m.eventLog)
	return out
}

func (m *Monitor) AddEvent(text string) {
	m.logMu.Lock()
	defer m.logMu.Unlock()
	m.eventLog = append(m.eventLog, LogEntry{Time: time.Now(), Text: text})
	if len(m.eventLog) > 100 {
		m.eventLog = m.eventLog[len(m.eventLog)-100:]
	}
}

func (m *Monitor) IncrRebalance(sats int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.counters.Rebalances++
	m.counters.SatsMoved += sats
	// Estimate fees saved: typical circular rebalance costs ~100 ppm per hop, 3 hops
	m.counters.FeesSaved += sats * 300 / 1_000_000
}

func (m *Monitor) IncrPayment() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.counters.Payments++
}

// Poll fetches channel balances from all nodes and deduplicates.
func (m *Monitor) Poll(ctx context.Context) error {
	type chanKey struct {
		pubA, pubB string
	}
	seen := make(map[chanKey]*ChannelBalance)

	for _, node := range m.mc.Nodes {
		channels, err := node.ListChannels(ctx)
		if err != nil {
			return fmt.Errorf("list channels %s: %w", node.info.Alias, err)
		}
		for _, ch := range channels {
			localPub := node.info.Pubkey
			remotePub := ch.RemotePubkey

			// Canonical ordering: sort pubkeys alphabetically
			pubA, pubB := localPub, remotePub
			localA := ch.LocalBalance
			if pubA > pubB {
				pubA, pubB = pubB, pubA
				localA = ch.RemoteBalance
			}
			key := chanKey{pubA, pubB}
			// Use first node's perspective (or overwrite — both should agree)
			if _, ok := seen[key]; !ok {
				aliasA := m.mc.AliasFor(pubA)
				aliasB := m.mc.AliasFor(pubB)
				seen[key] = &ChannelBalance{
					ChanID:   ch.ChanId,
					NodeA:    aliasA,
					NodeB:    aliasB,
					PubA:     pubA,
					PubB:     pubB,
					Capacity: ch.Capacity,
					LocalA:   localA,
				}
			}
		}
	}

	var result []ChannelBalance
	for _, cb := range seen {
		result = append(result, *cb)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].NodeA+result[i].NodeB < result[j].NodeA+result[j].NodeB
	})

	m.mu.Lock()
	m.channels = result
	m.mu.Unlock()
	return nil
}

// Run polls in a loop.
func (m *Monitor) Run(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	// Initial poll
	m.Poll(ctx)
	for {
		select {
		case <-ticker.C:
			m.Poll(ctx)
		case <-ctx.Done():
			return
		}
	}
}
