package main

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"github.com/eddy/internal/circulation"
)

func (d *eddyd) solverLoop(ctx context.Context) {
	const debounce = 50 * time.Millisecond
	for {
		select {
		case <-d.graph.Notify():
		case <-ctx.Done():
			return
		}
		// Debounce: gossip heartbeats upsert many edges at once.
		time.Sleep(debounce)
		// Drain any additional signals that arrived during debounce.
		for {
			select {
			case <-d.graph.Notify():
			default:
				goto run
			}
		}
	run:

		if d.circ.Draining() {
			slog.Debug("solver: skipping, draining")
			continue
		}

		// If a circulation is active, only proceed if it's been running
		// longer than the full lifecycle timeout (propose + payment),
		// in which case we clear it as stale. Otherwise skip this tick.
		if busy, startTime, cycleID := d.circ.IsBusy(); busy {
			if time.Since(startTime) > d.cfg.Circulation.LifecycleTimeout(d.cfg.Solver.MaxCycleLength) {
				d.circ.ClearStale(cycleID)
			} else {
				continue
			}
		}

		cycle := d.graph.FindCycles(d.selfID, d.cfg.Solver.MaxCycleLength, d.cfg.Solver.StaleThreshold)
		if cycle == nil {
			slog.Debug("solver: no cycles found")
			continue
		}
		if cycle.PushAmount < d.cfg.Solver.MinPushAmount {
			slog.Debug("solver: push amount below minimum", "amount", cycle.PushAmount, "min", d.cfg.Solver.MinPushAmount)
			continue
		}

		var nodeIDs []string
		var chanIDs []uint64
		for _, e := range cycle.Edges {
			nodeIDs = append(nodeIDs, e.From)
			chanIDs = append(chanIDs, e.ChannelID)
		}

		// Only the node with the lexicographically smallest pubkey in the
		// cycle should propose. This prevents multiple nodes from proposing
		// the same cycle simultaneously and deadlocking each other.
		minPub := nodeIDs[0]
		for _, id := range nodeIDs[1:] {
			if id < minPub {
				minPub = id
			}
		}
		if d.selfID != minPub {
			slog.Debug("solver: deferring to smaller pubkey", "min", minPub[:16])
			continue
		}

		// Build ring representation: duplicate first node at end so
		// cycleNodes[i] → cycleNodes[i+1] pairs align with chanIDs[i].
		cycleNodes := make([]string, len(nodeIDs)+1)
		copy(cycleNodes, nodeIDs)
		cycleNodes[len(nodeIDs)] = nodeIDs[0]

		slog.Info("solver: cycle found", "path", strings.Join(nodeIDs, " → "), "push_sats", cycle.PushAmount)

		cycleID, err := circulation.GenerateCycleID()
		if err != nil {
			slog.Error("solver: generate cycle ID", "error", err)
			continue
		}

		if err := d.circ.StartProposal(ctx, cycleNodes, chanIDs, cycle.PushAmount, cycleID); err != nil {
			slog.Error("solver: start proposal", "error", err)
		}
	}
}
