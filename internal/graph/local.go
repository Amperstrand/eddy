package graph

import (
	"math/rand"
	"sync"
	"time"
)

// CycleEdge represents one hop in a detected cycle.
type CycleEdge struct {
	ChannelID uint64
	From, To  string
	Delta     int64
}

// Cycle represents a detected rebalancing cycle.
type Cycle struct {
	Edges      []CycleEdge
	PushAmount int64
}

// ChannelEdge represents a directed edge in the local graph.
type ChannelEdge struct {
	ChannelID uint64
	From      string
	To        string
	Delta     int64
	MinHtlc   int64 // msat
	MaxHtlc   int64 // msat
	Timestamp int64
}

// LocalGraph is a thread-safe in-memory directed graph of channel states.
type LocalGraph struct {
	mu sync.RWMutex
	// edges indexed by channelID+from for uniqueness
	edges map[edgeKey]*ChannelEdge
	// adjacency: from → list of edgeKeys
	adj    map[string][]edgeKey
	notify chan struct{}
}

type edgeKey struct {
	ChannelID uint64
	From      string
}

func NewLocalGraph() *LocalGraph {
	return &LocalGraph{
		edges:  make(map[edgeKey]*ChannelEdge),
		adj:    make(map[string][]edgeKey),
		notify: make(chan struct{}, 1),
	}
}

// Notify returns a channel that signals when the graph has been mutated
// in a way that could create new cycles (i.e. an edge delta changed).
func (g *LocalGraph) Notify() <-chan struct{} {
	return g.notify
}

func (g *LocalGraph) signal() {
	select {
	case g.notify <- struct{}{}:
	default:
	}
}

func (g *LocalGraph) UpsertEdge(from, to string, channelID uint64, delta, minHtlc, maxHtlc, timestamp int64) {
	g.mu.Lock()
	defer g.mu.Unlock()

	key := edgeKey{ChannelID: channelID, From: from}
	if existing, ok := g.edges[key]; ok {
		changed := existing.Delta != delta
		existing.To = to
		existing.Delta = delta
		existing.MinHtlc = minHtlc
		existing.MaxHtlc = maxHtlc
		existing.Timestamp = timestamp
		if changed {
			g.signal()
		}
		return
	}
	g.edges[key] = &ChannelEdge{
		ChannelID: channelID,
		From:      from,
		To:        to,
		Delta:     delta,
		MinHtlc:   minHtlc,
		MaxHtlc:   maxHtlc,
		Timestamp: timestamp,
	}
	g.adj[from] = append(g.adj[from], key)
	g.signal()
}

// GetEdge returns the edge for a given channel+from, or nil if not found.
func (g *LocalGraph) GetEdge(channelID uint64, from string) *ChannelEdge {
	g.mu.RLock()
	defer g.mu.RUnlock()
	if e, ok := g.edges[edgeKey{ChannelID: channelID, From: from}]; ok {
		cp := *e
		return &cp
	}
	return nil
}

func (g *LocalGraph) DeleteEdge(channelID uint64) {
	g.mu.Lock()
	defer g.mu.Unlock()

	for key := range g.edges {
		if key.ChannelID == channelID {
			delete(g.edges, key)
			adj := g.adj[key.From]
			for i, k := range adj {
				if k == key {
					g.adj[key.From] = append(adj[:i], adj[i+1:]...)
					break
				}
			}
		}
	}
}

func (g *LocalGraph) InvalidateEdges(edges []CycleEdge) {
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, e := range edges {
		key := edgeKey{ChannelID: e.ChannelID, From: e.From}
		if edge, ok := g.edges[key]; ok {
			edge.Timestamp = 0
		}
	}
}

// msatToSatCeil converts millisatoshi to satoshi, rounding up.
func msatToSatCeil(msat int64) int64 {
	return (msat + 999) / 1000
}

// msatToSatFloor converts millisatoshi to satoshi, rounding down.
func msatToSatFloor(msat int64) int64 {
	return msat / 1000
}

// FindCycles performs an iterative DFS from selfNodeID to find a circular
// payment path. Only edges with positive delta (local-heavy channels) and
// fresh timestamps (within staleThresholdSec) are traversed. The cycle
// must have at least 3 hops, at most maxLength hops, and its push amount
// (minimum delta along the path) must satisfy all HTLC min/max constraints.
// Adjacency lists are shuffled so repeated calls explore different paths.
// Returns the first valid cycle found, or nil.
func (g *LocalGraph) FindCycles(selfNodeID string, maxLength int, staleThresholdSec int) *Cycle {
	g.mu.RLock()
	defer g.mu.RUnlock()

	if maxLength < 3 {
		maxLength = 3
	}
	cutoff := time.Now().Unix() - int64(staleThresholdSec)

	// Build filtered adjacency: only positive-delta, non-stale edges
	type neighbor struct {
		to      string
		chanID  uint64
		delta   int64
		minHtlc int64
		maxHtlc int64
	}
	adj := make(map[string][]neighbor)
	for key, edge := range g.edges {
		if edge.Delta <= 0 || edge.Timestamp <= cutoff {
			continue
		}
		adj[key.From] = append(adj[key.From], neighbor{
			to:      edge.To,
			chanID:  edge.ChannelID,
			delta:   edge.Delta,
			minHtlc: edge.MinHtlc,
			maxHtlc: edge.MaxHtlc,
		})
	}

	// Shuffle adjacency lists for randomization
	for _, neighbors := range adj {
		rand.Shuffle(len(neighbors), func(i, j int) {
			neighbors[i], neighbors[j] = neighbors[j], neighbors[i]
		})
	}

	// Only search for cycles starting from selfNodeID
	startNeighbors := adj[selfNodeID]
	if len(startNeighbors) == 0 {
		return nil
	}

	type stackEntry struct {
		cur      string
		pathNIDs []string
		pathCIDs []uint64
		minDelta int64
		cMin     int64 // max of all min_htlc (msat)
		cMax     int64 // min of all max_htlc (msat), 0 = unlimited
		visited  map[string]bool
	}

	stack := make([]stackEntry, 0, len(startNeighbors))
	for _, n := range startNeighbors {
		stack = append(stack, stackEntry{
			cur:      n.to,
			pathNIDs: []string{selfNodeID, n.to},
			pathCIDs: []uint64{n.chanID},
			minDelta: n.delta,
			cMin:     n.minHtlc,
			cMax:     n.maxHtlc,
			visited:  map[string]bool{selfNodeID: true, n.to: true},
		})
	}

	for len(stack) > 0 {
		e := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		if e.cur == selfNodeID && len(e.pathCIDs) >= 3 {
			pushAmount := e.minDelta
			cycleMinSat := int64(0)
			if e.cMin > 0 {
				cycleMinSat = msatToSatCeil(e.cMin)
			}
			cycleMaxSat := int64(0)
			if e.cMax > 0 {
				cycleMaxSat = msatToSatFloor(e.cMax)
			}
			if cycleMaxSat > 0 && pushAmount > cycleMaxSat {
				pushAmount = cycleMaxSat
			}
			if pushAmount >= cycleMinSat && pushAmount > 0 {
				edges := make([]CycleEdge, len(e.pathCIDs))
				for i := range e.pathCIDs {
					edges[i] = CycleEdge{
						ChannelID: e.pathCIDs[i],
						From:      e.pathNIDs[i],
						To:        e.pathNIDs[i+1],
					}
				}
				return &Cycle{Edges: edges, PushAmount: pushAmount}
			}
			continue
		}

		if len(e.pathCIDs) >= maxLength {
			continue
		}

		for _, n := range adj[e.cur] {
			if n.to != selfNodeID && e.visited[n.to] {
				continue
			}
			newVisited := make(map[string]bool, len(e.visited)+1)
			for k, v := range e.visited {
				newVisited[k] = v
			}
			newVisited[n.to] = true

			newCMax := e.cMax
			if e.cMax > 0 && n.maxHtlc > 0 {
				if n.maxHtlc < newCMax {
					newCMax = n.maxHtlc
				}
			} else if newCMax == 0 {
				newCMax = n.maxHtlc
			}

			newCMin := e.cMin
			if n.minHtlc > newCMin {
				newCMin = n.minHtlc
			}

			newMinDelta := e.minDelta
			if n.delta < newMinDelta {
				newMinDelta = n.delta
			}

			stack = append(stack, stackEntry{
				cur:      n.to,
				pathNIDs: append(append([]string{}, e.pathNIDs...), n.to),
				pathCIDs: append(append([]uint64{}, e.pathCIDs...), n.chanID),
				minDelta: newMinDelta,
				cMin:     newCMin,
				cMax:     newCMax,
				visited:  newVisited,
			})
		}
	}

	return nil
}
