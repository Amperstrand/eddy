package graph

import (
	"sync"
	"testing"
	"time"
)

func TestNewLocalGraph(t *testing.T) {
	g := NewLocalGraph()
	if g.edges == nil || g.adj == nil {
		t.Fatal("expected non-nil maps")
	}
	if len(g.edges) != 0 || len(g.adj) != 0 {
		t.Fatal("expected empty graph")
	}
}

func TestUpsertEdge(t *testing.T) {
	g := NewLocalGraph()
	now := time.Now().Unix()
	g.UpsertEdge("A", "B", 1, 100, 0, 0, now)
	g.UpsertEdge("B", "C", 2, 200, 0, 0, now)

	if len(g.edges) != 2 {
		t.Fatalf("expected 2 edges, got %d", len(g.edges))
	}
	if len(g.adj["A"]) != 1 || len(g.adj["B"]) != 1 {
		t.Fatal("wrong adjacency")
	}
	e := g.edges[edgeKey{ChannelID: 1, From: "A"}]
	if e.To != "B" || e.Delta != 100 {
		t.Fatalf("wrong edge: %+v", e)
	}
}

func TestUpsertEdge_Update(t *testing.T) {
	g := NewLocalGraph()
	now := time.Now().Unix()
	g.UpsertEdge("A", "B", 1, 100, 0, 0, now)
	g.UpsertEdge("A", "B", 1, 300, 10, 20, now+1)

	if len(g.edges) != 1 {
		t.Fatalf("expected 1 edge, got %d", len(g.edges))
	}
	e := g.edges[edgeKey{ChannelID: 1, From: "A"}]
	if e.Delta != 300 || e.MinHtlc != 10 || e.MaxHtlc != 20 || e.Timestamp != now+1 {
		t.Fatalf("edge not updated: %+v", e)
	}
	// Adjacency should not grow
	if len(g.adj["A"]) != 1 {
		t.Fatalf("adjacency grew to %d", len(g.adj["A"]))
	}
}

func TestDeleteEdge(t *testing.T) {
	g := NewLocalGraph()
	now := time.Now().Unix()
	g.UpsertEdge("A", "B", 1, 100, 0, 0, now)
	g.UpsertEdge("B", "C", 2, 200, 0, 0, now)

	g.DeleteEdge(1)

	if len(g.edges) != 1 {
		t.Fatalf("expected 1 edge, got %d", len(g.edges))
	}
	if _, ok := g.edges[edgeKey{ChannelID: 1, From: "A"}]; ok {
		t.Fatal("edge 1 should be deleted")
	}
	if len(g.adj["A"]) != 0 {
		t.Fatalf("expected empty adjacency for A, got %d", len(g.adj["A"]))
	}
}

func TestInvalidateEdges(t *testing.T) {
	g := NewLocalGraph()
	now := time.Now().Unix()
	g.UpsertEdge("A", "B", 1, 100, 0, 0, now)
	g.UpsertEdge("B", "C", 2, 200, 0, 0, now)

	g.InvalidateEdges([]CycleEdge{{ChannelID: 1, From: "A", To: "B"}})

	e := g.edges[edgeKey{ChannelID: 1, From: "A"}]
	if e.Timestamp != 0 {
		t.Fatalf("expected timestamp 0, got %d", e.Timestamp)
	}
	// Other edge unchanged
	e2 := g.edges[edgeKey{ChannelID: 2, From: "B"}]
	if e2.Timestamp != now {
		t.Fatal("edge 2 should be unchanged")
	}
}

func TestFindCycles_Triangle(t *testing.T) {
	g := NewLocalGraph()
	now := time.Now().Unix()
	g.UpsertEdge("A", "B", 1, 100, 0, 0, now)
	g.UpsertEdge("B", "C", 2, 200, 0, 0, now)
	g.UpsertEdge("C", "A", 3, 150, 0, 0, now)

	cycle := g.FindCycles("A", 5, 60)
	if cycle == nil {
		t.Fatal("expected a cycle")
	}
	if len(cycle.Edges) != 3 {
		t.Fatalf("expected 3 edges, got %d", len(cycle.Edges))
	}
	if cycle.PushAmount != 100 {
		t.Fatalf("expected push amount 100, got %d", cycle.PushAmount)
	}
	// Verify cycle starts and ends at A
	if cycle.Edges[0].From != "A" {
		t.Fatalf("expected cycle to start at A, got %s", cycle.Edges[0].From)
	}
	if cycle.Edges[len(cycle.Edges)-1].To != "A" {
		t.Fatalf("expected cycle to end at A, got %s", cycle.Edges[len(cycle.Edges)-1].To)
	}
}

func TestFindCycles_NoCycle(t *testing.T) {
	g := NewLocalGraph()
	now := time.Now().Unix()
	g.UpsertEdge("A", "B", 1, 100, 0, 0, now)
	g.UpsertEdge("B", "C", 2, 200, 0, 0, now)
	// No edge back to A

	cycle := g.FindCycles("A", 5, 60)
	if cycle != nil {
		t.Fatalf("expected no cycle, got %+v", cycle)
	}
}

func TestFindCycles_StaleEdge(t *testing.T) {
	g := NewLocalGraph()
	now := time.Now().Unix()
	stale := now - 120 // 2 minutes ago
	g.UpsertEdge("A", "B", 1, 100, 0, 0, now)
	g.UpsertEdge("B", "C", 2, 200, 0, 0, stale) // stale
	g.UpsertEdge("C", "A", 3, 150, 0, 0, now)

	cycle := g.FindCycles("A", 5, 60)
	if cycle != nil {
		t.Fatal("expected no cycle due to stale edge")
	}
}

func TestFindCycles_ZeroDelta(t *testing.T) {
	g := NewLocalGraph()
	now := time.Now().Unix()
	g.UpsertEdge("A", "B", 1, 0, 0, 0, now)   // zero delta
	g.UpsertEdge("B", "C", 2, -50, 0, 0, now) // negative delta
	g.UpsertEdge("C", "A", 3, 150, 0, 0, now)

	cycle := g.FindCycles("A", 5, 60)
	if cycle != nil {
		t.Fatal("expected no cycle due to zero/negative delta")
	}
}

func TestFindCycles_PushAmount(t *testing.T) {
	g := NewLocalGraph()
	now := time.Now().Unix()
	g.UpsertEdge("A", "B", 1, 500, 0, 0, now)
	g.UpsertEdge("B", "C", 2, 200, 0, 0, now)
	g.UpsertEdge("C", "A", 3, 300, 0, 0, now)

	cycle := g.FindCycles("A", 5, 60)
	if cycle == nil {
		t.Fatal("expected a cycle")
	}
	// Push amount should be min delta = 200
	if cycle.PushAmount != 200 {
		t.Fatalf("expected push amount 200, got %d", cycle.PushAmount)
	}
}

func TestFindCycles_MaxLength(t *testing.T) {
	g := NewLocalGraph()
	now := time.Now().Unix()
	// 4-node cycle: A→B→C→D→A
	g.UpsertEdge("A", "B", 1, 100, 0, 0, now)
	g.UpsertEdge("B", "C", 2, 100, 0, 0, now)
	g.UpsertEdge("C", "D", 3, 100, 0, 0, now)
	g.UpsertEdge("D", "A", 4, 100, 0, 0, now)

	// maxLength=3 should not find a 4-hop cycle
	cycle := g.FindCycles("A", 3, 60)
	if cycle != nil {
		t.Fatalf("expected no cycle with maxLength=3, got %d edges", len(cycle.Edges))
	}

	// maxLength=4 should find it
	cycle = g.FindCycles("A", 4, 60)
	if cycle == nil {
		t.Fatal("expected cycle with maxLength=4")
	}
	if len(cycle.Edges) != 4 {
		t.Fatalf("expected 4 edges, got %d", len(cycle.Edges))
	}
}

func TestFindCycles_Concurrent(t *testing.T) {
	g := NewLocalGraph()
	now := time.Now().Unix()

	var wg sync.WaitGroup
	// Writer
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			g.UpsertEdge("A", "B", 1, int64(i+1), 0, 0, now)
			g.UpsertEdge("B", "C", 2, int64(i+1), 0, 0, now)
			g.UpsertEdge("C", "A", 3, int64(i+1), 0, 0, now)
		}
	}()
	// Reader
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			g.FindCycles("A", 5, 60)
		}
	}()
	// Deleter
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			g.DeleteEdge(1)
			g.UpsertEdge("A", "B", 1, 100, 0, 0, now)
		}
	}()

	wg.Wait()
	// If we get here without panic, the test passes
}
