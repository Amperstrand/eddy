package main

import (
	"fmt"
	"math"
	"sort"

	"github.com/charmbracelet/lipgloss"
)

// RenderTopology draws the K4 graph with colored edges.
func RenderTopology(channels []ChannelBalance, width int) string {
	header := lipgloss.NewStyle().Bold(true).Render("NETWORK TOPOLOGY")

	if len(channels) == 0 {
		return header + "\n\n  (no channels)"
	}

	// Build edge color map
	edgeColor := make(map[string]lipgloss.Color)
	for _, ch := range channels {
		key := ch.NodeA + "-" + ch.NodeB
		ratio := float64(ch.LocalA) / float64(ch.Capacity)
		imbalance := math.Abs(ratio-0.5) * 2
		var color lipgloss.Color
		switch {
		case imbalance < 0.3:
			color = lipgloss.Color("10") // green
		case imbalance < 0.6:
			color = lipgloss.Color("11") // yellow
		default:
			color = lipgloss.Color("9") // red
		}
		edgeColor[key] = color
	}

	getColor := func(a, b string) lipgloss.Color {
		if a > b {
			a, b = b, a
		}
		if c, ok := edgeColor[a+"-"+b]; ok {
			return c
		}
		return lipgloss.Color("8")
	}

	// Collect sorted unique names
	nameSet := make(map[string]bool)
	for _, ch := range channels {
		nameSet[ch.NodeA] = true
		nameSet[ch.NodeB] = true
	}
	var sorted []string
	for n := range nameSet {
		sorted = append(sorted, n)
	}
	sort.Strings(sorted)
	// Pad to 4
	for len(sorted) < 4 {
		sorted = append(sorted, "?")
	}
	a, b, c, d := sorted[0], sorted[1], sorted[2], sorted[3]

	colorStr := func(n1, n2, s string) string {
		return lipgloss.NewStyle().Foreground(getColor(n1, n2)).Render(s)
	}

	// Layout:
	//   alice ──── bob
	//    │ \    / │
	//    │  \  /  │
	//    │   \/   │
	//    │   /\   │
	//    │  /  \  │
	//    │ /    \ │
	//   carol ── dave
	ab := colorStr(a, b, "────")
	cd := colorStr(c, d, "────")
	al := colorStr(a, c, "│")
	ar := colorStr(b, d, "│")
	x1 := colorStr(a, d, "\\")
	x2 := colorStr(b, c, "/")
	x3 := colorStr(b, c, "\\")
	x4 := colorStr(a, d, "/")

	lines := []string{
		fmt.Sprintf("  %-7s%s %s", a, ab, b),
		fmt.Sprintf("   %s  %s    %s  %s", al, x1, x2, ar),
		fmt.Sprintf("   %s   %s  %s   %s", al, x1, x2, ar),
		fmt.Sprintf("   %s    %s%s    %s", al, x1, x2, ar),
		fmt.Sprintf("   %s    %s%s    %s", al, x4, x3, ar),
		fmt.Sprintf("   %s   %s  %s   %s", al, x4, x3, ar),
		fmt.Sprintf("   %s  %s    %s  %s", al, x4, x3, ar),
		fmt.Sprintf("  %-7s%s %s", c, cd, d),
	}

	result := header + "\n\n"
	for _, l := range lines {
		result += l + "\n"
	}
	result += "\n  (edges colored by\n   imbalance severity)"
	return result
}
