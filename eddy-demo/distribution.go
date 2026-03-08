package main

import (
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/charmbracelet/lipgloss"
)

// RenderDistribution draws a CDF of local/capacity ratios.
func RenderDistribution(channels []ChannelBalance) string {
	header := lipgloss.NewStyle().Bold(true).Render("BALANCE DISTRIBUTION")
	result := header + "\n\n"

	if len(channels) == 0 {
		return result + "  (no channels)"
	}

	// Collect ratios
	var ratios []float64
	for _, ch := range channels {
		ratios = append(ratios, float64(ch.LocalA)/float64(ch.Capacity))
	}
	sort.Float64s(ratios)

	// Simple histogram with 10 buckets [0.0, 0.1), [0.1, 0.2), ..., [0.9, 1.0]
	buckets := make([]int, 10)
	for _, r := range ratios {
		idx := int(r * 10)
		if idx >= 10 {
			idx = 9
		}
		buckets[idx]++
	}

	maxCount := 0
	for _, c := range buckets {
		if c > maxCount {
			maxCount = c
		}
	}

	chartHeight := 6
	green := lipgloss.NewStyle().Foreground(lipgloss.Color("10"))

	for row := chartHeight; row >= 1; row-- {
		threshold := float64(row) / float64(chartHeight) * float64(maxCount)
		label := fmt.Sprintf("  %3.0f%% │", threshold/float64(len(channels))*100)
		line := ""
		for _, c := range buckets {
			if float64(c) >= threshold {
				line += green.Render("█")
			} else if float64(c) >= threshold-float64(maxCount)/float64(chartHeight)/2 {
				line += green.Render("▄")
			} else {
				line += " "
			}
		}
		result += label + line + "\n"
	}
	result += "       └" + strings.Repeat("─", 10) + "\n"
	result += "        0    0.5   1.0\n"
	result += "        (local/capacity)"

	// Compute avg balance ratio
	var sum float64
	for _, r := range ratios {
		sum += r
	}
	avg := sum / float64(len(ratios))
	_ = avg

	// Compute std dev to show balance quality
	var variance float64
	for _, r := range ratios {
		variance += (r - 0.5) * (r - 0.5)
	}
	stddev := math.Sqrt(variance / float64(len(ratios)))
	_ = stddev

	return result
}
