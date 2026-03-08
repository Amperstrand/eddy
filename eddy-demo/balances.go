package main

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/lipgloss"
)

// RenderBalances draws horizontal balance bars for all channels.
func RenderBalances(channels []ChannelBalance, width int) string {
	header := lipgloss.NewStyle().Bold(true).Render("CHANNEL BALANCES")
	result := header + "\n\n"

	barWidth := 20
	if width > 50 {
		barWidth = width - 30
	}
	if barWidth > 40 {
		barWidth = 40
	}

	green := lipgloss.NewStyle().Foreground(lipgloss.Color("10"))
	dim := lipgloss.NewStyle().Foreground(lipgloss.Color("8"))

	for _, ch := range channels {
		ratio := float64(ch.LocalA) / float64(ch.Capacity)
		filled := int(ratio * float64(barWidth))
		if filled < 0 {
			filled = 0
		}
		if filled > barWidth {
			filled = barWidth
		}

		bar := green.Render(strings.Repeat("▓", filled)) +
			dim.Render(strings.Repeat("░", barWidth-filled))

		result += fmt.Sprintf("  %-6s %s %-6s %s\n",
			ch.NodeA, bar, ch.NodeB, formatSats(ch.Capacity))
	}

	return result
}
