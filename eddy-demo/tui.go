package main

import (
	"fmt"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

type tickMsg time.Time

type model struct {
	monitor  *Monitor
	traffic  *TrafficGenerator
	width    int
	height   int
	quitting bool
}

func newModel(monitor *Monitor, traffic *TrafficGenerator) model {
	return model{
		monitor: monitor,
		traffic: traffic,
		width:   120,
		height:  40,
	}
}

func (m model) Init() tea.Cmd {
	return tea.Tick(time.Second, func(t time.Time) tea.Msg {
		return tickMsg(t)
	})
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "q", "ctrl+c":
			m.quitting = true
			return m, tea.Quit
		case "p":
			m.traffic.TogglePause()
		}
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
	case tickMsg:
		return m, tea.Tick(time.Second, func(t time.Time) tea.Msg {
			return tickMsg(t)
		})
	}
	return m, nil
}

func (m model) View() string {
	if m.quitting {
		return "Shutting down...\n"
	}

	channels := m.monitor.Channels()
	counters := m.monitor.Counters()
	events := m.monitor.Events()

	// Title
	title := titleStyle.Render("  EDDY DEMO  ")
	titleLine := lipgloss.PlaceHorizontal(m.width, lipgloss.Center, title)

	// Left column width, right column width
	leftW := m.width/2 - 2
	rightW := m.width - leftW - 4

	// Top-left: Topology
	topo := RenderTopology(channels, leftW)
	topoPanel := panelStyle.Width(leftW).Render(topo)

	// Top-right: Balance bars
	bals := RenderBalances(channels, rightW)
	balsPanel := panelStyle.Width(rightW).Render(bals)

	topRow := lipgloss.JoinHorizontal(lipgloss.Top, topoPanel, balsPanel)

	// Bottom-left: Distribution
	dist := RenderDistribution(channels)
	distPanel := panelStyle.Width(leftW).Render(dist)

	// Bottom-right: Counters
	// Imbalance: average absolute deviation from 50/50 (0% = perfect, 50% = fully drained)
	imbalance := 0.0
	if len(channels) > 0 {
		var sum float64
		for _, ch := range channels {
			ratio := float64(ch.LocalA) / float64(ch.Capacity)
			dev := ratio - 0.5
			if dev < 0 {
				dev = -dev
			}
			sum += dev
		}
		imbalance = sum / float64(len(channels)) * 100
	}

	pauseStatus := ""
	if m.traffic.IsPaused() {
		pauseStatus = lipgloss.NewStyle().Foreground(lipgloss.Color("11")).Render("  [PAUSED]")
	}

	ctr := lipgloss.NewStyle().Bold(true).Render("COUNTERS") + pauseStatus + "\n\n"
	ctr += fmt.Sprintf("  %s  %s\n", counterLabel.Render("Rebalances:"), counterValue.Render(fmt.Sprintf("%d", counters.Rebalances)))
	ctr += fmt.Sprintf("  %s  %s\n", counterLabel.Render("Sats moved:"), counterValue.Render(formatSats(counters.SatsMoved)))
	ctr += fmt.Sprintf("  %s  %s\n", counterLabel.Render("Fees saved:"), counterValue.Render("≈ "+formatSats(counters.FeesSaved)+" sats"))
	ctr += "\n"
	ctr += fmt.Sprintf("  %s  %s\n", counterLabel.Render("Payments:  "), counterValue.Render(fmt.Sprintf("%d", counters.Payments)))
	ctr += fmt.Sprintf("  %s  %s\n", counterLabel.Render("Imbalance: "), counterValue.Render(fmt.Sprintf("%.0f%%", imbalance)))
	ctrPanel := panelStyle.Width(rightW).Render(ctr)

	midRow := lipgloss.JoinHorizontal(lipgloss.Top, distPanel, ctrPanel)

	// Event log
	evHeader := lipgloss.NewStyle().Bold(true).Render("EVENT LOG")
	maxEvents := 5
	if m.height > 50 {
		maxEvents = 8
	}
	start := 0
	if len(events) > maxEvents {
		start = len(events) - maxEvents
	}
	var evLines []string
	for _, e := range events[start:] {
		ts := eventTimeStyle.Render(e.Time.Format("15:04:05"))
		evLines = append(evLines, fmt.Sprintf("  %s %s", ts, eventStyle.Render(e.Text)))
	}
	evContent := evHeader + "\n" + strings.Join(evLines, "\n")
	evPanel := panelStyle.Width(m.width - 2).Render(evContent)

	// Footer
	footer := lipgloss.NewStyle().Foreground(lipgloss.Color("8")).Render(
		"  q: quit  p: pause/resume traffic")

	return titleLine + "\n" + topRow + "\n" + midRow + "\n" + evPanel + "\n" + footer
}
