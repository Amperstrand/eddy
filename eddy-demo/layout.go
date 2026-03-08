package main

import "github.com/charmbracelet/lipgloss"

var (
	titleStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("15")).
			Background(lipgloss.Color("62")).
			Padding(0, 2)

	panelStyle = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(lipgloss.Color("62")).
			Padding(0, 1)

	counterLabel = lipgloss.NewStyle().
			Foreground(lipgloss.Color("8"))

	counterValue = lipgloss.NewStyle().
			Foreground(lipgloss.Color("15")).
			Bold(true)

	eventStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("8"))

	eventTimeStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("12"))
)
