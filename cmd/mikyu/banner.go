package main

import (
	"fmt"
	"strings"

	"charm.land/lipgloss/v2"
)

const mikyuASCII = `
███╗   ███╗██╗██╗  ██╗██╗   ██╗██╗   ██╗
████╗ ████║██║██║ ██╔╝╚██╗ ██╔╝██║   ██║
██╔████╔██║██║█████═╝  ╚████╔╝ ██║   ██║
██║╚██╔╝██║██║██╔═██╗   ╚██╔╝  ██║   ██║
██║ ╚═╝ ██║██║██║  ██╗   ██║   ╚██████╔╝
╚═╝     ╚═╝╚═╝╚═╝  ╚═╝   ╚═╝    ╚═════╝
`

var (
	logoStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("205")) // Vibrant pink/magenta accent

	taglineStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("243")). // Subtle gray
			MarginBottom(1)

	cmdPrefixStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("240"))

	cmdNameStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("255")).
			Bold(true)

	cmdArgStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("245"))

	cmdDescStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("243"))

	tryLabelStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("255")).
			Bold(true)

	tryCmdStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("86")) // Cyan accent

	statusStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("241")).
			MarginTop(1)

	statusTargetStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("248")).
				Underline(true)
)

func PrintREPLBanner(target string) {
	var b strings.Builder

	// 1. Logo & Tagline
	logo := logoStyle.Render(strings.TrimSpace(mikyuASCII))
	tagline := taglineStyle.Render("High-performance zero-copy WAL message queue engine")
	header := lipgloss.JoinVertical(lipgloss.Left, logo, tagline)

	b.WriteString(header)
	b.WriteString("\n\n")

	// 2. Command Reference Table
	commands := []struct {
		cmd  string
		arg  string
		desc string
	}{
		{"produce", "<queue> [msg]", "Produce text or file payload (@file)"},
		{"read", "<queue> [offset]", "Read record at logical offset"},
		{"list", "", "List all active queues"},
		{"stats", "", "Display storage & throughput metrics"},
		{"help", "", "Show all commands"},
	}

	for _, c := range commands {
		prefix := cmdPrefixStyle.Render("  $ ")
		name := cmdNameStyle.Render(fmt.Sprintf("%-8s", c.cmd))
		arg := cmdArgStyle.Render(fmt.Sprintf("%-16s", c.arg))
		desc := cmdDescStyle.Render(c.desc)

		b.WriteString(fmt.Sprintf("%s%s %s %s\n", prefix, name, arg, desc))
	}

	b.WriteString("\n")

	// 3. Quickstart Hint
	tryLabel := tryLabelStyle.Render("try: ")
	tryCmd := tryCmdStyle.Render("produce events \"hello world\"")
	b.WriteString(fmt.Sprintf("  %s%s\n\n", tryLabel, tryCmd))

	// 4. Connection Status Footer
	statusText := statusStyle.Render("Connected to daemon at ")
	statusTarget := statusTargetStyle.Render(target)
	b.WriteString(fmt.Sprintf("  %s%s\n\n", statusText, statusTarget))

	fmt.Print(b.String())
}
