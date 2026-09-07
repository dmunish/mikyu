package repl

import (
	"fmt"
	"image/color"
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

var logoGradient = []color.Color{
	lipgloss.Color("#ffc279"),
	lipgloss.Color("#ff8851"),
	lipgloss.Color("#f84f44"),
	lipgloss.Color("#d2235b"),
	lipgloss.Color("#a01270"),
	lipgloss.Color("#68116c"),
}

var (
	accentColor  = lipgloss.Color("#ffc279")
	promptStyle  = lipgloss.NewStyle().Foreground(accentColor).Bold(true)
	infoStyle    = lipgloss.NewStyle().Foreground(accentColor)
	errStyle     = lipgloss.NewStyle().Foreground(lipgloss.Color("196")).Bold(true)
	dimStyle     = lipgloss.NewStyle().Foreground(lipgloss.Color("240"))
	confirmArrow = lipgloss.NewStyle().Foreground(lipgloss.Color("#d2235b")).Bold(true).Render(">")
)

var (
	taglineStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("243")). // Subtle gray
			MarginBottom(1)

	cmdPrefixStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("240"))

	cmdNameStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("255")). // White
			Bold(true)

	cmdArgStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("245"))

	cmdDescStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("243"))

	cmdReqArgStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#d2235b"))

	tryLabelStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("255")). // White
			Bold(true)

	tryCmdStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#ffc279"))

	statusStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("241"))

	statusTargetStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("248")).
				Underline(true)

	warnTagStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#d2235b")).
			Bold(true)
)

func PrintREPLBanner(target string) {
	var b strings.Builder

	// 1. Logo & Tagline — per-line fire gradient
	lines := strings.Split(strings.TrimSpace(mikyuASCII), "\n")
	var logoLines []string
	for i, line := range lines {
		colorIdx := i
		if colorIdx >= len(logoGradient) {
			colorIdx = len(logoGradient) - 1
		}
		logoLines = append(logoLines,
			lipgloss.NewStyle().
				Foreground(logoGradient[colorIdx]).
				Bold(true).
				Render(line))
	}
	logo := strings.Join(logoLines, "\n")
	tagline := taglineStyle.Render("High-performance, low-footprint message queue")

	header := lipgloss.NewStyle().MarginTop(1).MarginLeft(2).
		Render(lipgloss.JoinVertical(lipgloss.Left, logo, tagline))

	b.WriteString(header)
	b.WriteString("\n\n")

	// 2. Command Reference Table
	commands := []struct {
		cmd  string
		arg  string
		desc string
	}{
		{"produce", "<queue> <msg>", "Produce text or file payload (@file)"},
		{"read", "<queue> <offset>", "Read record at logical offset"},
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

	// 3. Quickstart Hint — yellow accent for the example command
	tryLabel := tryLabelStyle.Render("try: ")
	tryCmd := tryCmdStyle.Render("produce events \"hello world\"")
	b.WriteString(fmt.Sprintf("  %s%s\n\n", tryLabel, tryCmd))

	// 4. Connection Status Footer
	statusText := statusStyle.Render("Connected to daemon at ")
	statusTarget := statusTargetStyle.Render(target)
	b.WriteString(fmt.Sprintf("  %s%s\n\n", statusText, statusTarget))

	fmt.Print(b.String())
}
