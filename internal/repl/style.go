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

// Palette & Colors
var (
	logoGradient = []color.Color{
		lipgloss.Color("#ffc279"),
		lipgloss.Color("#ff8851"),
		lipgloss.Color("#f84f44"),
		lipgloss.Color("#d2235b"),
		lipgloss.Color("#a01270"),
		lipgloss.Color("#68116c"),
	}
	accentColor = lipgloss.Color("#ffc279")
)

// General Output & Messages
var (
	promptStyle    = lipgloss.NewStyle().Foreground(accentColor).Bold(true)
	infoStyle      = lipgloss.NewStyle().Foreground(accentColor)
	successStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("42")).Bold(true)
	errStyle       = lipgloss.NewStyle().Foreground(lipgloss.Color("196")).Bold(true)
	dimStyle       = lipgloss.NewStyle().Foreground(lipgloss.Color("243"))
	warnStyle      = lipgloss.NewStyle().Foreground(lipgloss.Color("214")).Bold(true)
	warnTagStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("#ee427d")).Bold(true)
	queueNameStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("255"))
)

// Record Output (read & tail)
var (
	recordOffsetStyle  = lipgloss.NewStyle().Foreground(lipgloss.Color("243"))
	recordDividerStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("243"))
	recordHeaderStyle  = lipgloss.NewStyle().Foreground(accentColor).Bold(true)
	recordValueStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("255"))
)

// Header & Quickstart Banner
var (
	taglineStyle      = lipgloss.NewStyle().Foreground(lipgloss.Color("243")).MarginBottom(1)
	tryLabelStyle     = lipgloss.NewStyle().Foreground(lipgloss.Color("255")).Bold(true)
	tryCmdStyle       = lipgloss.NewStyle().Foreground(lipgloss.Color("#ffc279"))
	statusStyle       = lipgloss.NewStyle().Foreground(lipgloss.Color("243"))
	statusTargetStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("248")).Underline(true)
)

// Command Table & Help
var (
	cmdPrefixStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("243"))
	cmdNameStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("255")).Bold(true)
	cmdArgStyle    = lipgloss.NewStyle().Foreground(lipgloss.Color("245"))
	cmdReqArgStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("#ee427d"))
	cmdDescStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("243"))
)

// Confirmation Prompts
var (
	confirmArrow         = lipgloss.NewStyle().Foreground(lipgloss.Color("#ee427d")).Bold(true).Render(">")
	confirmFocusedButton = lipgloss.NewStyle().Foreground(lipgloss.Color("#ee427d")).Bold(true).Padding(0, 1)
	confirmBlurredButton = lipgloss.NewStyle().Foreground(lipgloss.Color("245")).Padding(0, 1)
)

var (
	taskCardBorderStyle = lipgloss.NewStyle().
				Border(lipgloss.NormalBorder()).
				Padding(0, 1)

	taskKeyStyle     = lipgloss.NewStyle().Foreground(lipgloss.Color("245"))
	taskValueStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("255"))
	taskLeaseStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("42"))
	taskPendingStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("243"))
	taskExpiredStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("196"))
	taskDeadStyle    = lipgloss.NewStyle().Foreground(lipgloss.Color("196"))
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

	// 2. Command Reference Table — essential commands for first-time onboarding
	commands := []struct {
		cmd  string
		arg  string
		desc string
	}{
		{"create", "<queue>", "Create a new queue"},
		{"produce", "<queue> <msg>", "Append record to write-ahead log"},
		{"publish", "<queue> <msg>", "Enqueue task into work pool"},
		{"pop", "[queue]", "Claim next available task lease"},
		{"help", "", "Show all available commands"},
	}

	for _, c := range commands {
		prefix := cmdPrefixStyle.Render("  > ")
		name := cmdNameStyle.Render(fmt.Sprintf("%-10s", c.cmd))
		argsFormatted := formatHelpArgs(c.arg, 18)
		desc := cmdDescStyle.Render(c.desc)

		b.WriteString(fmt.Sprintf("%s%s %s %s\n", prefix, name, argsFormatted, desc))
	}

	b.WriteString("\n")

	// 3. Quickstart Hint — yellow accent for the example command
	tryLabel := tryLabelStyle.Render("try: ")
	tryCmd := tryCmdStyle.Render("produce events \"hello world\"")
	b.WriteString(fmt.Sprintf("  %s%s\n\n", tryLabel, tryCmd))

	// 4. Connection Status Footer
	statusText := statusStyle.Render("Connected to daemon at ")
	statusTarget := statusTargetStyle.Render(target) + "\x1b[24m"
	b.WriteString(fmt.Sprintf("  %s%s\n\n", statusText, statusTarget))

	fmt.Print(b.String())
}
