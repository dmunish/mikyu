package repl

import (
	"fmt"
	"strings"

	"charm.land/lipgloss/v2"
	"github.com/dmunish/mikyu/internal/ui/command"
)

const mikyuASCII = `
███╗   ███╗██╗██╗  ██╗██╗   ██╗██╗   ██╗
████╗ ████║██║██║ ██╔╝╚██╗ ██╔╝██║   ██║
██╔████╔██║██║█████═╝  ╚████╔╝ ██║   ██║
██║╚██╔╝██║██║██╔═██╗   ╚██╔╝  ██║   ██║
██║ ╚═╝ ██║██║██║  ██╗   ██║   ╚██████╔╝
╚═╝     ╚═╝╚═╝╚═╝  ╚═╝   ╚═╝    ╚═════╝
`

// The palette lives in command so the CLI, REPL, and help renderer share it.

// General Output & Messages
var (
	promptStyle    = lipgloss.NewStyle().Foreground(command.AccentColor).Bold(true)
	infoStyle      = lipgloss.NewStyle().Foreground(command.AccentColor)
	successStyle   = lipgloss.NewStyle().Foreground(command.SuccessColor).Bold(true)
	errStyle       = lipgloss.NewStyle().Foreground(command.ErrorColor).Bold(true)
	dimStyle       = lipgloss.NewStyle().Foreground(command.MutedColor)
	warnStyle      = lipgloss.NewStyle().Foreground(command.WarnColor).Bold(true)
	warnTagStyle   = lipgloss.NewStyle().Foreground(command.DangerColor).Bold(true)
	queueNameStyle = lipgloss.NewStyle().Foreground(command.BrightColor)
)

// Record Output (read & tail)
var (
	recordOffsetStyle  = lipgloss.NewStyle().Foreground(command.MutedColor)
	recordDividerStyle = lipgloss.NewStyle().Foreground(command.MutedColor)
	recordHeaderStyle  = lipgloss.NewStyle().Foreground(command.AccentColor).Bold(true)
	recordValueStyle   = lipgloss.NewStyle().Foreground(command.BrightColor)
)

// Header & Quickstart Banner
var (
	taglineStyle      = lipgloss.NewStyle().Foreground(command.MutedColor).MarginBottom(1)
	tryLabelStyle     = lipgloss.NewStyle().Foreground(command.BrightColor).Bold(true)
	tryCmdStyle       = lipgloss.NewStyle().Foreground(command.AccentColor)
	statusStyle       = lipgloss.NewStyle().Foreground(command.MutedColor)
	statusTargetStyle = lipgloss.NewStyle().Foreground(command.SubtleColor).Underline(true)
)

// Command Table & Help
var (
	cmdPrefixStyle = lipgloss.NewStyle().Foreground(command.MutedColor)
	cmdNameStyle   = lipgloss.NewStyle().Foreground(command.BrightColor).Bold(true)
	cmdDescStyle   = lipgloss.NewStyle().Foreground(command.MutedColor)
)

// Confirmation Prompts
var (
	confirmArrow         = lipgloss.NewStyle().Foreground(command.DangerColor).Bold(true).Render(">")
	confirmFocusedButton = lipgloss.NewStyle().Foreground(command.DangerColor).Bold(true).Padding(0, 1)
	confirmBlurredButton = lipgloss.NewStyle().Foreground(command.SubtleColor).Padding(0, 1)
)

var (
	taskCardBorderStyle = lipgloss.NewStyle().
				Border(lipgloss.NormalBorder()).
				Padding(0, 1)

	taskKeyStyle     = lipgloss.NewStyle().Foreground(command.SubtleColor)
	taskValueStyle   = lipgloss.NewStyle().Foreground(command.BrightColor)
	taskLeaseStyle   = lipgloss.NewStyle().Foreground(command.SuccessColor)
	taskPendingStyle = lipgloss.NewStyle().Foreground(command.MutedColor)
	taskExpiredStyle = lipgloss.NewStyle().Foreground(command.ErrorColor)
	taskDeadStyle    = lipgloss.NewStyle().Foreground(command.ErrorColor)
)

// PrintREPLBanner draws the welcome screen: logo, quickstart card, footer.
func PrintREPLBanner(target, dir string) {
	var b strings.Builder

	lines := strings.Split(strings.TrimSpace(mikyuASCII), "\n")
	var logoLines []string
	for i, line := range lines {
		idx := i
		if idx >= len(command.LogoGradient) {
			idx = len(command.LogoGradient) - 1
		}
		logoLines = append(logoLines, lipgloss.NewStyle().
			Foreground(command.LogoGradient[idx]).
			Bold(true).
			Render(line))
	}
	logo := strings.Join(logoLines, "\n")
	tagline := taglineStyle.Render("High-performance, low-footprint message queue")

	header := lipgloss.NewStyle().MarginTop(1).MarginLeft(2).
		Render(lipgloss.JoinVertical(lipgloss.Left, logo, tagline))

	b.WriteString(header)
	b.WriteString("\n\n")

	commands := []struct {
		cmd  string
		arg  string
		desc string
	}{
		{"create", "<queue>", "Create a new queue"},
		{"append", "<queue> <msg>", "Append a record to the log"},
		{"enqueue", "<queue> <msg>", "Enqueue a task"},
		{"dequeue", "[queue]", "Lease the next due task"},
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

	tryLabel := tryLabelStyle.Render("try: ")
	tryCmd := tryCmdStyle.Render(`append events "hello world"`)
	b.WriteString(fmt.Sprintf("  %s%s\n\n", tryLabel, tryCmd))

	statusText := statusStyle.Render("Connected to daemon at ")
	where := target
	if dir != "" {
		statusText = statusStyle.Render("Running embedded on ")
		where = dir
	}
	statusTarget := statusTargetStyle.Render(where) + "\x1b[24m"
	b.WriteString(fmt.Sprintf("  %s%s\n\n", statusText, statusTarget))

	fmt.Print(b.String())
}
