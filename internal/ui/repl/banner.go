package repl

import (
	"fmt"
	"strings"

	"charm.land/lipgloss/v2"
	"github.com/dmunish/mikyu/internal/daemon"
	"github.com/dmunish/mikyu/internal/ui/style"
)

const mikyuASCII = `
███╗   ███╗██╗██╗  ██╗██╗   ██╗██╗   ██╗
████╗ ████║██║██║ ██╔╝╚██╗ ██╔╝██║   ██║
██╔████╔██║██║█████═╝  ╚████╔╝ ██║   ██║
██║╚██╔╝██║██║██╔═██╗   ╚██╔╝  ██║   ██║
██║ ╚═╝ ██║██║██║  ██╗   ██║   ╚██████╔╝
╚═╝     ╚═╝╚═╝╚═╝  ╚═╝   ╚═╝    ╚═════╝
`

// PrintBanner draws the welcome screen: logo, quickstart card, footer.
func PrintBanner(target, dir string) {
	var b strings.Builder

	lines := strings.Split(strings.TrimSpace(mikyuASCII), "\n")
	gradient := style.Default.Gradient
	var logoLines []string
	for i, line := range lines {
		idx := i
		if idx >= len(gradient) {
			idx = len(gradient) - 1
		}
		logoLines = append(logoLines, lipgloss.NewStyle().
			Foreground(gradient[idx]).
			Bold(true).
			Render(line))
	}
	logo := strings.Join(logoLines, "\n")
	tagline := taglineStyle.Render("High-performance, low-footprint message queue")
	header := lipgloss.NewStyle().MarginTop(1).MarginLeft(2).
		Render(lipgloss.JoinVertical(lipgloss.Left, logo, tagline))

	b.WriteString(header)
	b.WriteString("\n\n")

	commands := []struct{ cmd, arg, desc string }{
		{"create", "<queue>", "Create a new queue"},
		{"append", "<queue> <msg>", "Append a record to the log"},
		{"enqueue", "<queue> <msg>", "Enqueue a task"},
		{"dequeue", "<queue>", "Lease the next due task"},
		{"open", "<dir>", "Work in-process on a data dir"},
		{"daemon", "start", "Launch a background daemon here"},
		{"help", "", "Show all available commands"},
	}
	for _, c := range commands {
		prefix := cmdPrefixStyle.Render("  > ")
		name := cmdNameStyle.Render(fmt.Sprintf("%-10s", c.cmd))
		args := formatHelpArgs(c.arg, 18)
		desc := cmdDescStyle.Render(c.desc)
		b.WriteString(fmt.Sprintf("%s%s %s %s\n", prefix, name, args, desc))
	}
	b.WriteString("\n")

	tryLabel := tryLabelStyle.Render("try: ")
	tryCmd := tryCmdStyle.Render(`daemon start`)
	if dir != "" {
		tryCmd = tryCmdStyle.Render(`append events "hello world"`)
	}
	b.WriteString(fmt.Sprintf("  %s%s\n\n", tryLabel, tryCmd))

	var statusText, where string
	switch {
	case dir != "":
		statusText = statusStyle.Render("Embedded data dir (connects on first command): ")
		where = dir
	case daemon.IsDaemonAlive(target):
		statusText = statusStyle.Render("Daemon serving ")
		where = target
	default:
		fmt.Print(b.String())
		return
	}
	statusTarget := statusTargetStyle.Render(where) + "\x1b[24m"
	b.WriteString(fmt.Sprintf("  %s%s\n\n", statusText, statusTarget))

	fmt.Print(b.String())
}
