package repl

import (
	"charm.land/lipgloss/v2"
	"github.com/dmunish/mikyu/internal/ui/style"
)

var pal = style.Default

// General output & messages.
var (
	promptStyle    = lipgloss.NewStyle().Foreground(pal.Primary).Bold(true)
	infoStyle      = lipgloss.NewStyle().Foreground(pal.Primary)
	successStyle   = lipgloss.NewStyle().Foreground(pal.Success).Bold(true)
	errStyle       = lipgloss.NewStyle().Foreground(pal.Error).Bold(true)
	dimStyle       = lipgloss.NewStyle().Foreground(pal.Muted)
	warnStyle      = lipgloss.NewStyle().Foreground(pal.Warning).Bold(true)
	warnTagStyle   = lipgloss.NewStyle().Foreground(pal.Danger).Bold(true)
	queueNameStyle = lipgloss.NewStyle().Foreground(pal.Bright)
)

// Record output (read & tail).
var (
	recordOffsetStyle  = lipgloss.NewStyle().Foreground(pal.Muted)
	recordDividerStyle = lipgloss.NewStyle().Foreground(pal.Muted)
	recordHeaderStyle  = lipgloss.NewStyle().Foreground(pal.Primary).Bold(true)
	recordValueStyle   = lipgloss.NewStyle().Foreground(pal.Bright)
)

// Banner.
var (
	taglineStyle      = lipgloss.NewStyle().Foreground(pal.Muted).MarginBottom(1)
	tryLabelStyle     = lipgloss.NewStyle().Foreground(pal.Bright).Bold(true)
	tryCmdStyle       = lipgloss.NewStyle().Foreground(pal.Primary)
	statusStyle       = lipgloss.NewStyle().Foreground(pal.Muted)
	statusTargetStyle = lipgloss.NewStyle().Foreground(pal.Subtle).Underline(true)
)

// Command table & help.
var (
	cmdPrefixStyle = lipgloss.NewStyle().Foreground(pal.Muted)
	cmdNameStyle   = lipgloss.NewStyle().Foreground(pal.Bright).Bold(true)
	cmdDescStyle   = lipgloss.NewStyle().Foreground(pal.Muted)
)

// Confirmation prompts.
var (
	confirmArrow         = lipgloss.NewStyle().Foreground(pal.Danger).Bold(true).Render(">")
	confirmFocusedButton = lipgloss.NewStyle().Foreground(pal.Danger).Bold(true).Padding(0, 1)
	confirmBlurredButton = lipgloss.NewStyle().Foreground(pal.Subtle).Padding(0, 1)
)

// Task cards.
var (
	taskCardBorderStyle = lipgloss.NewStyle().
				Border(lipgloss.NormalBorder()).
				Padding(0, 1)
	taskKeyStyle     = lipgloss.NewStyle().Foreground(pal.Subtle)
	taskValueStyle   = lipgloss.NewStyle().Foreground(pal.Bright)
	taskLeaseStyle   = lipgloss.NewStyle().Foreground(pal.Success)
	taskPendingStyle = lipgloss.NewStyle().Foreground(pal.Muted)
	taskExpiredStyle = lipgloss.NewStyle().Foreground(pal.Error)
	taskDeadStyle    = lipgloss.NewStyle().Foreground(pal.Error)
)
