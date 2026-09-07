package repl

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"charm.land/huh/v2"
	"charm.land/lipgloss/v2"
	"github.com/dmunish/mikyu"
	"github.com/dmunish/mikyu/internal/command"
	"github.com/dmunish/mikyu/internal/daemon"
)

type replSession struct {
	client mikyu.Client
	target string
	ctx    context.Context
}

// RunREPL starts the interactive REPL session with full TUI styling.
func RunREPL(target string) {
	if err := daemon.EnsureDaemonRunning(target); err != nil {
		fmt.Println(errStyle.Render(fmt.Sprintf("Error initializing daemon: %v", err)))
		return
	}

	client, err := mikyu.Dial(target)
	if err != nil {
		fmt.Println(errStyle.Render(fmt.Sprintf("Failed to connect to daemon at %s: %v", target, err)))
		return
	}
	defer client.Close()

	fmt.Print("\x1b[2J\x1b[H")
	PrintREPLBanner(target)

	sess := &replSession{
		client: client,
		target: target,
		ctx:    context.Background(),
	}

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print(promptStyle.Render("mikyu> "))
		if !scanner.Scan() {
			if err := scanner.Err(); err != nil {
				fmt.Println(errStyle.Render(fmt.Sprintf("\nInput read error: %v", err)))
			} else {
				fmt.Println("\nGoodbye!")
			}
			break
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		if sess.dispatch(line) {
			return
		}
	}
}

func (s *replSession) dispatch(line string) bool {
	args := parseREPLArgs(line)
	cmd := strings.ToLower(args[0])

	switch cmd {
	case "exit", "quit":
		fmt.Println("Goodbye!")
		return true

	case "help":
		printREPLHelp()

	case "list":
		s.handleList()

	case "create":
		s.handleCreate(args)

	case "delete":
		s.handleDelete(args)

	case "produce":
		s.handleProduce(args)

	case "read":
		s.handleRead(args)

	case "tail":
		s.handleTail(args)

	case "stats":
		s.handleStats()

	case "compact", "truncate":
		s.handleCompact(args)

	case "shutdown":
		return s.handleShutdown()

	default:
		fmt.Println(errStyle.Render(fmt.Sprintf("Unknown command %q. Type 'help' for options.", cmd)))
	}

	return false
}

func (s *replSession) handleList() {
	queues, err := command.List(s.ctx, s.client)
	if err != nil {
		fmt.Println(errStyle.Render("Error: " + err.Error()))
		return
	}
	if len(queues) == 0 {
		fmt.Println(dimStyle.Render("No queues found."))
		return
	}
	fmt.Println(infoStyle.Render("Queues:"))
	for _, q := range queues {
		fmt.Println(infoStyle.Render(fmt.Sprintf("  ● %s", q)))
	}
}

func (s *replSession) handleCreate(args []string) {
	if len(args) < 2 {
		fmt.Println(errStyle.Render("Usage: create <queue_name>"))
		return
	}
	name := args[1]
	if err := command.Create(s.ctx, s.client, name); err != nil {
		fmt.Println(errStyle.Render("Error: " + err.Error()))
		return
	}
	fmt.Println(infoStyle.Render(fmt.Sprintf("● Queue %q created.", name)))
}

func (s *replSession) handleDelete(args []string) {
	if len(args) < 2 {
		fmt.Println(errStyle.Render("Usage: delete <queue_name>"))
		return
	}
	name := args[1]
	confirmFn := func() (bool, error) {
		return confirm(fmt.Sprintf("Are you sure you want to delete queue %q?", name)), nil
	}

	executed, err := command.Delete(s.ctx, s.client, name, confirmFn)
	if err != nil {
		fmt.Println(errStyle.Render("Error: " + err.Error()))
		return
	}
	if !executed {
		fmt.Println("Cancelled.")
		return
	}
	fmt.Println(infoStyle.Render(fmt.Sprintf("● Queue %q deleted.", name)))
}

func (s *replSession) handleProduce(args []string) {
	if len(args) < 3 {
		fmt.Println(errStyle.Render("Usage: produce <queue_name> <payload | @file>"))
		return
	}
	name, input := args[1], args[2]
	off, payloadLen, err := command.Produce(s.ctx, s.client, name, input)
	if err != nil {
		fmt.Println(errStyle.Render("Error: " + err.Error()))
		return
	}
	fmt.Println(infoStyle.Render(fmt.Sprintf("● Produced %d bytes to %q at offset %d", payloadLen, name, off)))
}

func (s *replSession) handleRead(args []string) {
	if len(args) < 3 {
		fmt.Println(errStyle.Render("Usage: read <queue_name> <offset>"))
		return
	}
	name := args[1]
	off, err := strconv.ParseUint(args[2], 10, 64)
	if err != nil {
		fmt.Println(errStyle.Render("Invalid offset format"))
		return
	}
	msg, err := command.Read(s.ctx, s.client, name, off)
	if err != nil {
		fmt.Println(errStyle.Render("Error: " + err.Error()))
		return
	}
	fmt.Printf("[%d] ID: %s | Time: %s | Payload: %s\n",
		msg.Offset, msg.ID, msg.Timestamp.Format("15:04:05.000"), string(msg.Payload))
}

func (s *replSession) handleTail(args []string) {
	if len(args) < 2 {
		fmt.Println(errStyle.Render("Usage: tail <queue_name> [start_offset]"))
		return
	}
	name := args[1]
	var startOffset uint64 = 0
	if len(args) >= 3 {
		startOffset, _ = strconv.ParseUint(args[2], 10, 64)
	}

	sigCtx, stopSig := signal.NotifyContext(s.ctx, os.Interrupt, syscall.SIGTERM)
	defer stopSig()

	fmt.Println(dimStyle.Render(fmt.Sprintf("Tailing %q from offset %d (Press Ctrl+C to stop stream)...", name, startOffset)))
	err := command.Tail(sigCtx, s.client, name, startOffset, func(msg mikyu.Message) error {
		prefix := fmt.Sprintf("[%d] ID: %s | Time: %s | Payload: ",
			msg.Offset, msg.ID, msg.Timestamp.Format("15:04:05.000"))

		termWidth := command.GetTerminalWidth()
		avail := -1
		if termWidth > 0 {
			avail = termWidth - lipgloss.Width(prefix)
		}
		payloadStr := command.FormatSingleLinePayload(msg.Payload, avail)

		fmt.Printf("%s%s\n", prefix, payloadStr)
		return nil
	})
	if err != nil {
		if sigCtx.Err() != nil {
			fmt.Println(infoStyle.Render("\nStream stopped."))
		} else {
			fmt.Println(errStyle.Render("Stream error: " + err.Error()))
		}
	}
}

func (s *replSession) handleCompact(args []string) {
	if len(args) < 2 {
		fmt.Println(errStyle.Render("Usage: compact <queue_name> [min_offset]"))
		return
	}
	name := args[1]
	var minOffset uint64 = 0
	if len(args) >= 3 {
		var err error
		minOffset, err = strconv.ParseUint(args[2], 10, 64)
		if err != nil {
			fmt.Println(errStyle.Render("Invalid min_offset format"))
			return
		}
	}

	tag := warnTagStyle.Render("[IRREVERSIBLE]")
	var prompt string

	if minOffset > 0 {
		prompt = fmt.Sprintf("%s Truncate queue %q before offset %d?", tag, name, minOffset)
	} else {
		prompt = fmt.Sprintf("%s Truncate physical segments for queue %q?", tag, name)
	}

	confirmFn := func() (bool, error) {
		return confirm(prompt), nil
	}

	reclaimed, executed, err := command.Compact(s.ctx, s.client, name, minOffset, confirmFn)
	if err != nil {
		fmt.Println(errStyle.Render("Error: " + err.Error()))
		return
	}
	if !executed {
		fmt.Println("Cancelled.")
		return
	}
	fmt.Println(infoStyle.Render(fmt.Sprintf("● Queue %q truncated (%d bytes reclaimed).", name, reclaimed)))
}

func (s *replSession) handleStats() {
	st, err := command.Stats(s.ctx, s.client)
	if err != nil {
		fmt.Println(errStyle.Render("Error: " + err.Error()))
		return
	}
	fmt.Printf("Active Queues: %d | Total Storage: %d Bytes\n", st.ActiveQueues, st.TotalBytes)
}

func (s *replSession) handleShutdown() bool {
	confirmFn := func() (bool, error) {
		if confirm("Are you sure you want to shut down the daemon?") {
			fmt.Println(infoStyle.Render("Sending shutdown command to daemon..."))
			return true, nil
		}
		return false, nil
	}

	executed, err := command.Shutdown(s.target, confirmFn)
	if err != nil {
		fmt.Println(errStyle.Render("Error: " + err.Error()))
		return false
	}
	if !executed {
		fmt.Println("Cancelled.")
		return false
	}
	fmt.Println("Daemon stopped. Exiting shell.")
	return true
}

func parseREPLArgs(line string) []string {
	var args []string
	var current strings.Builder
	inQuotes := false
	started := false
	for _, r := range line {
		switch r {
		case '"':
			inQuotes = !inQuotes
			started = true
		case ' ':
			if inQuotes {
				current.WriteRune(r)
				started = true
			} else if started {
				args = append(args, current.String())
				current.Reset()
				started = false
			}
		default:
			current.WriteRune(r)
			started = true
		}
	}
	if started {
		args = append(args, current.String())
	}
	return args
}

func confirm(prompt string) bool {
	var isConfirmed bool
	fullTitle := confirmArrow + " " + prompt

	theme := huh.ThemeFunc(func(isDark bool) *huh.Styles {
		s := huh.ThemeCharm(isDark)

		s.Focused.Base = lipgloss.NewStyle().Padding(0).Margin(0)
		s.Blurred.Base = lipgloss.NewStyle().Padding(0).Margin(0)

		s.Focused.Title = lipgloss.NewStyle().Foreground(lipgloss.Color("255")).Bold(true)
		s.Blurred.Title = lipgloss.NewStyle().Foreground(lipgloss.Color("255"))

		s.Focused.FocusedButton = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#EE427D")).
			Bold(true).
			Padding(0, 1)
		s.Focused.BlurredButton = lipgloss.NewStyle().
			Foreground(lipgloss.Color("245")).
			Padding(0, 1)

		return s
	})

	err := huh.NewForm(
		huh.NewGroup(
			huh.NewConfirm().
				Title(fullTitle).
				Affirmative("Yes").
				Negative("No").
				Value(&isConfirmed).
				Inline(true).
				WithButtonAlignment(lipgloss.Left),
		).WithShowHelp(false),
	).WithTheme(theme).Run()

	if err != nil {
		return false
	}
	return isConfirmed
}

func printREPLHelp() {
	var b strings.Builder
	b.WriteString("\n")
	b.WriteString(infoStyle.Render("  Available Commands:"))
	b.WriteString("\n\n")

	commands := []struct {
		cmd  string
		arg  string
		desc string
	}{
		{"list", "", "List all active queues"},
		{"create", "<queue>", "Create a new queue"},
		{"delete", "<queue>", "Delete a queue and its stored records"},
		{"produce", "<q> <msg|@f>", "Produce text, file payload (@file), or stdin (-)"},
		{"read", "<queue> <off>", "Read record at specific logical offset"},
		{"tail", "<queue> [off]", "Stream live records continuously"},
		{"stats", "", "Display engine storage metrics"},
		{"compact", "<q> [off]", "Truncate physical log segments before offset"},
		{"shutdown", "", "Stop the background daemon process"},
		{"exit / quit", "", "Exit the REPL shell"},
	}

	for _, c := range commands {
		prefix := cmdPrefixStyle.Render("  $ ")
		name := cmdNameStyle.Render(fmt.Sprintf("%-12s", c.cmd))
		argsFormatted := formatHelpArgs(c.arg, 18)
		desc := cmdDescStyle.Render(c.desc)

		b.WriteString(fmt.Sprintf("%s%s %s %s\n", prefix, name, argsFormatted, desc))
	}
	b.WriteString("\n")
	fmt.Print(b.String())
}

func formatHelpArgs(raw string, width int) string {
	if raw == "" {
		return strings.Repeat(" ", width)
	}

	var sb strings.Builder
	visibleLen := 0

	tokens := strings.Split(raw, " ")
	for i, token := range tokens {
		if i > 0 {
			sb.WriteString(" ")
			visibleLen++
		}
		if strings.HasPrefix(token, "<") && strings.HasSuffix(token, ">") {
			sb.WriteString(cmdReqArgStyle.Render(token))
		} else {
			sb.WriteString(cmdArgStyle.Render(token))
		}
		visibleLen += len(token)
	}

	if visibleLen < width {
		sb.WriteString(strings.Repeat(" ", width-visibleLen))
	}
	return sb.String()
}
