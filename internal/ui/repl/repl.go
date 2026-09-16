package repl

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"charm.land/huh/v2"
	"charm.land/lipgloss/v2"
	"github.com/dmunish/mikyu"
	"github.com/dmunish/mikyu/internal/spec"
	"github.com/dmunish/mikyu/internal/ui/command"
)

type replSession struct {
	client  mikyu.Client
	target  string
	dir     string
	session spec.Session
	ctx     context.Context
	scanner *bufio.Scanner

	activeQueue   string
	lastTaskQueue string
	lastTaskID    string
	done          bool
}

// replHandlers maps every verb to its REPL implementation, plus the session
// commands only the REPL has. It shares command.Verbs with the CLI.
var replHandlers = map[string]func(*replSession, []string){
	"use":      (*replSession).handleUse,
	"list":     (*replSession).handleList,
	"create":   (*replSession).handleCreate,
	"update":   (*replSession).handleUpdate,
	"describe": (*replSession).handleDescribe,
	"delete":   (*replSession).handleDelete,
	"append":   (*replSession).handleAppend,
	"read":     (*replSession).handleRead,
	"tail":     (*replSession).handleTail,
	"truncate": (*replSession).handleTruncate,
	"metrics":  (*replSession).handleMetrics,
	"shutdown": (*replSession).handleShutdown,
	"login":    (*replSession).handleLogin,
	"logout":   (*replSession).handleLogout,

	"enqueue": (*replSession).handleEnqueue,
	"dequeue": (*replSession).handleDequeue,
	"ack":     (*replSession).handleAck,
	"nack":    (*replSession).handleNack,
	"extend":  (*replSession).handleExtend,
	"dlq":     (*replSession).handleDLQ,
	"dlq-ack": (*replSession).handleDLQAck,
}

// RunREPL starts the interactive REPL: embedded with --dir, remote otherwise.
func RunREPL(target string, args []string) error {
	global, _, err := command.ParseGlobal(target, args)
	if err != nil {
		return err
	}
	if global.Help {
		printREPLHelp()
		return nil
	}

	client, err := command.OpenClient(global.Session)
	if err != nil {
		return err
	}
	defer client.Close()

	fmt.Print("\x1b[2J\x1b[H")
	PrintREPLBanner(global.Session.Target, global.Session.DataDir)

	scanner := bufio.NewScanner(os.Stdin)
	sess := &replSession{
		client:  client,
		target:  global.Session.Target,
		dir:     global.Session.DataDir,
		session: global.Session,
		ctx:     context.Background(),
		scanner: scanner,
	}

	for {
		fmt.Print(sess.prompt())

		if !scanner.Scan() {
			if err := scanner.Err(); err != nil {
				fmt.Println(errStyle.Render(fmt.Sprintf("\nInput read error: %v", err)))
			} else {
				fmt.Println("\nGoodbye!")
			}
			return nil
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		if sess.dispatch(line) {
			return nil
		}
	}
}

func (s *replSession) dispatch(line string) bool {
	args := parseREPLArgs(line)
	if len(args) == 0 {
		return false
	}
	cmd := strings.ToLower(args[0])

	switch cmd {
	case "exit", "quit":
		fmt.Println("Goodbye!")
		return true
	case "help":
		printREPLHelp()
		return false
	}

	if handler, ok := replHandlers[cmd]; ok {
		handler(s, args[1:])
		return s.done
	}

	fmt.Println(errStyle.Render(fmt.Sprintf("Unknown command %q. Type 'help' for options.", cmd)))
	return false
}

// fail reports an error in the session's style.
func (s *replSession) fail(err error) {
	fmt.Println(errStyle.Render("Error: " + err.Error()))
}

// usage reports a malformed command line.
func (s *replSession) usage(text string) {
	fmt.Println(errStyle.Render("Usage: " + text))
}

func (s *replSession) handleUse(args []string) {
	if len(args) < 1 {
		if s.activeQueue == "" {
			fmt.Println(dimStyle.Render("No active queue. Use: use <queue>"))
		} else {
			fmt.Println(infoStyle.Render(fmt.Sprintf("Active queue: %s", s.activeQueue)))
		}
		return
	}

	name := args[0]

	if name == "-" || name == "none" {
		s.activeQueue = ""
		fmt.Println(infoStyle.Render("Cleared active queue."))
		return
	}

	queues, err := command.List(s.ctx, s.client)
	if err != nil {
		s.fail(err)
		return
	}

	if !slices.Contains(queues, name) {
		fmt.Println(errStyle.Render(fmt.Sprintf("Queue %q not found.", name)))
		return
	}

	s.activeQueue = name
	fmt.Println(successStyle.Render(fmt.Sprintf("● Active queue set to %q.", name)))
}

func (s *replSession) handleList(args []string) {
	queues, err := command.List(s.ctx, s.client)
	if err != nil {
		s.fail(err)
		return
	}
	if len(queues) == 0 {
		fmt.Println(dimStyle.Render("No queues found."))
		return
	}
	fmt.Println(infoStyle.Render("Queues:"))
	bullet := infoStyle.Render("●")
	for _, q := range queues {
		fmt.Printf("  %s %s\n", bullet, queueNameStyle.Render(q))
	}
}

func (s *replSession) handleCreate(args []string) {
	var patch spec.Spec
	rest, err := command.ParseAnywhere(command.QueueFlagSet("create", &patch), args)
	if err != nil {
		s.usage("create [flags] <queue_name> (" + err.Error() + ")")
		return
	}
	if len(rest) < 1 {
		s.usage("create [flags] <queue_name>")
		return
	}
	name := rest[0]
	if err := command.Create(s.ctx, s.client, name, command.SpecOption(patch)); err != nil {
		s.fail(err)
		return
	}
	fmt.Println(successStyle.Render(fmt.Sprintf("● Queue %q created.", name)))
}

func (s *replSession) handleUpdate(args []string) {
	var patch spec.Spec
	rest, err := command.ParseAnywhere(command.QueuePatchFlagSet("update", &patch), args)
	if err != nil {
		s.usage("update [flags] <queue_name> (" + err.Error() + ")")
		return
	}
	if len(rest) < 1 {
		s.usage("update [flags] <queue_name>")
		return
	}
	name := rest[0]

	if patch.Retention.Age != nil || patch.Retention.Bytes != nil {
		current, err := command.Describe(s.ctx, s.client, name)
		if err != nil {
			s.fail(err)
			return
		}
		if command.TightensRetention(current.Effective, patch) {
			tag := warnTagStyle.Render("[IRREVERSIBLE]")
			if !confirm(fmt.Sprintf("%s Shrink retention for queue %q?", tag, name)) {
				fmt.Println("Cancelled.")
				return
			}
		}
	}

	if err := command.Update(s.ctx, s.client, name, command.SpecOption(patch)); err != nil {
		s.fail(err)
		return
	}
	fmt.Println(successStyle.Render(fmt.Sprintf("● Queue %q updated.", name)))
}

func (s *replSession) handleDescribe(args []string) {
	var asJSON bool
	fs := command.NewFlagSet("describe")
	fs.BoolVar(&asJSON, "json", false, "emit the machine-readable description")

	rest, err := command.ParseAnywhere(fs, args)
	if err != nil {
		s.usage("describe <queue> [--json] (" + err.Error() + ")")
		return
	}
	if len(rest) < 1 {
		s.usage("describe <queue> [--json]")
		return
	}
	d, err := command.Describe(s.ctx, s.client, rest[0])
	if err != nil {
		s.fail(err)
		return
	}
	if asJSON {
		data, err := json.MarshalIndent(d, "", "  ")
		if err != nil {
			s.fail(err)
			return
		}
		fmt.Println(string(data))
		return
	}
	stored, _ := json.Marshal(d.Spec)
	effective, _ := json.Marshal(d.Effective)
	fmt.Println(infoStyle.Render(fmt.Sprintf("Queue %q", rest[0])))
	fmt.Printf("  stored:    %s\n", stored)
	fmt.Printf("  effective: %s\n", effective)
}

func (s *replSession) handleDelete(args []string) {
	if len(args) < 1 {
		s.usage("delete <queue_name>")
		return
	}
	name := args[0]
	confirmFn := func() (bool, error) {
		tag := warnTagStyle.Render("[IRREVERSIBLE]")
		return confirm(fmt.Sprintf("%s Are you sure you want to delete queue %q?", tag, name)), nil
	}

	executed, err := command.Delete(s.ctx, s.client, name, confirmFn)
	if err != nil {
		s.fail(err)
		return
	}
	if !executed {
		fmt.Println("Cancelled.")
		return
	}
	fmt.Println(successStyle.Render(fmt.Sprintf("● Queue %q deleted.", name)))
}

func (s *replSession) handleAppend(args []string) {
	if len(args) < 2 {
		s.usage("append <queue_name> <payload | @file>")
		return
	}
	name, input := args[0], args[1]
	off, payloadLen, err := command.Append(s.ctx, s.client, name, input)
	if err != nil {
		s.fail(err)
		return
	}
	fmt.Println(successStyle.Render(fmt.Sprintf("● Appended %d bytes to %q at offset %d", payloadLen, name, off)))
}

func (s *replSession) handleRead(args []string) {
	if len(args) < 2 {
		s.usage("read <queue_name> <offset>")
		return
	}
	name := args[0]
	off, err := strconv.ParseUint(args[1], 10, 64)
	if err != nil {
		fmt.Println(errStyle.Render("Invalid offset format"))
		return
	}
	msg, err := command.Read(s.ctx, s.client, name, off)
	if err != nil {
		s.fail(err)
		return
	}
	fmt.Println(formatRecord(msg))
}

func (s *replSession) handleTail(args []string) {
	if len(args) < 1 {
		s.usage("tail <queue_name> [start_offset]")
		return
	}
	name := args[0]
	var startOffset uint64 = 0
	if len(args) >= 2 {
		startOffset, _ = strconv.ParseUint(args[1], 10, 64)
	}

	sigCtx, stopSig := signal.NotifyContext(s.ctx, os.Interrupt, syscall.SIGTERM)
	defer stopSig()

	statusText := dimStyle.Render(fmt.Sprintf("Tailing %q from offset %d (Press Ctrl+C to stop stream)...", name, startOffset))
	fmt.Print(statusText)

	err := command.Tail(sigCtx, s.client, name, startOffset, func(msg mikyu.Message) error {
		fmt.Print("\r\x1b[2K")
		fmt.Println(formatRecord(msg))
		fmt.Print(statusText)
		return nil
	})

	fmt.Print("\r\x1b[2K")

	if err != nil {
		if sigCtx.Err() != nil {
			fmt.Println(infoStyle.Render("Stream stopped."))
		} else {
			fmt.Println(errStyle.Render("Stream error: " + err.Error()))
		}
	}
}

func (s *replSession) handleTruncate(args []string) {
	if len(args) < 1 {
		s.usage("truncate <queue_name> [min_offset]")
		return
	}
	name := args[0]
	var minOffset uint64 = 0
	if len(args) >= 2 {
		var err error
		minOffset, err = strconv.ParseUint(args[1], 10, 64)
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

	reclaimed, executed, err := command.Truncate(s.ctx, s.client, name, minOffset, confirmFn)
	if err != nil {
		s.fail(err)
		return
	}
	if !executed {
		fmt.Println("Cancelled.")
		return
	}
	fmt.Println(successStyle.Render(fmt.Sprintf("● Queue %q truncated (%d bytes reclaimed).", name, reclaimed)))
}

func (s *replSession) handleMetrics(args []string) {
	var watch bool
	fs := command.NewFlagSet("metrics")
	fs.BoolVar(&watch, "watch", false, "stream snapshots until interrupted")
	rest, err := command.ParseAnywhere(fs, args)
	if err != nil {
		s.usage("metrics [--watch] (" + err.Error() + ")")
		return
	}
	if len(rest) > 0 {
		s.usage("metrics [--watch]")
		return
	}

	render := func(snap mikyu.Metrics) {
		uptime := time.Duration(snap.Runtime.UptimeSec * float64(time.Second))
		queueCount := 0
		for _, qv := range snap.Queues {
			if qv.Name != "server" {
				queueCount++
			}
		}
		fmt.Printf("Uptime: %s | Data Dir: %s\n", uptime.Round(time.Second), snap.Storage.DataDir)
		fmt.Printf("Queues: %d | Total Storage: %d Bytes\n", queueCount, snap.Totals.Bytes)
	}

	if watch {
		sigCtx, stopSig := signal.NotifyContext(s.ctx, os.Interrupt, syscall.SIGTERM)
		defer stopSig()
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-sigCtx.Done():
				fmt.Println(infoStyle.Render("Watch stopped."))
				return
			case <-ticker.C:
				snap, err := command.Metrics(sigCtx, s.client)
				if err != nil {
					s.fail(err)
					return
				}
				render(snap)
			}
		}
	}

	snap, err := command.Metrics(s.ctx, s.client)
	if err != nil {
		s.fail(err)
		return
	}
	render(snap)
}

// handleLogin reads a token from the REPL's own scanner and verifies it
func (s *replSession) handleLogin(args []string) {
	target := s.target
	if len(args) == 1 {
		target = args[0]
	}
	fmt.Print("Token: ")
	if !s.scanner.Scan() {
		if err := s.scanner.Err(); err != nil {
			s.fail(err)
		} else {
			fmt.Println(errStyle.Render("No token provided."))
		}
		return
	}
	if err := command.LoginWithToken(s.ctx, target, strings.TrimSpace(s.scanner.Text())); err != nil {
		s.fail(err)
		return
	}
	fmt.Println(successStyle.Render(fmt.Sprintf("● Stored token for %s.", target)))
}
func (s *replSession) handleLogout(args []string) {
	target := s.target
	if len(args) == 1 {
		target = args[0]
	}
	if err := command.Logout(target); err != nil {
		s.fail(err)
		return
	}
	fmt.Println(successStyle.Render(fmt.Sprintf("● Forgot token for %s.", target)))
}

func (s *replSession) handleShutdown(args []string) {
	if s.dir != "" {
		fmt.Println(errStyle.Render("Shutdown needs a daemon; --dir runs embedded."))
		return
	}
	confirmFn := func() (bool, error) {
		if confirm("Are you sure you want to shut down the daemon?") {
			fmt.Println(infoStyle.Render("Sending shutdown command to daemon..."))
			return true, nil
		}
		return false, nil
	}

	executed, err := command.Shutdown(s.target, s.session, confirmFn)
	if err != nil {
		s.fail(err)
		return
	}
	if !executed {
		fmt.Println("Cancelled.")
		return
	}
	fmt.Println("Daemon stopped. Exiting shell.")
	s.done = true
}

func formatRecord(msg mikyu.Message) string {
	offsetStr := recordOffsetStyle.Render(fmt.Sprintf("[%d]", msg.Offset))
	divider := recordDividerStyle.Render("|")

	idLabel := recordHeaderStyle.Render("ID:")
	idVal := recordValueStyle.Render(msg.ID)

	timeLabel := recordHeaderStyle.Render("Time:")
	timeVal := recordValueStyle.Render(msg.Timestamp.Format("15:04:05.000"))

	payloadLabel := recordHeaderStyle.Render("Payload:")

	prefixRaw := fmt.Sprintf("[%d] ID: %s | Time: %s | Payload: ",
		msg.Offset, msg.ID, msg.Timestamp.Format("15:04:05.000"))

	termWidth := command.GetTerminalWidth()
	avail := -1
	if termWidth > 0 {
		avail = termWidth - lipgloss.Width(prefixRaw)
	}
	payloadStr := recordValueStyle.Render(command.FormatSingleLinePayload(msg.Payload, avail))

	return fmt.Sprintf("%s %s %s %s %s %s %s %s %s",
		offsetStr, idLabel, idVal, divider, timeLabel, timeVal, divider, payloadLabel, payloadStr)
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

		s.Focused.FocusedButton = confirmFocusedButton
		s.Focused.BlurredButton = confirmBlurredButton

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
	b.WriteString(infoStyle.Render("  Log Commands:"))
	b.WriteString("\n")
	writeVerbHelp(&b, command.KindLog)

	b.WriteString("\n")
	b.WriteString(infoStyle.Render("  Task Commands:"))
	b.WriteString("\n")
	writeVerbHelp(&b, command.KindTask)

	b.WriteString("\n")
	b.WriteString(infoStyle.Render("  Admin Commands:"))
	b.WriteString("\n")
	writeVerbHelp(&b, command.KindAdmin)

	b.WriteString("\n")
	writeHelpCommands(&b, []struct {
		cmd  string
		arg  string
		desc string
	}{
		{"use", "<q>", "Set active session queue context"},
		{"help", "", "Show this command list"},
		{"exit / quit", "", "Close the interactive shell"},
	})

	b.WriteString("\n")
	fmt.Print(b.String())
}

// writeVerbHelp prints one verb kind from the shared table.
func writeVerbHelp(b *strings.Builder, kind command.VerbKind) {
	for _, v := range command.VerbsOfKind(kind) {
		prefix := cmdPrefixStyle.Render("  > ")
		name := cmdNameStyle.Render(fmt.Sprintf("%-12s", v.Name))
		argsFormatted := formatHelpArgs(v.Args, 24)
		desc := cmdDescStyle.Render(v.Summary)
		b.WriteString(fmt.Sprintf("%s%s %s %s\n", prefix, name, argsFormatted, desc))
	}
}

func writeHelpCommands(b *strings.Builder, cmds []struct {
	cmd  string
	arg  string
	desc string
}) {
	for _, c := range cmds {
		prefix := cmdPrefixStyle.Render("  > ")
		name := cmdNameStyle.Render(fmt.Sprintf("%-12s", c.cmd))
		argsFormatted := formatHelpArgs(c.arg, 24)
		desc := cmdDescStyle.Render(c.desc)

		b.WriteString(fmt.Sprintf("%s%s %s %s\n", prefix, name, argsFormatted, desc))
	}
}

func formatHelpArgs(raw string, width int) string {
	if raw == "" {
		return strings.Repeat(" ", width)
	}

	var sb strings.Builder
	visibleLen := 0

	first := true
	for token := range strings.SplitSeq(raw, " ") {
		if !first {
			sb.WriteString(" ")
			visibleLen++
		}
		first = false
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

func (s *replSession) prompt() string {
	if s.activeQueue != "" {
		return promptStyle.Render(fmt.Sprintf("mikyu (%s)> ", s.activeQueue))
	}
	return promptStyle.Render("mikyu> ")
}
