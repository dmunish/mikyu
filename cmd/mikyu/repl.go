package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"charm.land/huh/v2"
	"charm.land/lipgloss/v2"
	"github.com/dmunish/mikyu"
	"github.com/dmunish/mikyu/internal/protocol"
)

var (
	accentColor = lipgloss.Color("#FFB200")

	promptStyle = lipgloss.NewStyle().
			Foreground(accentColor).
			Bold(true)

	infoStyle = lipgloss.NewStyle().
			Foreground(accentColor)

	errStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("196")).
			Bold(true)

	dimStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("240"))
)

// RunREPL starts the interactive REPL shell.
func RunREPL(target string) {
	if err := EnsureDaemonRunning(target); err != nil {
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

	scanner := bufio.NewScanner(os.Stdin)
	ctx := context.Background()

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

		args := parseREPLArgs(line)
		cmd := strings.ToLower(args[0])

		switch cmd {
		case "exit", "quit":
			fmt.Println("Goodbye!")
			return

		case "help":
			printREPLHelp()

		case "list":
			queues, err := client.ListQueues(ctx)
			if err != nil {
				fmt.Println(errStyle.Render("Error: " + err.Error()))
				continue
			}
			if len(queues) == 0 {
				fmt.Println(dimStyle.Render("No queues found."))
				continue
			}
			fmt.Println(infoStyle.Render("Queues:"))
			for _, q := range queues {
				fmt.Printf("  • %s\n", q)
			}

		case "create":
			if len(args) < 2 {
				fmt.Println(errStyle.Render("Usage: create <queue_name>"))
				continue
			}
			name := args[1]
			if _, err := client.CreateQueue(ctx, name); err != nil {
				fmt.Println(errStyle.Render("Error: " + err.Error()))
				continue
			}
			fmt.Println(infoStyle.Render(fmt.Sprintf("%s Queue %q created.", successBullet(), name)))

		case "delete":
			if len(args) < 2 {
				fmt.Println(errStyle.Render("Usage: delete <queue_name>"))
				continue
			}
			name := args[1]
			if !confirm(fmt.Sprintf("Are you sure you want to delete queue %q?", name)) {
				fmt.Println("Cancelled.")
				continue
			}
			if err := client.DeleteQueue(ctx, name); err != nil {
				fmt.Println(errStyle.Render("Error: " + err.Error()))
				continue
			}
			fmt.Println(infoStyle.Render(fmt.Sprintf("%s Queue %q deleted.", successBullet(), name)))

		case "produce":
			if len(args) < 3 {
				fmt.Println(errStyle.Render("Usage: produce <queue_name> <payload | @file>"))
				continue
			}
			name, input := args[1], args[2]
			payload, err := resolvePayload(input)
			if err != nil {
				fmt.Println(errStyle.Render("Error: " + err.Error()))
				continue
			}
			q, err := client.Queue(name)
			if err != nil {
				fmt.Println(errStyle.Render("Error: " + err.Error()))
				continue
			}
			off, err := q.Produce(ctx, payload)
			if err != nil {
				fmt.Println(errStyle.Render("Error: " + err.Error()))
				continue
			}
			fmt.Println(infoStyle.Render(fmt.Sprintf("+ Produced %d bytes to %q at offset %d", len(payload), name, off)))

		case "read":
			if len(args) < 3 {
				fmt.Println(errStyle.Render("Usage: read <queue_name> <offset>"))
				continue
			}
			name := args[1]
			off, err := strconv.ParseUint(args[2], 10, 64)
			if err != nil {
				fmt.Println(errStyle.Render("Invalid offset format"))
				continue
			}
			q, err := client.Queue(name)
			if err != nil {
				fmt.Println(errStyle.Render("Error: " + err.Error()))
				continue
			}
			msg, err := q.Read(ctx, off)
			if err != nil {
				fmt.Println(errStyle.Render("Error: " + err.Error()))
				continue
			}
			fmt.Printf("[%d] ID: %s | Time: %s | Payload: %s\n",
				msg.Offset, msg.ID, msg.Timestamp.Format("15:04:05.000"), string(msg.Payload))

		case "tail":
			if len(args) < 2 {
				fmt.Println(errStyle.Render("Usage: tail <queue_name> [start_offset]"))
				continue
			}
			name := args[1]
			var startOffset uint64 = 0
			if len(args) >= 3 {
				startOffset, _ = strconv.ParseUint(args[2], 10, 64)
			}
			q, err := client.Queue(name)
			if err != nil {
				fmt.Println(errStyle.Render("Error: " + err.Error()))
				continue
			}

			sigCtx, stopSig := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
			fmt.Println(dimStyle.Render(fmt.Sprintf("Tailing %q from offset %d (Press Ctrl+C to stop stream)...", name, startOffset)))
			for msg, err := range q.Tail(sigCtx, startOffset) {
				if err != nil {
					if sigCtx.Err() != nil {
						fmt.Println(infoStyle.Render("\nStream stopped."))
					} else {
						fmt.Println(errStyle.Render("Stream error: " + err.Error()))
					}
					break
				}
				fmt.Printf("[%d] ID: %s | Time: %s | %s\n",
					msg.Offset, msg.ID, msg.Timestamp.Format("15:04:05.000"), string(msg.Payload))
			}
			stopSig()

		case "stats":
			st, err := client.Stats(ctx)
			if err != nil {
				fmt.Println(errStyle.Render("Error: " + err.Error()))
				continue
			}
			fmt.Printf("Active Queues: %d | Total Storage: %d Bytes\n", st.ActiveQueues, st.TotalBytes)

		case "shutdown":
			if !confirm("Are you sure you want to shut down the daemon?") {
				fmt.Println("Cancelled.")
				continue
			}
			fmt.Println(infoStyle.Render("Sending shutdown command to daemon..."))
			_ = sendShutdownSignal(target)
			fmt.Println("Daemon stopped. Exiting shell.")
			return

		default:
			fmt.Println(errStyle.Render(fmt.Sprintf("Unknown command %q. Type 'help' for options.", cmd)))
		}
	}
}

// parseREPLArgs splits a REPL line into args, respecting double quotes.
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

// resolvePayload reads raw bytes from input string, @file, or stdin.
func resolvePayload(input string) ([]byte, error) {
	if input == "-" {
		return io.ReadAll(os.Stdin)
	}
	if strings.HasPrefix(input, "@") {
		return os.ReadFile(strings.TrimPrefix(input, "@"))
	}
	return []byte(input), nil
}

// confirm shows a themed Yes/No confirmation prompt, returns true if Yes.
func confirm(prompt string) bool {
	var confirm bool

	// Colored arrow + plain text title
	arrow := lipgloss.NewStyle().Foreground(lipgloss.Color("#EE427D")).Render(">")
	fullTitle := arrow + " " + prompt

	theme := huh.ThemeFunc(func(isDark bool) *huh.Styles {
		s := huh.ThemeCharm(isDark)

		// Remove the left border/line and any padding
		s.Focused.Base = lipgloss.NewStyle().Padding(0).Margin(0)
		s.Blurred.Base = lipgloss.NewStyle().Padding(0).Margin(0)

		// Title text: light grey like the normal REPL output
		s.Focused.Title = lipgloss.NewStyle().Foreground(lipgloss.Color("250")).Padding(0).Margin(0)
		s.Blurred.Title = lipgloss.NewStyle().Foreground(lipgloss.Color("250")).Padding(0).Margin(0)

		// Buttons: active = EE427D text, inactive = grey
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
				Value(&confirm).
				Inline(true).
				WithButtonAlignment(lipgloss.Left),
		).WithShowHelp(false),
	).WithTheme(theme).Run()

	if err != nil {
		return false
	}
	return confirm
}

func successBullet() string {
	return lipgloss.NewStyle().
		Foreground(lipgloss.Color("#EE427D")).
		Render("●")
}

// sendShutdownSignal sends OpShutdown frame to the daemon.
func sendShutdownSignal(target string) error {
	network, addr, err := protocol.ParseTarget(target)
	if err != nil {
		return err
	}
	conn, err := net.Dial(network, addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	return protocol.WriteFrame(conn, protocol.OpShutdown, nil)
}

// printREPLHelp prints the available REPL commands.
func printREPLHelp() {
	fmt.Print(`Available Commands:
  list                 List all active queues
  create <queue>       Create a new queue
  delete <queue>       Delete a queue and its stored records
  produce <q> <msg|@f> Produce text, file payload (@file.json), or stdin (-)
  read <queue> <off>   Read record at specific logical offset
  tail <queue> [off]   Stream live records continuously
  stats                Display engine storage metrics
  shutdown             Stop the background daemon process
  exit / quit          Exit the REPL shell
`)
}
