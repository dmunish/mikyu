package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"

	"charm.land/lipgloss/v2"
	"github.com/dmunish/mikyu"
	"github.com/dmunish/mikyu/internal/protocol"
)

var (
	promptStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("205")).Bold(true)
	infoStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("86"))
	errStyle    = lipgloss.NewStyle().Foreground(lipgloss.Color("196")).Bold(true)
	dimStyle    = lipgloss.NewStyle().Foreground(lipgloss.Color("240"))
)

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

	// Clear screen and render welcome banner
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
			fmt.Println(infoStyle.Render(fmt.Sprintf("✓ Queue %q created.", name)))

		case "delete":
			if len(args) < 2 {
				fmt.Println(errStyle.Render("Usage: delete <queue_name>"))
				continue
			}
			name := args[1]
			if err := client.DeleteQueue(ctx, name); err != nil {
				fmt.Println(errStyle.Render("Error: " + err.Error()))
				continue
			}
			fmt.Println(infoStyle.Render(fmt.Sprintf("✓ Queue %q deleted.", name)))

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
			fmt.Println(infoStyle.Render(fmt.Sprintf("✓ Produced %d bytes to %q at offset %d", len(payload), name, off)))

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
			fmt.Println(dimStyle.Render(fmt.Sprintf("Tailing %q from offset %d (Press Ctrl+C to stop stream)...", name, startOffset)))
			for msg, err := range q.Tail(ctx, startOffset) {
				if err != nil {
					fmt.Println(errStyle.Render("Stream error: " + err.Error()))
					break
				}
				fmt.Printf("[%d] ID: %s | Time: %s | %s\n",
					msg.Offset, msg.ID, msg.Timestamp.Format("15:04:05.000"), string(msg.Payload))
			}

		case "stats":
			st, err := client.Stats(ctx)
			if err != nil {
				fmt.Println(errStyle.Render("Error: " + err.Error()))
				continue
			}
			fmt.Printf("Active Queues: %d | Total Storage: %d Bytes\n", st.ActiveQueues, st.TotalBytes)

		case "shutdown":
			fmt.Println(infoStyle.Render("Sending shutdown command to daemon..."))
			_ = sendShutdownSignal(target)
			fmt.Println("Daemon stopped. Exiting shell.")
			return

		default:
			fmt.Println(errStyle.Render(fmt.Sprintf("Unknown command %q. Type 'help' for options.", cmd)))
		}
	}
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

func resolvePayload(input string) ([]byte, error) {
	if input == "-" {
		return io.ReadAll(os.Stdin)
	}
	if strings.HasPrefix(input, "@") {
		return os.ReadFile(strings.TrimPrefix(input, "@"))
	}
	return []byte(input), nil
}

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
