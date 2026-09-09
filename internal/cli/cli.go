package cli

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/dmunish/mikyu"
	"github.com/dmunish/mikyu/internal/command"
	"github.com/dmunish/mikyu/internal/daemon"
)

func RunCLI(target string, args []string) {
	if err := daemon.EnsureDaemonRunning(target); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	client, err := mikyu.Dial(target)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer client.Close()

	if len(args) == 0 {
		printUsage()
		os.Exit(2)
	}

	ctx := context.Background()
	switch args[0] {
	case "list":
		handleList(ctx, client)
	case "create":
		handleCreate(ctx, client, args[1:])
	case "delete":
		handleDelete(ctx, client, args[1:])
	case "produce":
		handleProduce(ctx, client, args[1:])
	case "read":
		handleRead(ctx, client, args[1:])
	case "tail":
		handleTail(ctx, client, args[1:])
	case "stats":
		handleStats(ctx, client)
	case "truncate":
		handleTruncate(ctx, client, args[1:])
	case "shutdown":
		handleShutdown(target, args[1:])

	case "publish":
		handlePublish(ctx, client, args[1:])
	case "pop":
		handlePop(ctx, client, args[1:])
	case "ack":
		handleAck(ctx, client, args[1:])
	case "nack":
		handleNack(ctx, client, args[1:])
	case "extend":
		handleExtend(ctx, client, args[1:])
	case "dlq":
		handleDLQ(ctx, client, args[1:])
	case "dlq-ack":
		handleDLQAck(ctx, client, args[1:])

	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", args[0])
		printUsage()
		os.Exit(2)
	}
}

func printUsage() {
	fmt.Fprintf(os.Stderr, "Usage: mikyu <command> [args]\n\n")

	fmt.Fprintf(os.Stderr, "Log Commands:\n")
	fmt.Fprintf(os.Stderr, "  list\n")
	fmt.Fprintf(os.Stderr, "  create <queue>\n")
	fmt.Fprintf(os.Stderr, "  delete <queue>\n")
	fmt.Fprintf(os.Stderr, "  produce <queue> <payload|@file|->\n")
	fmt.Fprintf(os.Stderr, "  read <queue> <offset>\n")
	fmt.Fprintf(os.Stderr, "  tail <queue> [offset]\n")
	fmt.Fprintf(os.Stderr, "  stats\n")
	fmt.Fprintf(os.Stderr, "  truncate <queue> [offset]\n")
	fmt.Fprintf(os.Stderr, "\n")

	fmt.Fprintf(os.Stderr, "Task Commands:\n")
	fmt.Fprintf(os.Stderr, "  publish <queue> <payload|@file|-> [--visibility 30s] [--max-attempts 5]\n")
	fmt.Fprintf(os.Stderr, "  pop <queue> [--timeout 30s]\n")
	fmt.Fprintf(os.Stderr, "  ack <queue> <task_id>\n")
	fmt.Fprintf(os.Stderr, "  nack <queue> <task_id> [--retry-after 5s]\n")
	fmt.Fprintf(os.Stderr, "  extend <queue> <task_id> <timeout>\n")
	fmt.Fprintf(os.Stderr, "  dlq <queue>\n")
	fmt.Fprintf(os.Stderr, "  dlq-ack <queue> <task_id>\n")
	fmt.Fprintf(os.Stderr, "\n")

	fmt.Fprintf(os.Stderr, "Admin Commands:\n")
	fmt.Fprintf(os.Stderr, "  shutdown\n")
}

func handleList(ctx context.Context, client mikyu.Client) {
	queues, err := command.List(ctx, client)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	for _, q := range queues {
		fmt.Println(q)
	}
}

func handleCreate(ctx context.Context, client mikyu.Client, args []string) {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "usage: mikyu create <queue>")
		os.Exit(2)
	}
	if err := command.Create(ctx, client, args[0]); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("created queue %q\n", args[0])
}

func handleDelete(ctx context.Context, client mikyu.Client, args []string) {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "usage: mikyu delete <queue> [-y]")
		os.Exit(2)
	}
	name := args[0]
	force := false
	for _, a := range args[1:] {
		if a == "-y" || a == "--yes" || a == "-f" || a == "--force" {
			force = true
		}
	}

	var confirmFn func() (bool, error)
	if !force {
		confirmFn = func() (bool, error) {
			return confirmCLI(fmt.Sprintf("delete queue %q? [y/N]: ", name), args)
		}
	}

	executed, err := command.Delete(ctx, client, name, confirmFn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	if !executed {
		fmt.Println("aborted")
		return
	}
	fmt.Printf("deleted queue %q\n", name)
}

func handleProduce(ctx context.Context, client mikyu.Client, args []string) {
	if len(args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: mikyu produce <queue> <payload|@file|->")
		os.Exit(2)
	}
	offset, _, err := command.Produce(ctx, client, args[0], args[1])
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("produced at offset %d\n", offset)
}

func handleRead(ctx context.Context, client mikyu.Client, args []string) {
	if len(args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: mikyu read <queue> <offset>")
		os.Exit(2)
	}
	offset, err := strconv.ParseUint(args[1], 10, 64)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid offset: %v\n", err)
		os.Exit(2)
	}
	msg, err := command.Read(ctx, client, args[0], offset)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("offset=%d id=%s ts=%s payload=%s\n", msg.Offset, msg.ID, msg.Timestamp.Format(time.RFC3339Nano), string(msg.Payload))
}

func handleTail(ctx context.Context, client mikyu.Client, args []string) {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "usage: mikyu tail <queue> [start-offset]")
		os.Exit(2)
	}
	var start uint64 = 0
	if len(args) > 1 {
		var err error
		start, err = strconv.ParseUint(args[1], 10, 64)
		if err != nil {
			fmt.Fprintf(os.Stderr, "invalid offset: %v\n", err)
			os.Exit(2)
		}
	}

	err := command.Tail(ctx, client, args[0], start, func(msg mikyu.Message) error {
		prefix := fmt.Sprintf("offset=%d id=%s ts=%s payload=", msg.Offset, msg.ID, msg.Timestamp.Format(time.RFC3339Nano))
		termWidth := command.GetTerminalWidth()
		avail := -1
		if termWidth > 0 {
			avail = termWidth - len(prefix)
		}
		payloadStr := command.FormatSingleLinePayload(msg.Payload, avail)
		fmt.Printf("%s%s\n", prefix, payloadStr)
		return nil
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func handleTruncate(ctx context.Context, client mikyu.Client, args []string) {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "usage: mikyu truncate <queue> [min-offset] [-y]")
		os.Exit(2)
	}
	name := args[0]
	var minOffset uint64 = 0
	force := false

	for _, a := range args[1:] {
		if a == "-y" || a == "--yes" || a == "-f" || a == "--force" {
			force = true
			continue
		}
		if off, err := strconv.ParseUint(a, 10, 64); err == nil {
			minOffset = off
		}
	}

	var confirmFn func() (bool, error)
	if !force {
		prompt := fmt.Sprintf("truncate physical log segments for queue %q? [y/N]: ", name)
		if minOffset > 0 {
			prompt = fmt.Sprintf("truncate physical log segments before offset %d for queue %q? [y/N]: ", minOffset, name)
		}
		confirmFn = func() (bool, error) {
			return confirmCLI(prompt, args)
		}
	}

	reclaimed, executed, err := command.Truncate(ctx, client, name, minOffset, confirmFn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	if !executed {
		fmt.Println("aborted")
		return
	}
	fmt.Printf("truncated queue %q (%d bytes reclaimed)\n", name, reclaimed)
}

func handleStats(ctx context.Context, client mikyu.Client) {
	stats, err := command.Stats(ctx, client)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("active queues: %d\n", stats.ActiveQueues)
	fmt.Printf("total bytes: %d\n", stats.TotalBytes)
	for name, qs := range stats.Queues {
		fmt.Printf("queue %s: messages=%d, segments=%d, size=%d, last_offset=%d, min_offset=%d\n", name, qs.MessageCount, qs.SegmentCount, qs.SizeBytes, qs.LastOffset, qs.MinOffset)
	}
}

func handleShutdown(target string, args []string) {
	force := false
	for _, a := range args {
		if a == "-y" || a == "--yes" || a == "-f" || a == "--force" {
			force = true
		}
	}

	var confirmFn func() (bool, error)
	if !force {
		confirmFn = func() (bool, error) {
			return confirmCLI("shutdown daemon? [y/N]: ", args)
		}
	}

	executed, err := command.Shutdown(target, confirmFn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	if !executed {
		fmt.Println("aborted")
		return
	}
	fmt.Println("shutdown signal sent")
}

func confirmCLI(prompt string, args []string) (bool, error) {
	for _, a := range args {
		if a == "-y" || a == "--yes" || a == "-f" || a == "--force" {
			return true, nil
		}
	}
	if !isTerminal(os.Stdin) {
		return false, fmt.Errorf("cannot prompt in non-interactive mode; use -y/--yes")
	}
	fmt.Print(prompt)
	var answer string
	if _, err := fmt.Scanln(&answer); err != nil {
		return false, err
	}
	answer = strings.ToLower(strings.TrimSpace(answer))
	return answer == "y" || answer == "yes", nil
}

func isTerminal(f *os.File) bool {
	fi, err := f.Stat()
	if err != nil {
		return false
	}
	return fi.Mode()&os.ModeCharDevice != 0
}
