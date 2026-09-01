package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/dmunish/mikyu"
)

func RunCLI(target string, args []string) {
	if len(args) == 0 {
		printUsage()
		return
	}

	if err := EnsureDaemonRunning(target); err != nil {
		log.Fatalf("Daemon error: %v", err)
	}

	client, err := mikyu.Dial(target)
	if err != nil {
		log.Fatalf("Failed to connect to %s: %v", target, err)
	}
	defer client.Close()

	cmd := args[0]
	ctx := context.Background()

	switch cmd {
	case "list":
		queues, err := client.ListQueues(ctx)
		if err != nil {
			log.Fatalf("Error listing queues: %v", err)
		}
		if len(queues) == 0 {
			fmt.Println("No queues found.")
			return
		}
		fmt.Println("Queues:")
		for _, q := range queues {
			fmt.Printf(" - %s\n", q)
		}

	case "create":
		if len(args) < 2 {
			log.Fatal("Usage: mikyu create <queue_name>")
		}
		name := args[1]
		if _, err := client.CreateQueue(ctx, name); err != nil {
			log.Fatalf("Error creating queue %q: %v", name, err)
		}
		fmt.Printf("Queue %q created successfully.\n", name)

	case "delete":
		if len(args) < 2 {
			log.Fatal("Usage: mikyu delete <queue_name>")
		}
		name := args[1]
		if err := client.DeleteQueue(ctx, name); err != nil {
			log.Fatalf("Error deleting queue %q: %v", name, err)
		}
		fmt.Printf("Queue %q deleted successfully.\n", name)

	case "produce":
		if len(args) < 3 {
			log.Fatal("Usage: mikyu produce <queue_name> <payload | @file | ->")
		}
		name, input := args[1], args[2]
		payload, err := resolvePayload(input)
		if err != nil {
			log.Fatalf("Failed to resolve payload input: %v", err)
		}

		q, err := client.Queue(name)
		if err != nil {
			log.Fatalf("Error opening queue %q: %v", name, err)
		}
		offset, err := q.Produce(ctx, payload)
		if err != nil {
			log.Fatalf("Error producing message: %v", err)
		}
		fmt.Printf("Produced %d bytes to %q at offset %d\n", len(payload), name, offset)

	case "read":
		if len(args) < 3 {
			log.Fatal("Usage: mikyu read <queue_name> <offset>")
		}
		name := args[1]
		offset, err := strconv.ParseUint(args[2], 10, 64)
		if err != nil {
			log.Fatalf("Invalid offset %q: %v", args[2], err)
		}
		q, err := client.Queue(name)
		if err != nil {
			log.Fatalf("Error opening queue %q: %v", name, err)
		}
		msg, err := q.Read(ctx, offset)
		if err != nil {
			log.Fatalf("Error reading offset %d: %v", offset, err)
		}
		fmt.Printf("[%d] ID: %s | Time: %s | Payload: %s\n",
			msg.Offset, msg.ID, msg.Timestamp.Format("15:04:05.000"), string(msg.Payload))

	case "tail":
		if len(args) < 2 {
			log.Fatal("Usage: mikyu tail <queue_name> [start_offset]")
		}
		name := args[1]
		var startOffset uint64 = 0
		if len(args) >= 3 {
			startOffset, _ = strconv.ParseUint(args[2], 10, 64)
		}
		q, err := client.Queue(name)
		if err != nil {
			log.Fatalf("Error opening queue %q: %v", name, err)
		}

		fmt.Printf("Tailing queue %q starting from offset %d...\n", name, startOffset)
		for msg, err := range q.Tail(ctx, startOffset) {
			if err != nil {
				log.Fatalf("Tail error: %v", err)
			}
			fmt.Printf("[%d] ID: %s | Time: %s | %s\n",
				msg.Offset, msg.ID, msg.Timestamp.Format("15:04:05.000"), string(msg.Payload))
		}

	case "stats":
		st, err := client.Stats(ctx)
		if err != nil {
			log.Fatalf("Error fetching stats: %v", err)
		}
		fmt.Printf("Active Queues: %d\nTotal Storage: %d Bytes\n", st.ActiveQueues, st.TotalBytes)

	case "shutdown":
		if err := sendShutdownSignal(target); err != nil {
			log.Fatalf("Failed to stop daemon: %v", err)
		}
		fmt.Println("Mikyu daemon shutdown successfully.")

	default:
		printUsage()
		os.Exit(1)
	}
}
