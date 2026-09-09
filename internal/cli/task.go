package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"charm.land/lipgloss/v2"
	"github.com/dmunish/mikyu"
	"github.com/dmunish/mikyu/internal/command"
)

func cliFatal(err error) {
	fmt.Fprintf(os.Stderr, "error: %v\n", err)
	os.Exit(1)
}

func cliBadUsage(msg string) {
	fmt.Fprintln(os.Stderr, msg)
	os.Exit(2)
}

func shortTaskID(id string) string {
	if len(id) <= 12 {
		return id
	}
	return id[:12]
}

func renderCLITask(task mikyu.Task) string {
	lease := "pending"
	if !task.Visibility.IsZero() {
		d := time.Until(task.Visibility)
		if d <= 0 {
			lease = "expired"
		} else {
			lease = fmt.Sprintf("%s remaining", d.Round(time.Second))
		}
	}

	payload := command.FormatSingleLinePayload(task.Payload, 72)

	content := fmt.Sprintf(
		"ID:       %s\nAttempts: %d / %d\nLease:    %s\nPayload:  %s",
		task.ID,
		task.Attempts,
		task.MaxAttempts,
		lease,
		payload,
	)

	return lipgloss.NewStyle().
		Border(lipgloss.NormalBorder()).
		Padding(0, 1).
		Render(content)
}

func printCLITask(task mikyu.Task) {
	if isTerminal(os.Stdout) {
		fmt.Println(renderCLITask(task))
		return
	}

	out := struct {
		ID          string    `json:"id"`
		Attempts    uint32    `json:"attempts"`
		MaxAttempts uint32    `json:"max_attempts"`
		Visibility  time.Time `json:"visibility"`
		ScheduledAt time.Time `json:"scheduled_at"`
		DeliveryID  string    `json:"delivery_id"`
		Payload     string    `json:"payload"`
	}{
		ID:          task.ID,
		Attempts:    task.Attempts,
		MaxAttempts: task.MaxAttempts,
		Visibility:  task.Visibility,
		ScheduledAt: task.ScheduledAt,
		DeliveryID:  task.DeliveryID,
		Payload:     string(task.Payload),
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(out)
}

func printCLITasks(tasks []mikyu.Task) {
	if isTerminal(os.Stdout) {
		if len(tasks) == 0 {
			fmt.Println("no dead letters")
			return
		}

		for _, task := range tasks {
			fmt.Println(renderCLITask(task))
		}
		return
	}

	type jsonTask struct {
		ID          string    `json:"id"`
		Attempts    uint32    `json:"attempts"`
		MaxAttempts uint32    `json:"max_attempts"`
		ScheduledAt time.Time `json:"scheduled_at"`
		Payload     string    `json:"payload"`
	}

	out := make([]jsonTask, 0, len(tasks))
	for _, task := range tasks {
		out = append(out, jsonTask{
			ID:          task.ID,
			Attempts:    task.Attempts,
			MaxAttempts: task.MaxAttempts,
			ScheduledAt: task.ScheduledAt,
			Payload:     string(task.Payload),
		})
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(out)
}

func handlePublish(ctx context.Context, client mikyu.Client, args []string) {
	var err error

	visibility, args, err := command.ExtractDuration(args, "--visibility", 0)
	if err != nil {
		cliFatal(err)
	}

	maxAttempts, args, err := command.ExtractUint32(args, "--max-attempts", 0)
	if err != nil {
		cliFatal(err)
	}

	at, args, err := command.ExtractTime(args, "--at", time.Time{})
	if err != nil {
		cliFatal(err)
	}

	if len(args) < 2 {
		cliBadUsage("usage: mikyu publish <queue> <payload|@file|-> [--visibility 30s] [--max-attempts 5] [--at RFC3339]")
	}

	queue := args[0]
	input := args[1]

	task, n, err := command.Publish(ctx, client, queue, input, command.PublishOptions{
		VisibilityTimeout: visibility,
		MaxAttempts:       maxAttempts,
		ScheduleAt:        at,
	})
	if err != nil {
		cliFatal(err)
	}

	fmt.Printf("published task %s to %q (%d bytes)\n", shortTaskID(task.ID), queue, n)
}

func handlePop(ctx context.Context, client mikyu.Client, args []string) {
	timeout, args, err := command.ExtractDuration(args, "--timeout", 30*time.Second)
	if err != nil {
		cliFatal(err)
	}

	if len(args) != 1 {
		cliBadUsage("usage: mikyu pop <queue> [--timeout 30s]")
	}

	queue := args[0]

	task, err := command.Pop(ctx, client, queue, timeout)
	if err != nil {
		if errors.Is(err, mikyu.ErrTaskNotAvailable) {
			fmt.Println("no task available")
			return
		}
		cliFatal(err)
	}

	printCLITask(task)
}

func handleAck(ctx context.Context, client mikyu.Client, args []string) {
	if len(args) != 2 {
		cliBadUsage("usage: mikyu ack <queue> <task_id>")
	}

	queue := args[0]
	taskID := args[1]

	if err := command.TaskAck(ctx, client, queue, taskID); err != nil {
		cliFatal(err)
	}

	fmt.Printf("acknowledged task %s\n", shortTaskID(taskID))
}

func handleNack(ctx context.Context, client mikyu.Client, args []string) {
	retryAfter, explicit, args, err := command.ExtractOptionalDuration(args, "--retry-after")
	if err != nil {
		cliFatal(err)
	}

	if len(args) != 2 {
		cliBadUsage("usage: mikyu nack <queue> <task_id> [--retry-after 5s]")
	}

	queue := args[0]
	taskID := args[1]

	if err := command.TaskNack(ctx, client, queue, taskID, retryAfter, explicit); err != nil {
		cliFatal(err)
	}

	fmt.Printf("released task %s\n", shortTaskID(taskID))
}

func handleExtend(ctx context.Context, client mikyu.Client, args []string) {
	timeout, explicit, args, err := command.ExtractOptionalDuration(args, "--timeout")
	if err != nil {
		cliFatal(err)
	}

	if !explicit {
		if len(args) != 3 {
			cliBadUsage("usage: mikyu extend <queue> <task_id> <timeout>")
		}

		timeout, err = time.ParseDuration(args[2])
		if err != nil {
			cliFatal(fmt.Errorf("invalid timeout: %w", err))
		}

		args = args[:2]
	}

	if len(args) != 2 {
		cliBadUsage("usage: mikyu extend <queue> <task_id> <timeout>")
	}

	queue := args[0]
	taskID := args[1]

	if err := command.TaskExtend(ctx, client, queue, taskID, timeout); err != nil {
		cliFatal(err)
	}

	fmt.Printf("extended task %s\n", shortTaskID(taskID))
}

func handleDLQ(ctx context.Context, client mikyu.Client, args []string) {
	if len(args) != 1 {
		cliBadUsage("usage: mikyu dlq <queue>")
	}

	tasks, err := command.TaskDeadLetters(ctx, client, args[0])
	if err != nil {
		cliFatal(err)
	}

	printCLITasks(tasks)
}

func handleDLQAck(ctx context.Context, client mikyu.Client, args []string) {
	if len(args) != 2 {
		cliBadUsage("usage: mikyu dlq-ack <queue> <task_id>")
	}

	queue := args[0]
	taskID := args[1]

	if err := command.TaskAckDeadLetter(ctx, client, queue, taskID); err != nil {
		cliFatal(err)
	}

	fmt.Printf("removed dead-letter task %s\n", shortTaskID(taskID))
}
