package cli

import (
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"charm.land/lipgloss/v2"
	"github.com/dmunish/mikyu"
	"github.com/dmunish/mikyu/internal/ui/command"
)

func shortTaskID(id string) string {
	if len(id) <= 12 {
		return id
	}
	return id[:12]
}

// isWriterTerminal reports whether w is the process's own terminal, which is
// what decides styled output versus machine-readable records.
func isWriterTerminal(w io.Writer) bool {
	f, ok := w.(*os.File)
	return ok && isTerminal(f)
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

// emitTask writes one task as a card on a terminal and as a JSON record
// otherwise, so `dequeue --json` feeds the ack/nack/extend verbs.
func (in *invocation) emitTask(task mikyu.Task, asJSON bool) error {
	if asJSON || !isWriterTerminal(in.app.Stdout) {
		data, err := command.FormatTaskRecord(task)
		if err != nil {
			return err
		}
		fmt.Fprintln(in.app.Stdout, string(data))
		return nil
	}
	fmt.Fprintln(in.app.Stdout, renderCLITask(task))
	return nil
}

func (in *invocation) emitTasks(tasks []mikyu.Task, asJSON bool) error {
	if asJSON || !isWriterTerminal(in.app.Stdout) {
		data, err := command.FormatTaskRecords(tasks)
		if err != nil {
			return err
		}
		fmt.Fprintln(in.app.Stdout, string(data))
		return nil
	}
	if len(tasks) == 0 {
		fmt.Fprintln(in.app.Stdout, "no dead letters")
		return nil
	}
	for _, task := range tasks {
		fmt.Fprintln(in.app.Stdout, renderCLITask(task))
	}
	return nil
}

func (in *invocation) handleEnqueue(args []string) error {
	var opts command.EnqueueOptions
	fs := command.EnqueueFlagSet("enqueue", &opts)

	rest, err := command.ParseAnywhere(fs, args)
	if err != nil {
		return in.app.usage("enqueue: %v", err)
	}
	if len(rest) != 2 {
		return in.app.usage("usage: mikyu enqueue [flags] <queue> <payload|@file|->")
	}

	task, n, err := command.Enqueue(in.ctx, in.client, rest[0], rest[1], opts)
	if err != nil {
		return err
	}
	fmt.Fprintf(in.app.Stdout, "enqueued task %s to %q (%d bytes)\n", shortTaskID(task.ID), rest[0], n)
	return nil
}

func (in *invocation) handleDequeue(args []string) error {
	var timeout time.Duration
	var asJSON bool
	fs := newFlagSet("dequeue")
	fs.DurationVar(&timeout, "timeout", 30*time.Second, "how long to wait for a due task")
	fs.BoolVar(&asJSON, "json", false, "emit the task record as JSON")

	rest, err := command.ParseAnywhere(fs, args)
	if err != nil {
		return in.app.usage("dequeue: %v", err)
	}
	if len(rest) != 1 {
		return in.app.usage("usage: mikyu dequeue <queue> [--timeout 30s] [--json]")
	}

	task, err := command.Dequeue(in.ctx, in.client, rest[0], timeout)
	if err != nil {
		if errors.Is(err, mikyu.ErrTaskNotAvailable) {
			fmt.Fprintln(in.app.Stdout, "no task available")
			return nil
		}
		return err
	}
	return in.emitTask(task, asJSON)
}

func (in *invocation) handleAck(args []string) error {
	if len(args) != 2 {
		return in.app.usage("usage: mikyu ack <queue> <task|json|id>")
	}
	ref, id := command.ParseTaskRef(args[1])
	if err := command.TaskAck(in.ctx, in.client, args[0], ref); err != nil {
		return err
	}
	fmt.Fprintf(in.app.Stdout, "acknowledged task %s\n", shortTaskID(id))
	return nil
}

func (in *invocation) handleNack(args []string) error {
	var retryAfter time.Duration
	var explicit bool
	fs := command.NackFlagSet("nack", &retryAfter, &explicit)

	rest, err := command.ParseAnywhere(fs, args)
	if err != nil {
		return in.app.usage("nack: %v", err)
	}
	if len(rest) != 2 {
		return in.app.usage("usage: mikyu nack <queue> <task|json|id> [--retry-after 5s]")
	}

	ref, id := command.ParseTaskRef(rest[1])
	if err := command.TaskNack(in.ctx, in.client, rest[0], ref, retryAfter, explicit); err != nil {
		return err
	}
	fmt.Fprintf(in.app.Stdout, "released task %s\n", shortTaskID(id))
	return nil
}

func (in *invocation) handleExtend(args []string) error {
	if len(args) != 3 {
		return in.app.usage("usage: mikyu extend <queue> <task|json|id> <timeout>")
	}
	timeout, err := time.ParseDuration(args[2])
	if err != nil {
		return in.app.usage("invalid timeout: %v", err)
	}
	ref, id := command.ParseTaskRef(args[1])
	if err := command.TaskExtend(in.ctx, in.client, args[0], ref, timeout); err != nil {
		return err
	}
	fmt.Fprintf(in.app.Stdout, "extended task %s\n", shortTaskID(id))
	return nil
}

func (in *invocation) handleDLQ(args []string) error {
	var asJSON bool
	fs := newFlagSet("dlq")
	fs.BoolVar(&asJSON, "json", false, "emit the dead-letter records as JSON")

	rest, err := command.ParseAnywhere(fs, args)
	if err != nil {
		return in.app.usage("dlq: %v", err)
	}
	if len(rest) != 1 {
		return in.app.usage("usage: mikyu dlq <queue> [--json]")
	}

	tasks, err := command.TaskDeadLetters(in.ctx, in.client, rest[0])
	if err != nil {
		return err
	}
	return in.emitTasks(tasks, asJSON)
}

func (in *invocation) handleDLQAck(args []string) error {
	if len(args) != 2 {
		return in.app.usage("usage: mikyu dlq-ack <queue> <task|json|id>")
	}
	ref, id := command.ParseTaskRef(args[1])
	if err := command.TaskAckDeadLetter(in.ctx, in.client, args[0], ref); err != nil {
		return err
	}
	fmt.Fprintf(in.app.Stdout, "removed dead-letter task %s\n", shortTaskID(id))
	return nil
}

func (in *invocation) handleArchive(args []string) error {
	var queueName string
	fs := newFlagSet("archive")
	fs.StringVar(&queueName, "queue", "", "queue name")
	rest, err := command.ParseAnywhere(fs, args)
	if err != nil {
		return in.app.usage("archive: %v", err)
	}
	if len(rest) != 1 || queueName == "" {
		return in.app.usage("usage: mikyu archive <task|json|id> --queue <name>")
	}
	ref, id := command.ParseTaskRef(rest[0])
	if err := command.TaskArchive(in.ctx, in.client, queueName, ref); err != nil {
		return err
	}
	fmt.Fprintf(in.app.Stdout, "archived task %s\n", shortTaskID(id))
	return nil
}
