package command

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/dmunish/mikyu"
)

type EnqueueOptions struct {
	Visibility  time.Duration
	MaxAttempts uint32
	ScheduledAt time.Time
}

func Enqueue(ctx context.Context, client mikyu.Client, queue string, input string, opts EnqueueOptions) (mikyu.Task, int, error) {
	payload, err := ResolvePayload(input)
	if err != nil {
		return mikyu.Task{}, 0, err
	}

	tq, err := client.TaskQueue(queue)
	if err != nil {
		return mikyu.Task{}, 0, err
	}

	var taskOpts []mikyu.EnqueueOption

	if opts.Visibility > 0 {
		taskOpts = append(taskOpts, mikyu.WithVisibility(opts.Visibility))
	}

	if opts.MaxAttempts > 0 {
		taskOpts = append(taskOpts, mikyu.WithMaxAttempts(opts.MaxAttempts))
	}

	if !opts.ScheduledAt.IsZero() {
		taskOpts = append(taskOpts, mikyu.WithScheduledAt(opts.ScheduledAt))
	}

	task, err := tq.Enqueue(ctx, payload, taskOpts...)
	if err != nil {
		return mikyu.Task{}, 0, err
	}

	return task, len(payload), nil
}

func Dequeue(ctx context.Context, client mikyu.Client, queue string, timeout time.Duration) (mikyu.Task, error) {
	tq, err := client.TaskQueue(queue)
	if err != nil {
		return mikyu.Task{}, err
	}

	// timeout < 0 means wait indefinitely.
	if timeout < 0 {
		return tq.Dequeue(ctx)
	}

	// timeout == 0 means non-blocking-ish.
	if timeout == 0 {
		timeout = 25 * time.Millisecond
	}

	deqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	task, err := tq.Dequeue(deqCtx)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return mikyu.Task{}, mikyu.ErrTaskNotAvailable
		}
		return mikyu.Task{}, err
	}

	return task, nil
}

func TaskAck(ctx context.Context, client mikyu.Client, queue string, ref mikyu.TaskRef) error {
	tq, err := client.TaskQueue(queue)
	if err != nil {
		return err
	}

	return tq.Ack(ctx, ref)
}

func TaskNack(ctx context.Context, client mikyu.Client, queue string, ref mikyu.TaskRef, retryAfter time.Duration, explicitRetry bool) error {
	tq, err := client.TaskQueue(queue)
	if err != nil {
		return err
	}

	if explicitRetry {
		return tq.Nack(ctx, ref, mikyu.WithRetryAfter(retryAfter))
	}

	return tq.Nack(ctx, ref)
}

func TaskExtend(ctx context.Context, client mikyu.Client, queue string, ref mikyu.TaskRef, timeout time.Duration) error {
	tq, err := client.TaskQueue(queue)
	if err != nil {
		return err
	}

	return tq.Extend(ctx, ref, timeout)
}

func TaskDeadLetters(ctx context.Context, client mikyu.Client, queue string) ([]mikyu.Task, error) {
	tq, err := client.TaskQueue(queue)
	if err != nil {
		return nil, err
	}

	return tq.DeadLetters(ctx)
}

func TaskAckDeadLetter(ctx context.Context, client mikyu.Client, queue string, ref mikyu.TaskRef) error {
	tq, err := client.TaskQueue(queue)
	if err != nil {
		return err
	}

	return tq.AckDeadLetter(ctx, ref)
}

// TaskRecord is the JSON shape a task takes on the CLI, and the shape the task verbs accept back: `dequeue --json` feeds straight into ack, nack, extend, and dlq-ack, carrying the delivery token that enables the strict check.
type TaskRecord struct {
	ID          string    `json:"id"`
	Attempts    uint32    `json:"attempts"`
	MaxAttempts uint32    `json:"max_attempts"`
	Visibility  time.Time `json:"visibility"`
	ScheduledAt time.Time `json:"scheduled_at"`
	DeliveryID  string    `json:"delivery_id"`
	Offset      uint64    `json:"offset"`
	Payload     string    `json:"payload"`
}

// Record renders a task as its CLI record shape.
func Record(t mikyu.Task) TaskRecord {
	return TaskRecord{
		ID:          t.ID,
		Attempts:    t.Attempts,
		MaxAttempts: t.MaxAttempts,
		Visibility:  t.Visibility,
		ScheduledAt: t.ScheduledAt,
		DeliveryID:  t.DeliveryID,
		Offset:      t.Offset,
		Payload:     string(t.Payload),
	}
}

// ParseTaskRef accepts a JSON task record or a bare task ID, returning the ref and the task ID for display.
func ParseTaskRef(arg string) (mikyu.TaskRef, string) {
	var rec TaskRecord
	if err := json.Unmarshal([]byte(arg), &rec); err == nil && rec.ID != "" {
		return mikyu.Task{
			ID:          rec.ID,
			Payload:     []byte(rec.Payload),
			Attempts:    rec.Attempts,
			MaxAttempts: rec.MaxAttempts,
			Visibility:  rec.Visibility,
			ScheduledAt: rec.ScheduledAt,
			DeliveryID:  rec.DeliveryID,
			Offset:      rec.Offset,
		}, rec.ID
	}
	return mikyu.TaskID(arg), arg
}

// FormatTaskRecord renders one task as indented JSON.
func FormatTaskRecord(t mikyu.Task) ([]byte, error) {
	return json.MarshalIndent(Record(t), "", "  ")
}

// FormatTaskRecords renders a task list as indented JSON.
func FormatTaskRecords(tasks []mikyu.Task) ([]byte, error) {
	records := make([]TaskRecord, 0, len(tasks))
	for _, t := range tasks {
		records = append(records, Record(t))
	}
	return json.MarshalIndent(records, "", "  ")
}
