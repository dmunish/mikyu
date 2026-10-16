package command

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/dmunish/mikyu"
)

// EnqueueOptions carries the per-call values for enqueue.
type EnqueueOptions struct {
	Lease  time.Duration
	MaxAttempts uint32
	ScheduledAt time.Time
	Key         []byte
}

// Enqueue submits one task and returns it with the payload size in bytes.
func Enqueue(ctx context.Context, client mikyu.Client, queue, input string, opts EnqueueOptions) (mikyu.Task, int, error) {
	payload, err := ResolvePayload(input)
	if err != nil {
		return mikyu.Task{}, 0, err
	}
	tq, err := client.Task(ctx, queue)
	if err != nil {
		return mikyu.Task{}, 0, err
	}

	var taskOpts []mikyu.EnqueueOption
	if opts.Lease > 0 {
		taskOpts = append(taskOpts, mikyu.WithTaskLease(opts.Lease))
	}
	if opts.MaxAttempts > 0 {
		taskOpts = append(taskOpts, mikyu.WithTaskMaxAttempts(opts.MaxAttempts))
	}
	if !opts.ScheduledAt.IsZero() {
		taskOpts = append(taskOpts, mikyu.WithScheduledAt(opts.ScheduledAt))
	}
	if len(opts.Key) > 0 {
		taskOpts = append(taskOpts, mikyu.WithKey(opts.Key))
	}

	task, err := tq.Enqueue(ctx, payload, taskOpts...)
	if err != nil {
		return mikyu.Task{}, 0, err
	}
	return task, len(payload), nil
}

// Dequeue waits up to timeout for a due task; timeout < 0 waits indefinitely, 0 is a single non-blocking attempt.
func Dequeue(ctx context.Context, client mikyu.Client, queue string, timeout time.Duration) (mikyu.Task, error) {
	tq, err := client.Task(ctx, queue)
	if err != nil {
		return mikyu.Task{}, err
	}
	if timeout < 0 {
		return tq.Dequeue(ctx)
	}
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

// TaskAck acknowledges a leased task.
func TaskAck(ctx context.Context, client mikyu.Client, queue string, ref mikyu.TaskRef) error {
	tq, err := client.Task(ctx, queue)
	if err != nil {
		return err
	}
	return tq.Ack(ctx, ref)
}

// TaskNack returns a task for redelivery with either the server-computed backoff or an explicit retry delay.
func TaskNack(ctx context.Context, client mikyu.Client, queue string, ref mikyu.TaskRef, retryAfter time.Duration, explicitRetry bool) error {
	tq, err := client.Task(ctx, queue)
	if err != nil {
		return err
	}
	if explicitRetry {
		return tq.Nack(ctx, ref, mikyu.WithRetryAfter(retryAfter))
	}
	return tq.Nack(ctx, ref)
}

// TaskExtend extends a task lease forward.
func TaskExtend(ctx context.Context, client mikyu.Client, queue string, ref mikyu.TaskRef, timeout time.Duration) error {
	tq, err := client.Task(ctx, queue)
	if err != nil {
		return err
	}
	return tq.Extend(ctx, ref, timeout)
}

// TaskDeadLetters returns a queue's dead-letter tasks.
func TaskDeadLetters(ctx context.Context, client mikyu.Client, queue string) ([]mikyu.Task, error) {
	tq, err := client.Task(ctx, queue)
	if err != nil {
		return nil, err
	}
	return tq.DeadLetters(ctx)
}

// TaskAckDeadLetter permanently removes a dead-letter task.
func TaskAckDeadLetter(ctx context.Context, client mikyu.Client, queue string, ref mikyu.TaskRef) error {
	tq, err := client.Task(ctx, queue)
	if err != nil {
		return err
	}
	return tq.AckDeadLetter(ctx, ref)
}

// TaskArchive moves a task into its queue's archive log.
func TaskArchive(ctx context.Context, client mikyu.Client, queue string, ref mikyu.TaskRef) error {
	tq, err := client.Task(ctx, queue)
	if err != nil {
		return err
	}
	return tq.Archive(ctx, ref)
}

// TaskRecord is the JSON shape a task takes on the CLI, accepted back by the task verbs for the strict lease check.
type TaskRecord struct {
	ID          string    `json:"id"`
	Attempts    uint32    `json:"attempts"`
	MaxAttempts uint32    `json:"max_attempts"`
	Lease  time.Time `json:"lease"`
	ScheduledAt time.Time `json:"scheduled_at"`
	DeliveryID  string    `json:"delivery_id"`
	Offset      uint64    `json:"offset"`
	Payload     string    `json:"payload"`
	Key         []byte    `json:"key,omitempty"`
	Stalled     uint32    `json:"stalled"`
}

// Record renders a task as its CLI record shape.
func Record(t mikyu.Task) TaskRecord {
	return TaskRecord{
		ID:          t.ID,
		Attempts:    t.Attempts,
		MaxAttempts: t.MaxAttempts,
		Lease:  t.Lease,
		ScheduledAt: t.ScheduledAt,
		DeliveryID:  t.DeliveryID,
		Offset:      t.Offset,
		Payload:     string(t.Payload),
		Key:         t.Key,
		Stalled:     t.Stalled,
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
			Lease:  rec.Lease,
			ScheduledAt: rec.ScheduledAt,
			DeliveryID:  rec.DeliveryID,
			Offset:      rec.Offset,
			Key:         rec.Key,
			Stalled:     rec.Stalled,
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
