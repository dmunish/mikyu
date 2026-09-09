package command

import (
	"context"
	"errors"
	"time"

	"github.com/dmunish/mikyu"
)

type PublishOptions struct {
	VisibilityTimeout time.Duration
	MaxAttempts       uint32
	ScheduleAt        time.Time
}

func Publish(ctx context.Context, client mikyu.Client, queue string, input string, opts PublishOptions) (mikyu.Task, int, error) {
	payload, err := ResolvePayload(input)
	if err != nil {
		return mikyu.Task{}, 0, err
	}

	tq, err := client.TaskQueue(queue)
	if err != nil {
		return mikyu.Task{}, 0, err
	}

	var taskOpts []mikyu.TaskOption

	if opts.VisibilityTimeout > 0 {
		taskOpts = append(taskOpts, mikyu.WithVisibilityTimeout(opts.VisibilityTimeout))
	}

	if opts.MaxAttempts > 0 {
		taskOpts = append(taskOpts, mikyu.WithMaxAttempts(opts.MaxAttempts))
	}

	if !opts.ScheduleAt.IsZero() {
		taskOpts = append(taskOpts, mikyu.WithScheduleAt(opts.ScheduleAt))
	}

	task, err := tq.Enqueue(ctx, payload, taskOpts...)
	if err != nil {
		return mikyu.Task{}, 0, err
	}

	return task, len(payload), nil
}

func Pop(ctx context.Context, client mikyu.Client, queue string, timeout time.Duration) (mikyu.Task, error) {
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

	popCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	task, err := tq.Dequeue(popCtx)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return mikyu.Task{}, mikyu.ErrTaskNotAvailable
		}
		return mikyu.Task{}, err
	}

	return task, nil
}

func TaskAck(ctx context.Context, client mikyu.Client, queue string, taskID string) error {
	tq, err := client.TaskQueue(queue)
	if err != nil {
		return err
	}

	return tq.AckID(ctx, taskID)
}

func TaskNack(ctx context.Context, client mikyu.Client, queue string, taskID string, retryAfter time.Duration, explicitRetry bool) error {
	tq, err := client.TaskQueue(queue)
	if err != nil {
		return err
	}

	if explicitRetry {
		return tq.FailID(ctx, taskID, mikyu.WithRetryAfter(retryAfter))
	}

	return tq.FailID(ctx, taskID)
}

func TaskExtend(ctx context.Context, client mikyu.Client, queue string, taskID string, timeout time.Duration) error {
	tq, err := client.TaskQueue(queue)
	if err != nil {
		return err
	}

	return tq.ExtendID(ctx, taskID, timeout)
}

func TaskDeadLetters(ctx context.Context, client mikyu.Client, queue string) ([]mikyu.Task, error) {
	tq, err := client.TaskQueue(queue)
	if err != nil {
		return nil, err
	}

	return tq.DeadLetters(ctx)
}

func TaskAckDeadLetter(ctx context.Context, client mikyu.Client, queue string, taskID string) error {
	tq, err := client.TaskQueue(queue)
	if err != nil {
		return err
	}

	return tq.AckDeadLetterID(ctx, taskID)
}
