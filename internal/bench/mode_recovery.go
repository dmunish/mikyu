package bench

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/dmunish/mikyu"
	"github.com/dmunish/mikyu/internal/bench/result"
)

// errRecoveryQueues reports a recovery run asked to spread across more than one queue.
var errRecoveryQueues = errors.New("recovery mode requires --queues 1")

// childWriteWindow is how long the crash child writes before the parent kills it.
const childWriteWindow = 200 * time.Millisecond

// runRecovery writes a backlog, kills a mid-write child, then times reopening the queue.
func runRecovery(ctx context.Context, r *runner) error {
	r.beginPhase(PhasePopulate)
	if err := r.recoveryPopulate(ctx); err != nil {
		return err
	}
	r.endPhase(time.Now())

	r.beginPhase(PhaseAbort)
	if err := r.recoveryCrash(ctx); err != nil {
		return err
	}
	recovery, err := r.recoveryMeasure(ctx)
	r.endPhase(time.Now())
	if err != nil {
		return err
	}
	r.recovery = recovery
	return nil
}

// recoveryPopulate writes CrashAfterBytes as seeded payloads through the run's own client.
func (r *runner) recoveryPopulate(ctx context.Context) error {
	w := r.opts.Workload
	total := w.CrashAfterBytes / int64(w.MsgSize)
	if total <= 0 {
		return fmt.Errorf("%w: crash_after_bytes must cover at least one message", ErrUsage)
	}
	buf := make([]byte, w.MsgSize)
	for i := range total {
		payloadFor(w.Seed, uint64(i), buf)
		if r.isLog() {
			if _, err := r.logQueues[0].Append(ctx, buf); err != nil {
				return err
			}
		} else if _, err := r.taskQueues[0].Enqueue(ctx, buf); err != nil {
			return err
		}
		if i%4096 == 0 {
			emitProgress(r.opts, ProgressEvent{Kind: ProgressTick, Phase: PhasePopulate, Bytes: i * int64(w.MsgSize), Total: w.CrashAfterBytes})
		}
	}
	return nil
}

// recoveryCrash releases the directory and kills a detached child writing into the queue.
func (r *runner) recoveryCrash(ctx context.Context) error {
	if err := r.client.Close(); err != nil {
		return err
	}
	args := []string{
		"bench-child",
		"--dir", r.opts.Dir,
		"--kind", r.opts.Workload.Kind,
		"--queue", r.queueNames[0],
		"--msg-size", strconv.Itoa(r.opts.Workload.MsgSize),
		"--seed", strconv.FormatUint(uint64(r.opts.Workload.Seed), 10),
	}
	proc, err := spawnDetached(args)
	if err != nil {
		return err
	}
	if !sleepCtx(ctx, childWriteWindow) {
		_ = kill(proc)
		return ctx.Err()
	}
	return kill(proc)
}

// recoveryMeasure reopens the directory and times the crash-to-open, crash-to-first-read, and crash-to-full-recovery milestones.
func (r *runner) recoveryMeasure(ctx context.Context) (*result.RecoveryMetrics, error) {
	start := time.Now()
	client, err := mikyu.Open(r.opts.Dir)
	if err != nil {
		return nil, err
	}
	defer client.Close()
	openNS := time.Since(start).Nanoseconds()

	if err := r.recoveryFirstRead(ctx, client); err != nil {
		return nil, err
	}
	firstNS := time.Since(start).Nanoseconds()

	if err := r.recoveryFull(ctx, client); err != nil {
		return nil, err
	}
	fullNS := time.Since(start).Nanoseconds()

	return &result.RecoveryMetrics{
		TimeToOpenNS:         openNS,
		TimeToFirstReadNS:    firstNS,
		TimeToFullRecoveryNS: fullNS,
	}, nil
}

// recoveryFirstRead proves the queue answers its first read after the crash.
func (r *runner) recoveryFirstRead(ctx context.Context, client mikyu.Client) error {
	if r.isLog() {
		q, err := client.Log(ctx, r.queueNames[0])
		if err != nil {
			return err
		}
		minOffset, err := q.MinOffset(ctx)
		if err != nil {
			return err
		}
		_, err = q.Read(ctx, minOffset)
		return err
	}
	q, err := client.Task(ctx, r.queueNames[0])
	if err != nil {
		return err
	}
	if _, err := q.TryDequeue(ctx); err != nil && !errors.Is(err, mikyu.ErrTaskNotAvailable) {
		return err
	}
	return nil
}

// recoveryFull proves the queue's tail is readable after the crash.
func (r *runner) recoveryFull(ctx context.Context, client mikyu.Client) error {
	if r.isLog() {
		q, err := client.Log(ctx, r.queueNames[0])
		if err != nil {
			return err
		}
		_, err = q.LastOffset(ctx)
		return err
	}
	q, err := client.Task(ctx, r.queueNames[0])
	if err != nil {
		return err
	}
	_, err = q.DeadLetters(ctx)
	return err
}
