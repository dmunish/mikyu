package taskqueue

import (
	"context"
	"errors"
	"fmt"
	"time"
)

const (
	ArchivePrefix = "-archive"
)

var ErrArchiveFailed = errors.New("archive append failed")

// Archive moves an in-flight or DLQ task to the archive log.
func (q *queue) Archive(ctx context.Context, ref Ref) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed || q.closing {
		return ErrTaskClosed
	}
	if err := q.refreshLocked(ctx); err != nil {
		return err
	}

	// Resolve: active first (strict), then dlq.
	if state, err := q.resolveLeaseLocked(ref); err == nil {
		return q.archiveActiveLocked(ctx, state, "inflight")
	} else if !errors.Is(err, ErrTaskNotFound) {
		return err // stale or not-inflight: don't fall through
	}
	if dl, ok := q.dlq[ref.taskID()]; ok {
		return q.archiveDLLocked(ctx, dl, "dlq")
	}
	return ErrTaskNotFound
}

func (q *queue) archiveActiveLocked(ctx context.Context, state *taskState, from string) error {
	env := encodeArchiveEnvelope(state.task, q.name, from)
	if q.archiveLog == nil {
		return fmt.Errorf("%w: archive log not configured", ErrArchiveFailed)
	}
	appender, err := q.archiveLog(ctx, q.name)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrArchiveFailed, err)
	}
	if _, err := appender.Append(ctx, env); err != nil {
		return fmt.Errorf("%w: %v", ErrArchiveFailed, err)
	}
	delete(q.active, state.task.ID)
	q.dirty++
	q.broadcastLocked()
	q.pruneScheduleLocked()
	return nil
}

func (q *queue) archiveDLLocked(ctx context.Context, dl Task, from string) error {
	env := encodeArchiveEnvelope(dl, q.name, from)
	if q.archiveLog == nil {
		return fmt.Errorf("%w: archive log not configured", ErrArchiveFailed)
	}
	appender, err := q.archiveLog(ctx, q.name)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrArchiveFailed, err)
	}
	if _, err := appender.Append(ctx, env); err != nil {
		return fmt.Errorf("%w: %v", ErrArchiveFailed, err)
	}
	delete(q.dlq, dl.ID)
	delete(q.dlqUncheckpointed, dl.ID)
	q.dirty++
	q.broadcastLocked()
	return nil
}

// encodeArchiveEnvelope serializes an archive record:
// sourceQueue(string) + key(string) + taskPayload(bytes) +
// attempts(u32) + stalled(u32) + deliveryID(string) +
// offset(u64) + archivedFrom(string) + timestamp(u64).
func encodeArchiveEnvelope(task Task, sourceQueue, from string) []byte {
	var b []byte
	b = appendString(b, sourceQueue)
	b = appendString(b, string(task.Key))
	b = appendBytes(b, task.Payload)
	b = appendU32(b, task.Attempts)
	b = appendU32(b, task.Stalled)
	b = appendString(b, task.DeliveryID)
	b = appendU64(b, task.Offset)
	b = appendString(b, from)
	b = appendU64(b, uint64(time.Now().UnixNano()))
	return b
}

// decodeArchiveEnvelope parses an archive record.
func decodeArchiveEnvelope(payload []byte) (sourceQueue, key string, taskPayload []byte, attempts, stalled uint32, deliveryID string, offset uint64, archivedFrom string, timestamp time.Time, err error) {
	r := newReader(payload)
	if sourceQueue, err = r.readString(); err != nil {
		return
	}
	if key, err = r.readString(); err != nil {
		return
	}
	if taskPayload, err = r.readBytes(); err != nil {
		return
	}
	if attempts, err = r.readU32(); err != nil {
		return
	}
	if stalled, err = r.readU32(); err != nil {
		return
	}
	if deliveryID, err = r.readString(); err != nil {
		return
	}
	if offset, err = r.readU64(); err != nil {
		return
	}
	if archivedFrom, err = r.readString(); err != nil {
		return
	}
	var ts uint64
	if ts, err = r.readU64(); err != nil {
		return
	}
	timestamp = time.Unix(0, int64(ts))
	return
}

func appendString(b []byte, s string) []byte {
	b = appendU32(b, uint32(len(s)))
	return append(b, s...)
}
func appendBytes(b, data []byte) []byte {
	b = appendU32(b, uint32(len(data)))
	return append(b, data...)
}
func appendU32(b []byte, v uint32) []byte {
	b = append(b, 0, 0, 0, 0)
	b[len(b)-4] = byte(v >> 24)
	b[len(b)-3] = byte(v >> 16)
	b[len(b)-2] = byte(v >> 8)
	b[len(b)-1] = byte(v)
	return b
}
func appendU64(b []byte, v uint64) []byte {
	b = append(b, 0, 0, 0, 0, 0, 0, 0, 0)
	b[len(b)-8] = byte(v >> 56)
	b[len(b)-7] = byte(v >> 48)
	b[len(b)-6] = byte(v >> 40)
	b[len(b)-5] = byte(v >> 32)
	b[len(b)-4] = byte(v >> 24)
	b[len(b)-3] = byte(v >> 16)
	b[len(b)-2] = byte(v >> 8)
	b[len(b)-1] = byte(v)
	return b
}
