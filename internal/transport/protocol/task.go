package protocol

import (
	"errors"
	"fmt"
	"time"
)

// ErrInvalidTask is returned when a task wire payload fails validation.
var ErrInvalidTask = errors.New("invalid task record")

// Task is the wire representation of a taskqueue.Task.
type Task struct {
	ID          string
	Payload     []byte
	Attempts    uint32
	MaxAttempts uint32
	Visibility  time.Time
	ScheduledAt time.Time
	DeliveryID  string
	Offset      uint64
}

// EncodeTask serializes a task for the task driver wire protocol.
func EncodeTask(b *Buffer, t Task) {
	b.WriteBytes([]byte(t.ID))
	b.WriteBytes(t.Payload)
	b.WriteU32(t.Attempts)
	b.WriteU32(t.MaxAttempts)
	b.WriteU64(encodeTime(t.Visibility))
	b.WriteU64(encodeTime(t.ScheduledAt))
	b.WriteBytes([]byte(t.DeliveryID))
	b.WriteU64(t.Offset)
}

// DecodeTask parses a task from a task-driver wire payload.
func DecodeTask(r *Reader) (Task, error) {
	invalid := func(err error) error {
		if err == nil {
			return ErrInvalidTask
		}
		return fmt.Errorf("%w: %v", ErrInvalidTask, err)
	}

	id, err := r.ReadBytes()
	if err != nil {
		return Task{}, invalid(err)
	}
	payload, err := r.ReadBytes()
	if err != nil {
		return Task{}, invalid(err)
	}
	attempts, err := r.ReadU32()
	if err != nil {
		return Task{}, invalid(err)
	}
	maxAttempts, err := r.ReadU32()
	if err != nil {
		return Task{}, invalid(err)
	}
	visibility, err := r.ReadU64()
	if err != nil {
		return Task{}, invalid(err)
	}
	scheduled, err := r.ReadU64()
	if err != nil {
		return Task{}, invalid(err)
	}
	deliveryID, err := r.ReadBytes()
	if err != nil {
		return Task{}, invalid(err)
	}
	offset, err := r.ReadU64()
	if err != nil {
		return Task{}, invalid(err)
	}
	if len(id) == 0 || maxAttempts == 0 || attempts > maxAttempts {
		return Task{}, ErrInvalidTask
	}
	if !decodeTime(visibility).IsZero() && len(deliveryID) == 0 {
		return Task{}, ErrInvalidTask
	}
	return Task{
		ID:          string(id),
		Payload:     payload,
		Attempts:    attempts,
		MaxAttempts: maxAttempts,
		Visibility:  decodeTime(visibility),
		ScheduledAt: decodeTime(scheduled),
		DeliveryID:  string(deliveryID),
		Offset:      offset,
	}, nil
}

func encodeTime(t time.Time) uint64 {
	if t.IsZero() {
		return 0
	}
	return uint64(t.UnixNano())
}

func decodeTime(v uint64) time.Time {
	if v == 0 {
		return time.Time{}
	}
	return time.Unix(0, int64(v))
}

// EncodeTaskList writes a length-prefixed task sequence.
func EncodeTaskList(b *Buffer, tasks []Task) {
	b.WriteU32(uint32(len(tasks)))
	for _, task := range tasks {
		EncodeTask(b, task)
	}
}

// DecodeTaskList parses a task sequence.
func DecodeTaskList(r *Reader) ([]Task, error) {
	count, err := r.ReadU32()
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidTask, err)
	}
	if count > 10_000_000 {
		return nil, ErrInvalidTask
	}
	tasks := make([]Task, 0, count)
	for range count {
		task, err := DecodeTask(r)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, task)
	}
	return tasks, nil
}

// EncodeTaskDequeue serializes a dequeue request.
func EncodeTaskDequeue(b *Buffer, queue string, timeout time.Duration) {
	b.WriteBytes([]byte(queue))
	ms := max(int64(timeout/time.Millisecond), 0)
	b.WriteU64(uint64(ms))
}

// DecodeTaskDequeue parses a dequeue request.
func DecodeTaskDequeue(r *Reader) (queue string, timeout time.Duration, err error) {
	q, err := r.ReadBytes()
	if err != nil {
		return "", 0, ErrInvalidTask
	}
	ms, err := r.ReadU64()
	if err != nil {
		return "", 0, ErrInvalidTask
	}
	if len(q) == 0 {
		return "", 0, ErrInvalidTask
	}
	return string(q), time.Duration(ms) * time.Millisecond, nil
}

// TaskRef mode bytes select the payload of Ack/Nack/Extend/DeadLetterAck
// frames. Mode 0 carries a full Task record, so the server can run the
// strict stale-lease check; mode 1 carries a bare task ID and skips it by
// design (stateless admin clients).
const (
	TaskRefModeTask   byte = 0
	TaskRefModeTaskID byte = 1
)

// Nack frame payload: bytes(queue) taskRef u8 mode [u64 retryDelayNanos if mode == NackModeExplicit].
// Inherit lets the server compute exponential backoff from the task's Attempts.
const (
	NackModeInherit  byte = 0
	NackModeExplicit byte = 1
)

// WriteMode appends a TaskRef mode byte.
func WriteMode(b *Buffer, mode byte) {
	b.b = append(b.b, mode)
}

// ReadMode reads a TaskRef mode byte.
func ReadMode(r *Reader) (byte, error) {
	if r.i+1 > len(r.b) {
		return 0, ErrTruncatedBody
	}
	m := r.b[r.i]
	r.i++
	return m, nil
}

// EncodeTaskRef writes a mode byte plus its payload.
func EncodeTaskRef(b *Buffer, ref any) {
	switch v := ref.(type) {
	case Task:
		WriteMode(b, TaskRefModeTask)
		EncodeTask(b, v)
	case *Task:
		WriteMode(b, TaskRefModeTask)
		EncodeTask(b, *v)
	case string:
		WriteMode(b, TaskRefModeTaskID)
		b.WriteBytes([]byte(v))
	default:
		panic(fmt.Sprintf("protocol: unsupported task ref type %T", ref))
	}
}

// DecodeTaskRef reads a mode byte and its payload, reconstructing whichever variant was sent.
func DecodeTaskRef(r *Reader) (taskID string, task *Task, mode byte, err error) {
	m, err := ReadMode(r)
	if err != nil {
		return "", nil, 0, ErrInvalidTask
	}
	switch m {
	case TaskRefModeTask:
		t, err := DecodeTask(r)
		if err != nil {
			return "", nil, 0, err
		}
		return "", &t, m, nil
	case TaskRefModeTaskID:
		id, err := r.ReadBytes()
		if err != nil || len(id) == 0 {
			return "", nil, 0, ErrInvalidTask
		}
		return string(id), nil, m, nil
	default:
		return "", nil, 0, ErrInvalidTask
	}
}
