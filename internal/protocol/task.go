package protocol

import (
	"fmt"
	"time"

	"github.com/dmunish/mikyu/internal/taskqueue"
)

// EncodeTask serializes a task for the task driver wire protocol.
// The engine offset is included because Ack/Fail must reject stale leases.
func EncodeTask(b *Buffer, task taskqueue.Task) {
	b.WriteBytes([]byte(task.ID))
	b.WriteBytes(task.Payload)
	b.WriteU32(task.Attempts)
	b.WriteU32(task.MaxAttempts)
	b.WriteU64(encodeTaskTime(task.Visibility))
	b.WriteU64(encodeTaskTime(task.ScheduledAt))
	b.WriteBytes([]byte(task.DeliveryID))
	b.WriteU64(task.OriginalOffset())
}

// DecodeTask parses a task from a task-driver wire payload.
func DecodeTask(r *Reader) (taskqueue.Task, error) {
	invalid := func(err error) error {
		if err == nil {
			return taskqueue.ErrInvalidTask
		}
		return fmt.Errorf("%w: %v", taskqueue.ErrInvalidTask, err)
	}

	id, err := r.ReadBytes()
	if err != nil {
		return taskqueue.Task{}, invalid(err)
	}
	payload, err := r.ReadBytes()
	if err != nil {
		return taskqueue.Task{}, invalid(err)
	}
	attempts, err := r.ReadU32()
	if err != nil {
		return taskqueue.Task{}, invalid(err)
	}
	maxAttempts, err := r.ReadU32()
	if err != nil {
		return taskqueue.Task{}, invalid(err)
	}
	visibility, err := r.ReadU64()
	if err != nil {
		return taskqueue.Task{}, invalid(err)
	}
	scheduled, err := r.ReadU64()
	if err != nil {
		return taskqueue.Task{}, invalid(err)
	}
	deliveryID, err := r.ReadBytes()
	if err != nil {
		return taskqueue.Task{}, invalid(err)
	}
	offset, err := r.ReadU64()
	if err != nil {
		return taskqueue.Task{}, invalid(err)
	}
	if len(id) == 0 || maxAttempts == 0 || attempts > maxAttempts {
		return taskqueue.Task{}, taskqueue.ErrInvalidTask
	}
	if !decodeTaskTime(visibility).IsZero() && len(deliveryID) == 0 {
		return taskqueue.Task{}, taskqueue.ErrInvalidTask
	}
	return taskqueue.NewTaskFromWireWithDeliveryID(
		string(id), payload, attempts, maxAttempts,
		decodeTaskTime(visibility), decodeTaskTime(scheduled), string(deliveryID), offset,
	), nil
}

func encodeTaskTime(t time.Time) uint64 {
	if t.IsZero() {
		return 0
	}
	return uint64(t.UnixNano())
}

func decodeTaskTime(v uint64) time.Time {
	if v == 0 {
		return time.Time{}
	}
	return time.Unix(0, int64(v))
}

// EncodeTaskList writes a length-prefixed task sequence.
func EncodeTaskList(b *Buffer, tasks []taskqueue.Task) {
	b.WriteU32(uint32(len(tasks)))
	for _, task := range tasks {
		EncodeTask(b, task)
	}
}

// DecodeTaskList parses a task sequence.
func DecodeTaskList(r *Reader) ([]taskqueue.Task, error) {
	count, err := r.ReadU32()
	if err != nil {
		return nil, fmt.Errorf("%w: %v", taskqueue.ErrInvalidTask, err)
	}
	if count > 10_000_000 {
		return nil, taskqueue.ErrInvalidTask
	}
	tasks := make([]taskqueue.Task, 0, count)
	for range count {
		task, err := DecodeTask(r)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, task)
	}
	return tasks, nil
}

// EncodeTaskID serializes a queue + task-id request.
func EncodeTaskID(b *Buffer, queue, taskID string) {
	b.WriteBytes([]byte(queue))
	b.WriteBytes([]byte(taskID))
}

// DecodeTaskID parses a queue + task-id request.
func DecodeTaskID(r *Reader) (queue string, taskID string, err error) {
	q, err := r.ReadBytes()
	if err != nil {
		return "", "", taskqueue.ErrInvalidTask
	}

	id, err := r.ReadBytes()
	if err != nil {
		return "", "", taskqueue.ErrInvalidTask
	}

	if len(q) == 0 || len(id) == 0 {
		return "", "", taskqueue.ErrInvalidTask
	}

	return string(q), string(id), nil
}
