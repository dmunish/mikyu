package mikyu

import (
	"context"
	"encoding/json"
	"iter"
	"time"
)

// Codec pairs a Go type with its byte form. Marshal turns a value into
// the bytes stored in a record or task; Unmarshal does the reverse.
//
// Codecs are plain function values, so any serialization works: JSON,
// protobuf, msgpack, or a hand-rolled format. JSONCodec covers the
// common case.
type Codec[T any] struct {
	// Marshal turns a value into the bytes stored in the queue.
	Marshal func(T) ([]byte, error)

	// Unmarshal turns stored bytes back into a value, writing into the
	// pointed-to target.
	Unmarshal func([]byte, *T) error
}

// JSONCodec returns a Codec that speaks JSON through encoding/json.
func JSONCodec[T any]() Codec[T] {
	return Codec[T]{
		Marshal:   func(v T) ([]byte, error) { return json.Marshal(v) },
		Unmarshal: func(b []byte, v *T) error { return json.Unmarshal(b, v) },
	}
}

// Typed is a LogQueue that speaks a Go type instead of raw bytes.
//
// It mirrors the log queue surface one to one, so everything you know
// about offsets, reads, and streaming still applies. The difference is
// at the boundary: Marshal runs before each append, Unmarshal after
// each read, and a value that does not decode is an error like any
// other.
//
// The typed surface trades the Record envelope for plain values. When
// you need offsets, record IDs, or timestamps, call Queue and drop
// back down to the LogQueue underneath.
type Typed[T any] struct {
	queue LogQueue
	codec Codec[T]
}

// NewTyped wraps a LogQueue with a codec.
func NewTyped[T any](q LogQueue, codec Codec[T]) *Typed[T] {
	return &Typed[T]{queue: q, codec: codec}
}

// Queue returns the LogQueue underneath, for when the typed surface is
// not enough.
func (t *Typed[T]) Queue() LogQueue { return t.queue }

// Append marshals val and appends it, returning its offset.
func (t *Typed[T]) Append(ctx context.Context, val T, opts ...AppendOption) (uint64, error) {
	payload, err := t.codec.Marshal(val)
	if err != nil {
		return 0, err
	}
	return t.queue.Append(ctx, payload, opts...)
}

// AppendBatch marshals and appends many values in one call. The first
// value that fails to marshal aborts the whole batch.
func (t *Typed[T]) AppendBatch(ctx context.Context, vals []T, opts ...AppendOption) ([]uint64, error) {
	payloads := make([][]byte, len(vals))
	for i, val := range vals {
		payload, err := t.codec.Marshal(val)
		if err != nil {
			return nil, err
		}
		payloads[i] = payload
	}
	return t.queue.AppendBatch(ctx, payloads, opts...)
}

// Read reads the record at offset and unmarshals it.
func (t *Typed[T]) Read(ctx context.Context, offset uint64) (T, error) {
	var zero T
	rec, err := t.queue.Read(ctx, offset)
	if err != nil {
		return zero, err
	}
	if err := t.codec.Unmarshal(rec.Payload, &zero); err != nil {
		return zero, err
	}
	return zero, nil
}

// ReadBatch reads up to count records starting at start and
// unmarshals each.
func (t *Typed[T]) ReadBatch(ctx context.Context, start uint64, count int) ([]T, error) {
	recs, err := t.queue.ReadBatch(ctx, start, count)
	if err != nil {
		return nil, err
	}
	return t.decodeAll(recs)
}

// Fetch waits up to maxWait for records at offset and unmarshals
// them.
func (t *Typed[T]) Fetch(ctx context.Context, offset uint64, maxCount int, maxWait time.Duration) ([]T, error) {
	recs, err := t.queue.Fetch(ctx, offset, maxCount, maxWait)
	if err != nil {
		return nil, err
	}
	return t.decodeAll(recs)
}

// Stream yields every decodable value from start that exists today,
// then stops.
//
// A decode failure yields a zero value with the error and stops the
// iteration, the same way a read error does.
func (t *Typed[T]) Stream(ctx context.Context, start uint64) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		for rec, err := range t.queue.Stream(ctx, start) {
			if err != nil {
				var zero T
				yield(zero, err)
				return
			}
			var val T
			if derr := t.codec.Unmarshal(rec.Payload, &val); derr != nil {
				yield(val, derr)
				return
			}
			if !yield(val, nil) {
				return
			}
		}
	}
}

// Tail yields values from start, then keeps waiting for new ones,
// until ctx is cancelled, an error occurs, or a value fails to
// decode.
func (t *Typed[T]) Tail(ctx context.Context, start uint64) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		for rec, err := range t.queue.Tail(ctx, start) {
			if err != nil {
				var zero T
				yield(zero, err)
				return
			}
			var val T
			if derr := t.codec.Unmarshal(rec.Payload, &val); derr != nil {
				yield(val, derr)
				return
			}
			if !yield(val, nil) {
				return
			}
		}
	}
}

// LastOffset returns the offset the next appended value will receive.
func (t *Typed[T]) LastOffset(ctx context.Context) (uint64, error) {
	return t.queue.LastOffset(ctx)
}

// MinOffset returns the oldest readable offset.
func (t *Typed[T]) MinOffset(ctx context.Context) (uint64, error) {
	return t.queue.MinOffset(ctx)
}

func (t *Typed[T]) decodeAll(recs []Record) ([]T, error) {
	vals := make([]T, len(recs))
	for i, rec := range recs {
		if err := t.codec.Unmarshal(rec.Payload, &vals[i]); err != nil {
			return nil, err
		}
	}
	return vals, nil
}

// Delivered is one task as the queue handed it to you, paired with its
// decoded value. Ack or Nack it, or Extend its lease while the work
// runs.
type Delivered[T any] struct {
	// Task is the raw task: ID, delivery identity, lease, attempts,
	// everything the queue tracks.
	Task Task

	// Value is the decoded payload.
	Value T
}

// TypedTask is a TaskQueue that speaks a Go type instead of raw bytes.
//
// Enqueue marshals, Dequeue and TryDequeue unmarshal, and the delivered
// pair travels together so acking what you decoded is one call. The
// identity errors (ErrTaskStale, ErrTaskNotInflight) behave exactly as
// on the raw queue.
//
// DeadLetters returns raw Task values, because payloads sitting in a
// dead-letter queue may predate the current codec and are worth seeing
// even when they do not decode. Queue returns the TaskQueue underneath.
type TypedTask[T any] struct {
	queue TaskQueue
	codec Codec[T]
}

// NewTypedTask wraps a TaskQueue with a codec.
func NewTypedTask[T any](q TaskQueue, codec Codec[T]) *TypedTask[T] {
	return &TypedTask[T]{queue: q, codec: codec}
}

// Queue returns the TaskQueue underneath, for when the typed surface
// is not enough.
func (t *TypedTask[T]) Queue() TaskQueue { return t.queue }

// Enqueue marshals val and adds it as a task.
func (t *TypedTask[T]) Enqueue(ctx context.Context, val T, opts ...EnqueueOption) (Task, error) {
	payload, err := t.codec.Marshal(val)
	if err != nil {
		return Task{}, err
	}
	return t.queue.Enqueue(ctx, payload, opts...)
}

// Dequeue waits until a task is due, unmarshals it, and returns the
// pair. It blocks until ctx is done.
func (t *TypedTask[T]) Dequeue(ctx context.Context) (Delivered[T], error) {
	return t.decode(t.queue.Dequeue(ctx))
}

// TryDequeue performs one non-blocking attempt and returns the pair,
// or ErrTaskNotAvailable when nothing is due.
func (t *TypedTask[T]) TryDequeue(ctx context.Context) (Delivered[T], error) {
	return t.decode(t.queue.TryDequeue(ctx))
}

// Ack confirms the work behind a delivered pair is done.
func (t *TypedTask[T]) Ack(ctx context.Context, d Delivered[T]) error {
	return t.queue.Ack(ctx, d.Task)
}

// Nack returns a delivered pair for another attempt. Without options
// the retry delay grows with the attempt count; WithRetryAfter sets it
// explicitly.
func (t *TypedTask[T]) Nack(ctx context.Context, d Delivered[T], opts ...NackOption) error {
	return t.queue.Nack(ctx, d.Task, opts...)
}

// Extend renews the lease of a delivered pair by timeout.
func (t *TypedTask[T]) Extend(ctx context.Context, d Delivered[T], timeout time.Duration) error {
	return t.queue.Extend(ctx, d.Task, timeout)
}

// DeadLetters returns the queue's dead-lettered tasks as raw Task
// values.
func (t *TypedTask[T]) DeadLetters(ctx context.Context) ([]Task, error) {
	return t.queue.DeadLetters(ctx)
}

// AckDeadLetter removes one dead-lettered task permanently.
func (t *TypedTask[T]) AckDeadLetter(ctx context.Context, ref Ref) error {
	return t.queue.AckDeadLetter(ctx, ref)
}

// Archive moves a task into the queue's durable archive log.
func (t *TypedTask[T]) Archive(ctx context.Context, ref Ref) error {
	return t.queue.Archive(ctx, ref)
}

// decode pairs a dequeued task with its value. A task whose payload
// does not decode comes back with the Task filled in and the decode
// error, so the caller can Nack it rather than lose it.
func (t *TypedTask[T]) decode(task Task, err error) (Delivered[T], error) {
	if err != nil {
		return Delivered[T]{}, err
	}
	var val T
	if derr := t.codec.Unmarshal(task.Payload, &val); derr != nil {
		return Delivered[T]{Task: task}, derr
	}
	return Delivered[T]{Task: task, Value: val}, nil
}
