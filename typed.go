package mikyu

import (
	"context"
	"encoding/json"
	"iter"
	"time"
)

type Codec[T any] struct {
	Marshal   func(T) ([]byte, error)
	Unmarshal func([]byte, *T) error
}

func JSONCodec[T any]() Codec[T] {
	return Codec[T]{
		Marshal:   func(v T) ([]byte, error) { return json.Marshal(v) },
		Unmarshal: func(b []byte, v *T) error { return json.Unmarshal(b, v) },
	}
}

type Typed[T any] struct {
	queue LogQueue
	codec Codec[T]
}

func NewTyped[T any](q LogQueue, codec Codec[T]) *Typed[T] {
	return &Typed[T]{queue: q, codec: codec}
}

func (t *Typed[T]) Queue() LogQueue { return t.queue }

func (t *Typed[T]) Append(ctx context.Context, val T, opts ...AppendOption) (uint64, error) {
	payload, err := t.codec.Marshal(val)
	if err != nil {
		return 0, err
	}
	return t.queue.Append(ctx, payload, opts...)
}

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

func (t *Typed[T]) ReadBatch(ctx context.Context, start uint64, count int) ([]T, error) {
	recs, err := t.queue.ReadBatch(ctx, start, count)
	if err != nil {
		return nil, err
	}
	return t.decodeAll(recs)
}

func (t *Typed[T]) Fetch(ctx context.Context, offset uint64, maxCount int, maxWait time.Duration) ([]T, error) {
	recs, err := t.queue.Fetch(ctx, offset, maxCount, maxWait)
	if err != nil {
		return nil, err
	}
	return t.decodeAll(recs)
}

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

func (t *Typed[T]) LastOffset(ctx context.Context) (uint64, error) {
	return t.queue.LastOffset(ctx)
}

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
