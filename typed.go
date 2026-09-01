package mikyu

import "context"

// Codec defines custom serialization rules for domain struct types.
type Codec[T any] struct {
	Marshal   func(T) ([]byte, error)
	Unmarshal func([]byte, *T) error
}

// Typed wraps a Queue with a Codec to produce and read typed objects T.
type Typed[T any] struct {
	queue Queue
	codec Codec[T]
}

// NewTyped decorates a Queue with typed marshaling capabilities.
func NewTyped[T any](q Queue, codec Codec[T]) *Typed[T] {
	return &Typed[T]{
		queue: q,
		codec: codec,
	}
}

func (t *Typed[T]) Produce(ctx context.Context, val T) (uint64, error) {
	data, err := t.codec.Marshal(val)
	if err != nil {
		return 0, err
	}
	return t.queue.Produce(ctx, data)
}

func (t *Typed[T]) Read(ctx context.Context, offset uint64) (T, error) {
	var zero T
	msg, err := t.queue.Read(ctx, offset)
	if err != nil {
		return zero, err
	}
	var val T
	if err := t.codec.Unmarshal(msg.Payload, &val); err != nil {
		return zero, err
	}
	return val, nil
}
