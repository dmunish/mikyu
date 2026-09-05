package mikyu

import "context"

// Codec provides serialization for a generic type.
type Codec[T any] struct {
	Marshal   func(T) ([]byte, error)
	Unmarshal func([]byte, *T) error
}

// Typed is a generic wrapper around a LogQueue that encodes/decodes values.
type Typed[T any] struct {
	queue LogQueue
	codec Codec[T]
}

// NewTyped creates a Typed wrapper.
func NewTyped[T any](q LogQueue, codec Codec[T]) *Typed[T] {
	return &Typed[T]{queue: q, codec: codec}
}

// Produce marshals and produces a value.
func (t *Typed[T]) Produce(ctx context.Context, val T) (uint64, error) {
	payload, err := t.codec.Marshal(val)
	if err != nil {
		return 0, err
	}
	return t.queue.Produce(ctx, payload)
}

// Read reads and unmarshals a value at offset.
func (t *Typed[T]) Read(ctx context.Context, offset uint64) (T, error) {
	var zero T
	msg, err := t.queue.Read(ctx, offset)
	if err != nil {
		return zero, err
	}
	err = t.codec.Unmarshal(msg.Payload, &zero)
	return zero, err
}
