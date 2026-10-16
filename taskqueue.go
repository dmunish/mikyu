package mikyu

import (
	"context"
	"time"
)

type TaskQueue interface {
	Name() string
	Enqueue(ctx context.Context, payload []byte, opts ...EnqueueOption) (Task, error)
	Dequeue(ctx context.Context) (Task, error)
	TryDequeue(ctx context.Context) (Task, error)
	Ack(ctx context.Context, ref Ref) error
	Nack(ctx context.Context, ref Ref, opts ...NackOption) error
	Extend(ctx context.Context, ref Ref, timeout time.Duration) error
	DeadLetters(ctx context.Context) ([]Task, error)
	AckDeadLetter(ctx context.Context, ref Ref) error
	Archive(ctx context.Context, ref Ref) error
}
