package engine

import "context"

// Storage defines the policy‑agnostic storage operations for a log queue[cite: 3].
type Storage interface {
	// Name returns the queue name[cite: 3].
	Name() string
	// Append stores a single payload and returns its offset[cite: 3].
	Append(ctx context.Context, payload []byte) (uint64, error)
	// AppendBatch stores multiple payloads and returns their offsets.
	AppendBatch(ctx context.Context, payloads [][]byte) ([]uint64, error)
	// Read retrieves a single message by offset[cite: 3].
	Read(ctx context.Context, offset uint64) (Message, error)
	// ReadBatch retrieves a batch of messages starting at offset[cite: 3].
	ReadBatch(ctx context.Context, start uint64, n int) ([]Message, error)
	// LastOffset returns the offset of the last written message[cite: 3].
	LastOffset() uint64
	// CommitOffset persists the consumer group offset[cite: 3].
	CommitOffset(ctx context.Context, group string, offset uint64) error
	// FetchOffset retrieves the persisted consumer group offset[cite: 3].
	FetchOffset(ctx context.Context, group string) (uint64, error)

	// MaxAppendPayloadBytes returns the largest payload that can fit in a single
	// storage record, accounting for the engine record overhead and the queue
	// maximum message size.
	MaxAppendPayloadBytes() int64

	// Policy-agnostic maintenance & truncation primitives
	MinOffset() uint64
	Segments() []SegmentInfo
	TruncateBefore(ctx context.Context, offset uint64) (reclaimedBytes int64, err error)
}
