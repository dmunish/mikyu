package engine

import (
	"context"
	"time"
)

// Storage defines the policy‑agnostic storage operations for a log queue.
type Storage interface {
	// Name returns the queue name.
	Name() string
	// Append stores a single payload and returns its offset.
	Append(ctx context.Context, payload []byte, opts ...AppendOption) (uint64, error)
	// AppendBatch stores multiple payloads and returns their offsets.
	AppendBatch(ctx context.Context, payloads [][]byte, opts ...AppendOption) ([]uint64, error)
	// Read retrieves a single message by offset .
	Read(ctx context.Context, offset uint64) (Record, error)
	// ReadBatch retrieves a batch of messages starting at offset.
	ReadBatch(ctx context.Context, start uint64, n int) ([]Record, error)
	// Fetch returns up to maxCount messages starting at offset, blocking up to maxWait for new data.
	Fetch(ctx context.Context, offset uint64, maxCount int, maxWait time.Duration) ([]Record, error)
	// LastOffset returns the offset of the last written message.
	LastOffset() uint64
	// CommitOffset persists the consumer group offset.
	CommitOffset(ctx context.Context, group string, offset uint64) error
	// Committed returns the offset a group last committed (0 if never).
	Committed(ctx context.Context, group string) (uint64, error)

	// MaxAppendPayloadBytes returns the largest payload that can fit in a single storage record.
	MaxAppendPayloadBytes() int64

	// Maintenance & truncation primitives
	MinOffset() uint64
	Segments() []SegmentInfo
	TruncateBefore(ctx context.Context, offset uint64) (reclaimedBytes int64, err error)

	// FloorOlderThan returns the highest truncation floor permitted by age:
	// the base offset of the first segment whose last record is at or after
	// olderThan (or of the active segment). Closed segments are walked oldest
	// to newest; one qualifies only if its last record's timestamp is strictly
	// before olderThan. The active segment is never included.
	FloorOlderThan(ctx context.Context, olderThan time.Time) (uint64, error)
	// FloorForSize returns the highest truncation floor that brings the
	// queue's on-disk size within maxBytes by dropping whole oldest closed
	// segments. The active segment is never dropped, so a queue whose active
	// segment alone exceeds the cap truncates nothing.
	FloorForSize(ctx context.Context, maxBytes int64) (uint64, error)
}
