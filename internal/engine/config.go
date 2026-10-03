package engine

import (
	"errors"
	"time"

	"github.com/dmunish/mikyu/internal/spec"
)

var (
	ErrQueueExists     = errors.New("queue with this name already exists")
	ErrQueueNotFound   = errors.New("queue not found")
	ErrOffsetNotFound  = errors.New("offset not found")
	ErrMessageTooLarge = errors.New("message payload exceeds maximum allowed size")
	ErrRecordTooLarge  = errors.New("record size exceeds max segment size")
	ErrOffsetTruncated = errors.New("offset has been truncated from storage")
	ErrQueueClosed     = errors.New("queue is closed")
	ErrNoData          = errors.New("no data available")
	ErrRecordCompacted = errors.New("record has been compacted")
	ErrDiskFull        = errors.New("disk is full")
)

// CompactionPolicy decides how a record's compaction group is derived.
type CompactionPolicy struct {
	// ExtractGroup derives a record's group from its payload. When nil, the
	// record's Group field is used.
	ExtractGroup func(payload []byte) []byte
}

// groupOf returns the effective group of a record under this policy.
func (p CompactionPolicy) groupOf(group, payload []byte) []byte {
	if p.ExtractGroup != nil {
		return p.ExtractGroup(payload)
	}
	return group
}

// Config holds per-queue configuration parameters; callers pass a spec.Resolve output, so every field is total.
type Config struct {
	Durability       spec.Durability
	Encryption       spec.Encryption
	Compaction       spec.Compaction
	MaxSegment       int64
	MaxSegmentAge    time.Duration
	MaxPayload       int64
	MaxCache         int64
	MaxFlushBytes    int64
	MaxFlushWait     time.Duration
	FlushLinger      time.Duration
	CompactionPolicy CompactionPolicy
	// GateCache feeds the ring cache only after the first read, saving the
	// per-append copy on produce-only workloads. Task-style drivers that read
	// back their own appends leave it off.
	GateCache bool
}

// Async reports whether appends publish without waiting for the flusher.
func (c Config) Async() bool { return c.Durability == spec.DurabilityAsync }

// KeyedCompaction reports whether closed segments shed superseded keys.
func (c Config) KeyedCompaction() bool { return c.Compaction == spec.CompactionKeys }

// EncryptedStorage reports whether the queue directory uses OS encryption.
func (c Config) EncryptedStorage() bool { return c.Encryption == spec.EncryptionOS }
