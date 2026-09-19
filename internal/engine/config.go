package engine

import (
	"errors"
	"time"
)

var (
	ErrQueueExists     = errors.New("queue with this name already exists")
	ErrQueueNotFound   = errors.New("queue not found")
	ErrOffsetNotFound  = errors.New("offset not found")
	ErrMessageTooLarge = errors.New("message payload exceeds maximum allowed size")
	ErrRecordTooLarge  = errors.New("record size exceeds max segment size")
	ErrOffsetTruncated  = errors.New("offset has been truncated from storage")
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

// Config holds per-queue configuration parameters.
type Config struct {
	BatchSize        int
	MaxInterval      time.Duration
	MinInterval      time.Duration
	SegmentSize      int64
	MaxMessageBytes  int64
	SegmentAge       time.Duration
	Encrypted        bool
	Fsync            bool
	Compaction       bool
	CompactionPolicy CompactionPolicy
}

func (c Config) withDefaults() Config {
	if c.BatchSize <= 0 {
		c.BatchSize = 256
	}
	if c.MaxInterval <= 0 {
		c.MaxInterval = 10 * time.Millisecond
	}
	if c.MinInterval == 0 {
		c.MinInterval = 30 * time.Microsecond
	} else if c.MinInterval < 0 {
		c.MinInterval = 0 // Negative duration explicitly disables linger
	}
	if c.SegmentSize <= 0 {
		c.SegmentSize = 64 * 1024 * 1024
	}
	if c.MaxMessageBytes <= 0 {
		c.MaxMessageBytes = 1 * 1024 * 1024
	}
	if c.SegmentAge <= 0 {
		c.SegmentAge = 1 * time.Hour
	}
	return c
}
