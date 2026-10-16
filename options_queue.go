package mikyu

import (
	"time"

	"github.com/dmunish/mikyu/internal/client"
)

// WithDurability selects when appends are acknowledged: DurabilitySync
// (the default) waits for the bytes to be fsynced, DurabilityAsync
// returns sooner with a small crash loss window. Create-only.
func WithDurability(d Durability) QueueOption { return client.WithDurability(d) }

// WithEncryption selects at-rest protection: EncryptionNone (the
// default), EncryptionOS for OS directory encryption, or EncryptionAES
// for per-record AES-256-GCM. Create-only, and encryption aes needs
// the node to hold master keys from WithEncryptionKeyFile.
func WithEncryption(e Encryption) QueueOption { return client.WithEncryption(e) }

// WithCompaction selects how closed segments shed superseded records:
// CompactionNone (the default) or CompactionKeys, which keeps only the
// newest record per WithRecordKey group. Create-only, log queues only;
// CreateTask rejects it.
func WithCompaction(c Compaction) QueueOption { return client.WithCompaction(c) }

// WithMaxSegment sets the size in bytes at which the active segment
// rolls over to a new one, 64 MiB by default. Create-only. Every
// record, including its header, must fit in one segment.
func WithMaxSegment(n int64) QueueOption { return client.WithMaxSegment(n) }

// WithMaxSegmentAge sets how long the active segment may stay open
// before it rolls even if it is not full, one hour by default.
func WithMaxSegmentAge(d time.Duration) QueueOption { return client.WithMaxSegmentAge(d) }

// WithMaxPayload sets the largest payload a producer may append, 1 MiB
// by default. Create-only. It must leave room for the record header
// inside max_segment, and oversized appends fail with
// ErrMessageTooLarge.
func WithMaxPayload(n int64) QueueOption { return client.WithMaxPayload(n) }

// WithMaxCache sets the byte budget of the tail cache that serves hot
// reads from memory, 32 MiB by default. It never holds more than one
// segment's worth.
func WithMaxCache(n int64) QueueOption { return client.WithMaxCache(n) }

// WithMaxFlushBytes sets how much unflushed WAL data triggers a flush,
// 1 MiB by default. Bigger batches amortize fsyncs across more
// records.
func WithMaxFlushBytes(n int64) QueueOption { return client.WithMaxFlushBytes(n) }

// WithMaxFlushWait sets the longest a partially filled batch waits
// before flushing anyway, 10 ms by default. It is the latency
// counterpart of WithMaxFlushBytes.
func WithMaxFlushWait(d time.Duration) QueueOption { return client.WithMaxFlushWait(d) }

// WithMaxRetentionAge lets the queue drop records older than d, capped
// by what consumer groups still need. Zero, the default, means no age
// bound.
func WithMaxRetentionAge(d time.Duration) QueueOption { return client.WithMaxRetentionAge(d) }

// WithMaxRetentionBytes lets the queue drop whole oldest segments to
// stay under n bytes on disk, capped by what consumer groups still
// need. Zero, the default, means no size bound.
func WithMaxRetentionBytes(n int64) QueueOption { return client.WithMaxRetentionBytes(n) }

// WithLease sets the queue's fallback visibility lease for tasks
// enqueued without WithTaskLease, 30 seconds by default. The lease is
// how long a dequeued task stays hidden from other workers.
func WithLease(d time.Duration) QueueOption { return client.WithLease(d) }

// WithMaxAttempts sets the queue's fallback delivery budget for tasks
// enqueued without WithTaskMaxAttempts, 5 by default. A task past its
// budget lands in the dead-letter queue.
func WithMaxAttempts(n uint32) QueueOption { return client.WithMaxAttempts(n) }

// WithMaxInFlight caps how many tasks may be leased at once, 100000 by
// default. An enqueue that would exceed the bound fails with
// ErrQueueFull.
func WithMaxInFlight(n uint32) QueueOption { return client.WithMaxInFlight(n) }

// WithMaxDeadLetters caps how many tasks the durable dead-letter queue
// holds, 100000 by default. Overflow drops the oldest entry, so treat
// the dead-letter queue as a buffer to drain, not an archive.
func WithMaxDeadLetters(n uint32) QueueOption { return client.WithMaxDeadLetters(n) }
