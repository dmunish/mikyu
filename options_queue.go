package mikyu

import (
	"time"

	"github.com/dmunish/mikyu/internal/client"
)

func WithDurability(d Durability) QueueOption { return client.WithDurability(d) }

func WithEncryption(e Encryption) QueueOption { return client.WithEncryption(e) }

func WithCompaction(c Compaction) QueueOption { return client.WithCompaction(c) }

func WithMaxSegment(n int64) QueueOption { return client.WithMaxSegment(n) }

func WithMaxSegmentAge(d time.Duration) QueueOption { return client.WithMaxSegmentAge(d) }

func WithMaxPayload(n int64) QueueOption { return client.WithMaxPayload(n) }

func WithMaxCache(n int64) QueueOption { return client.WithMaxCache(n) }

func WithMaxFlushBytes(n int64) QueueOption { return client.WithMaxFlushBytes(n) }

func WithMaxFlushWait(d time.Duration) QueueOption { return client.WithMaxFlushWait(d) }

func WithMaxRetentionAge(d time.Duration) QueueOption { return client.WithMaxRetentionAge(d) }

func WithMaxRetentionBytes(n int64) QueueOption { return client.WithMaxRetentionBytes(n) }

func WithLease(d time.Duration) QueueOption { return client.WithLease(d) }

func WithMaxAttempts(n uint32) QueueOption { return client.WithMaxAttempts(n) }

func WithMaxInFlight(n uint32) QueueOption { return client.WithMaxInFlight(n) }

func WithMaxDeadLetters(n uint32) QueueOption { return client.WithMaxDeadLetters(n) }
