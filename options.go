package mikyu

import "time"

// ClientConfig holds configuration settings for Client instances.
type ClientConfig struct {
	// Engine & WAL tuning
	BatchSize int
	Interval  time.Duration

	// Connection pool tuning (Remote mode)
	MaxConns int

	// Direct storage tuning defaults
	DefaultMaxSegmentBytes int64
	DefaultMaxMessageBytes int64
	DefaultMaxSegmentAge   time.Duration
}

func DefaultClientConfig() ClientConfig {
	return ClientConfig{
		BatchSize:              256,
		Interval:               10 * time.Millisecond,
		MaxConns:               10,
		DefaultMaxSegmentBytes: 64 * 1024 * 1024, // 64 MiB
		DefaultMaxMessageBytes: 1 * 1024 * 1024,  // 1 MiB
		DefaultMaxSegmentAge:   1 * time.Hour,
	}
}

// ClientOption mutates a ClientConfig.
type ClientOption func(*ClientConfig)

func WithBatchSize(n int) ClientOption {
	return func(c *ClientConfig) {
		if n > 0 {
			c.BatchSize = n
		}
	}
}

func WithFlushInterval(d time.Duration) ClientOption {
	return func(c *ClientConfig) {
		if d > 0 {
			c.Interval = d
		}
	}
}

func WithMaxConns(n int) ClientOption {
	return func(c *ClientConfig) {
		if n > 0 {
			c.MaxConns = n
		}
	}
}

// QueueConfig holds per-queue storage parameters.
type QueueConfig struct {
	MaxSegmentBytes int64
	MaxMessageBytes int64
	MaxSegmentAge   time.Duration
}

// QueueOption mutates a QueueConfig.
type QueueOption func(*QueueConfig)

func WithMaxSegmentBytes(bytes int64) QueueOption {
	return func(c *QueueConfig) {
		if bytes > 0 {
			c.MaxSegmentBytes = bytes
		}
	}
}

func WithMaxMessageBytes(bytes int64) QueueOption {
	return func(c *QueueConfig) {
		if bytes > 0 {
			c.MaxMessageBytes = bytes
		}
	}
}

func WithMaxSegmentAge(age time.Duration) QueueOption {
	return func(c *QueueConfig) {
		if age > 0 {
			c.MaxSegmentAge = age
		}
	}
}
