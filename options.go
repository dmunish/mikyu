package mikyu

import "time"

type ClientConfig struct {
	BatchSize int
	Interval  time.Duration

	MaxConns int

	DefaultMaxSegmentBytes int64
	DefaultMaxMessageBytes int64
	DefaultMaxSegmentAge   time.Duration
	Encrypted              bool
}

func DefaultClientConfig() ClientConfig {
	return ClientConfig{
		BatchSize:              256,
		Interval:               10 * time.Millisecond,
		MaxConns:               10,
		DefaultMaxSegmentBytes: 64 * 1024 * 1024,
		DefaultMaxMessageBytes: 1 * 1024 * 1024,
		DefaultMaxSegmentAge:   1 * time.Hour,
		Encrypted:              false,
	}
}

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

func WithClientDirectoryEncryption(enabled bool) ClientOption {
	return func(c *ClientConfig) {
		c.Encrypted = enabled
	}
}

type QueueConfig struct {
	MaxSegmentBytes int64
	MaxMessageBytes int64
	MaxSegmentAge   time.Duration
	Encrypted       bool
}

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

func WithDirectoryEncryption(enabled bool) QueueOption {
	return func(c *QueueConfig) {
		c.Encrypted = enabled
	}
}
