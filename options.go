package mikyu

import (
	"crypto/tls"
	"time"
)

type ClientConfig struct {
	BatchSize   int
	MaxInterval time.Duration
	MinInterval time.Duration

	MaxConns int

	DefaultMaxSegmentBytes int64
	DefaultMaxMessageBytes int64
	DefaultMaxSegmentAge   time.Duration
	Encrypted              bool
	Fsync                  bool
	TLSConfig              *tls.Config
}

func DefaultClientConfig() ClientConfig {
	return ClientConfig{
		BatchSize:              256,
		MaxInterval:            10 * time.Millisecond,
		MinInterval:            30 * time.Microsecond,
		MaxConns:               10,
		DefaultMaxSegmentBytes: 64 * 1024 * 1024,
		DefaultMaxMessageBytes: 1 * 1024 * 1024,
		DefaultMaxSegmentAge:   1 * time.Hour,
		Encrypted:              false,
		Fsync:                  true,
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

func WithMaxInterval(d time.Duration) ClientOption {
	return func(c *ClientConfig) {
		if d > 0 {
			c.MaxInterval = d
		}
	}
}

// WithFlushInterval is an alias for WithMaxInterval.
func WithFlushInterval(d time.Duration) ClientOption {
	return WithMaxInterval(d)
}

func WithMinInterval(d time.Duration) ClientOption {
	return func(c *ClientConfig) {
		if d >= 0 {
			c.MinInterval = d
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

func WithClientTLS(tlsConfig *tls.Config) ClientOption {
	return func(c *ClientConfig) {
		c.TLSConfig = tlsConfig
	}
}

func WithFsync(enabled bool) ClientOption {
	return func(c *ClientConfig) {
		c.Fsync = enabled
	}
}

type QueueConfig struct {
	BatchSize       int
	MaxInterval     time.Duration
	MinInterval     time.Duration
	MaxSegmentBytes int64
	MaxMessageBytes int64
	MaxSegmentAge   time.Duration
	Encrypted       bool
	Fsync           bool
}

type QueueOption func(*QueueConfig)

func WithQueueBatchSize(n int) QueueOption {
	return func(c *QueueConfig) {
		if n > 0 {
			c.BatchSize = n
		}
	}
}

func WithQueueMaxInterval(d time.Duration) QueueOption {
	return func(c *QueueConfig) {
		if d > 0 {
			c.MaxInterval = d
		}
	}
}

func WithQueueMinInterval(d time.Duration) QueueOption {
	return func(c *QueueConfig) {
		if d >= 0 {
			c.MinInterval = d
		}
	}
}

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

func WithQueueFsync(enabled bool) QueueOption {
	return func(c *QueueConfig) {
		c.Fsync = enabled
	}
}
