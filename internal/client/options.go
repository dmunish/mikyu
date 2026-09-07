package client

import (
	"crypto/tls"
	"time"
)

// Config controls client-wide defaults and remote connection behavior.
type Config struct {
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

// DefaultClientConfig returns the same defaults used by the original public API.
func DefaultClientConfig() Config {
	return Config{
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

// Option configures a client.
type Option func(*Config)

func WithBatchSize(n int) Option {
	return func(c *Config) {
		if n > 0 {
			c.BatchSize = n
		}
	}
}

func WithMaxInterval(d time.Duration) Option {
	return func(c *Config) {
		if d > 0 {
			c.MaxInterval = d
		}
	}
}

// WithFlushInterval is an alias for WithMaxInterval.
func WithFlushInterval(d time.Duration) Option {
	return WithMaxInterval(d)
}

func WithMinInterval(d time.Duration) Option {
	return func(c *Config) {
		if d >= 0 {
			c.MinInterval = d
		}
	}
}

func WithMaxConns(n int) Option {
	return func(c *Config) {
		if n > 0 {
			c.MaxConns = n
		}
	}
}

func WithClientDirectoryEncryption(enabled bool) Option {
	return func(c *Config) {
		c.Encrypted = enabled
	}
}

func WithClientTLS(tlsConfig *tls.Config) Option {
	return func(c *Config) {
		c.TLSConfig = tlsConfig
	}
}

func WithFsync(enabled bool) Option {
	return func(c *Config) {
		c.Fsync = enabled
	}
}

// QueueConfig controls per-queue engine defaults.
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

// QueueOption configures a queue.
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
