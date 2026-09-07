package mikyu

import (
	"crypto/tls"
	"time"

	"github.com/dmunish/mikyu/internal/client"
)

// ClientConfig is re-exported from the internal client package for backwards compatibility.
type ClientConfig = client.Config

// ClientOption configures a client.
type ClientOption = client.Option

// QueueConfig is re-exported from the internal client package for backwards compatibility.
type QueueConfig = client.QueueConfig

// QueueOption configures a queue.
type QueueOption = client.QueueOption

func DefaultClientConfig() ClientConfig {
	return client.DefaultClientConfig()
}

func WithBatchSize(n int) ClientOption {
	return client.WithBatchSize(n)
}

func WithMaxInterval(d time.Duration) ClientOption {
	return client.WithMaxInterval(d)
}

// WithFlushInterval is an alias for WithMaxInterval.
func WithFlushInterval(d time.Duration) ClientOption {
	return client.WithFlushInterval(d)
}

func WithMinInterval(d time.Duration) ClientOption {
	return client.WithMinInterval(d)
}

func WithMaxConns(n int) ClientOption {
	return client.WithMaxConns(n)
}

func WithClientDirectoryEncryption(enabled bool) ClientOption {
	return client.WithClientDirectoryEncryption(enabled)
}

func WithClientTLS(tlsConfig *tls.Config) ClientOption {
	return client.WithClientTLS(tlsConfig)
}

func WithFsync(enabled bool) ClientOption {
	return client.WithFsync(enabled)
}

func WithQueueBatchSize(n int) QueueOption {
	return client.WithQueueBatchSize(n)
}

func WithQueueMaxInterval(d time.Duration) QueueOption {
	return client.WithQueueMaxInterval(d)
}

func WithQueueMinInterval(d time.Duration) QueueOption {
	return client.WithQueueMinInterval(d)
}

func WithMaxSegmentBytes(bytes int64) QueueOption {
	return client.WithMaxSegmentBytes(bytes)
}

func WithMaxMessageBytes(bytes int64) QueueOption {
	return client.WithMaxMessageBytes(bytes)
}

func WithMaxSegmentAge(age time.Duration) QueueOption {
	return client.WithMaxSegmentAge(age)
}

func WithDirectoryEncryption(enabled bool) QueueOption {
	return client.WithDirectoryEncryption(enabled)
}

func WithQueueFsync(enabled bool) QueueOption {
	return client.WithQueueFsync(enabled)
}



