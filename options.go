package mikyu

import (
	"time"

	"github.com/dmunish/mikyu/internal/client"
	"github.com/dmunish/mikyu/internal/queue/taskqueue"
)

// ClientOption configures Open or Dial.
type ClientOption = client.Option

// QueueOption sets one field of a queue's spec, so patches stay sparse.
type QueueOption = client.QueueOption

// EnqueueOption configures a task at enqueue time.
type EnqueueOption = taskqueue.EnqueueOption

// NackOption configures a retry after a nacked delivery.
type NackOption = taskqueue.NackOption

// WithSessionSpec sets the session defaults inherited by new queues; callers on
// one data directory must pass identical defaults.
func WithSessionSpec(s Spec) ClientOption { return client.WithSessionSpec(s) }

// WithTarget sets the remote target to dial.
func WithTarget(target string) ClientOption { return client.WithTarget(target) }

// WithDataDir sets the directory an embedded client opens.
func WithDataDir(dir string) ClientOption { return client.WithDataDir(dir) }

// WithMaxConns caps the remote connection pool.
func WithMaxConns(n int) ClientOption { return client.WithMaxConns(n) }

// WithTLSEnabled turns on TLS for remote tcp:// connections.
func WithTLSEnabled(enabled bool) ClientOption { return client.WithTLSEnabled(enabled) }

// WithTLSCertFile sets the client certificate presented to a TLS server.
func WithTLSCertFile(path string) ClientOption { return client.WithTLSCertFile(path) }

// WithTLSKeyFile sets the private key matching the client certificate.
func WithTLSKeyFile(path string) ClientOption { return client.WithTLSKeyFile(path) }

// WithAutoStart makes Dial spawn a daemon with the session's settings when the
// target is not already serving.
func WithAutoStart(enabled bool) ClientOption { return client.WithAutoStart(enabled) }

// WithAuthMethod sets the auth method the session presents to a server.
func WithAuthMethod(method string) ClientOption { return client.WithAuthMethod(method) }

// WithAuthToken sets the bearer token presented to a token-mode server.
func WithAuthToken(tok string) ClientOption { return client.WithAuthToken(tok) }

// WithAuthTokenFile sets a file containing the bearer token.
func WithAuthTokenFile(path string) ClientOption { return client.WithAuthTokenFile(path) }

// WithTLSClientCaFile sets the client CA bundle for mTLS servers.
func WithTLSClientCaFile(path string) ClientOption { return client.WithTLSClientCaFile(path) }

// WithMetricsEnabled turns the embedded node's live collector on or off; a
// remote client cannot configure the server's collector.
func WithMetricsEnabled(enabled bool) ClientOption { return client.WithMetricsEnabled(enabled) }

// WithMetricsInterval sets the embedded collector's tick interval; a remote
// client cannot configure the server's collector.
func WithMetricsInterval(d time.Duration) ClientOption { return client.WithMetricsInterval(d) }

// WithMetricsHistory sets how many snapshots the embedded collector retains in
// memory; a remote client cannot configure the server's collector.
func WithMetricsHistory(n int) ClientOption { return client.WithMetricsHistory(n) }

// WithMetricsListen sets the daemon's metrics HTTP listen address; embedded
// and remote clients have no HTTP endpoint.
func WithMetricsListen(addr string) ClientOption { return client.WithMetricsListen(addr) }

// WithMetricsPerQueuePrometheus toggles per-queue series on the daemon's
// Prometheus endpoint; embedded and remote clients have no HTTP endpoint.
func WithMetricsPerQueuePrometheus(enabled bool) ClientOption {
	return client.WithMetricsPerQueuePrometheus(enabled)
}

// WithMetricsHistograms toggles latency histograms in the embedded collector's
// snapshot; a remote client cannot configure the server's collector.
func WithMetricsHistograms(enabled bool) ClientOption { return client.WithMetricsHistograms(enabled) }

// WithBatchSize sets the max pending appends before a WAL flush.
func WithBatchSize(n int) QueueOption { return client.WithBatchSize(n) }

// WithMaxInterval sets the max delay forcing a WAL flush.
func WithMaxInterval(d time.Duration) QueueOption { return client.WithMaxInterval(d) }

// WithMinInterval sets the micro-linger window grouping concurrent appends.
func WithMinInterval(d time.Duration) QueueOption { return client.WithMinInterval(d) }

// WithSegmentSize sets the bytes at which a log segment rolls (create-only).
func WithSegmentSize(b int64) QueueOption { return client.WithSegmentSize(b) }

// WithMaxMessageBytes sets the producer payload contract (create-only).
func WithMaxMessageBytes(b int64) QueueOption { return client.WithMaxMessageBytes(b) }

// WithSegmentAge sets the age at which an inactive segment rolls.
func WithSegmentAge(d time.Duration) QueueOption { return client.WithSegmentAge(d) }

// WithEncrypted enables OS transparent directory encryption (create-only).
func WithEncrypted(enabled bool) QueueOption { return client.WithEncrypted(enabled) }

// WithFsync selects physical-disk durability on produce (create-only).
func WithFsync(enabled bool) QueueOption { return client.WithFsync(enabled) }

// WithRetentionAge deletes records older than d, capped by consumer lag.
func WithRetentionAge(d time.Duration) QueueOption { return client.WithRetentionAge(d) }

// WithRetentionBytes caps on-disk size, dropping whole oldest segments.
func WithRetentionBytes(b int64) QueueOption { return client.WithRetentionBytes(b) }

// WithDefaultVisibility sets the queue's fallback lease for optionless enqueues.
func WithDefaultVisibility(d time.Duration) QueueOption { return client.WithDefaultVisibility(d) }

// WithDefaultMaxAttempts sets the queue's fallback retry budget.
func WithDefaultMaxAttempts(n uint32) QueueOption { return client.WithDefaultMaxAttempts(n) }

// WithVisibility sets how long a dequeued task stays leased.
func WithVisibility(d time.Duration) EnqueueOption { return taskqueue.WithVisibility(d) }

// WithMaxAttempts sets the maximum number of deliveries before dead-lettering.
func WithMaxAttempts(n uint32) EnqueueOption { return taskqueue.WithMaxAttempts(n) }

// WithScheduledAt delays the first delivery until the supplied time.
func WithScheduledAt(at time.Time) EnqueueOption { return taskqueue.WithScheduledAt(at) }

// WithRetryAfter overrides the retry delay for a nacked task.
func WithRetryAfter(d time.Duration) NackOption { return taskqueue.WithRetryAfter(d) }
