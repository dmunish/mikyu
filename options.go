package mikyu

import (
	"time"

	"github.com/dmunish/mikyu/internal/client"
	"github.com/dmunish/mikyu/internal/queue/logqueue"
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

// WithMaxConnections caps the remote connection pool.
func WithMaxConnections(n int) ClientOption { return client.WithMaxConnections(n) }

// WithTLSEnabled turns on TLS for remote tcp:// connections.
func WithTLSEnabled(enabled bool) ClientOption { return client.WithTLSEnabled(enabled) }

// WithTLSCert sets the client certificate presented to a TLS server.
func WithTLSCert(path string) ClientOption { return client.WithTLSCert(path) }

// WithTLSKey sets the private key matching the client certificate.
func WithTLSKey(path string) ClientOption { return client.WithTLSKey(path) }

// WithAutoStart makes Dial spawn a daemon with the session's settings when the
// target is not already serving.
func WithAutoStart(enabled bool) ClientOption { return client.WithAutoStart(enabled) }

// WithAuthMethod sets the auth method the session presents to a server.
func WithAuthMethod(method string) ClientOption { return client.WithAuthMethod(method) }

// WithAuthToken sets the bearer token presented to a token-mode server.
func WithAuthToken(tok string) ClientOption { return client.WithAuthToken(tok) }

// WithAuthTokenFile sets a file containing the bearer token.
func WithAuthTokenFile(path string) ClientOption { return client.WithAuthTokenFile(path) }

// WithEncryptionKeyFile sets the master-key file an embedded node uses to unseal encrypted queues.
func WithEncryptionKeyFile(path string) ClientOption { return client.WithEncryptionKeyFile(path) }

// WithTLSClientCA sets the client CA bundle for mTLS servers.
func WithTLSClientCA(path string) ClientOption { return client.WithTLSClientCA(path) }

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

// WithDurability selects the append ack policy (create-only).
func WithDurability(d Durability) QueueOption { return client.WithDurability(d) }

// WithEncryption selects at-rest protection (create-only).
func WithEncryption(e Encryption) QueueOption { return client.WithEncryption(e) }

// WithCompaction selects how closed segments shed superseded keys
// (create-only, log queues only).
func WithCompaction(c Compaction) QueueOption { return client.WithCompaction(c) }

// WithMaxSegment sets the bytes at which a segment rolls (create-only).
func WithMaxSegment(n int64) QueueOption { return client.WithMaxSegment(n) }

// WithMaxSegmentAge sets the age at which the active segment rolls.
func WithMaxSegmentAge(d time.Duration) QueueOption { return client.WithMaxSegmentAge(d) }

// WithMaxPayload sets the producer payload contract (create-only).
func WithMaxPayload(n int64) QueueOption { return client.WithMaxPayload(n) }

// WithMaxCache sets the tail cache byte budget.
func WithMaxCache(n int64) QueueOption { return client.WithMaxCache(n) }

// WithMaxFlushBytes sets the pending WAL size that triggers a flush.
func WithMaxFlushBytes(n int64) QueueOption { return client.WithMaxFlushBytes(n) }

// WithMaxFlushWait sets the longest a batch waits before flushing.
func WithMaxFlushWait(d time.Duration) QueueOption { return client.WithMaxFlushWait(d) }

// WithMaxRetentionAge deletes records older than d, capped by consumer lag.
func WithMaxRetentionAge(d time.Duration) QueueOption { return client.WithMaxRetentionAge(d) }

// WithMaxRetentionBytes caps on-disk size, dropping whole oldest segments.
func WithMaxRetentionBytes(n int64) QueueOption { return client.WithMaxRetentionBytes(n) }

// WithLease sets the queue's fallback lease for optionless enqueues.
func WithLease(d time.Duration) QueueOption { return client.WithLease(d) }

// WithMaxAttempts sets the retry budget before a task is dead-lettered.
func WithMaxAttempts(n uint32) QueueOption { return client.WithMaxAttempts(n) }

// WithMaxInFlight caps the number of tasks that can be in flight at once.
func WithMaxInFlight(n uint32) QueueOption { return client.WithMaxInFlight(n) }

// WithMaxDeadLetters caps the durable dead-letter queue; overflow drops the
// oldest entry.
func WithMaxDeadLetters(n uint32) QueueOption { return client.WithMaxDeadLetters(n) }

// WithTaskLease sets how long this dequeued task stays leased.
func WithTaskLease(d time.Duration) EnqueueOption { return taskqueue.WithLease(d) }

// WithTaskMaxAttempts sets the maximum number of deliveries before
// dead-lettering this task.
func WithTaskMaxAttempts(n uint32) EnqueueOption { return taskqueue.WithMaxAttempts(n) }

// WithKey associates a key with the task for archival or compaction.
func WithKey(k []byte) EnqueueOption { return taskqueue.WithKey(k) }

// WithRecordKey associates a compaction key with a log record; records with the
// same key are versions, and only the newest survives compaction.
func WithRecordKey(k []byte) AppendOption { return logqueue.WithKey(k) }

// WithScheduledAt delays the first delivery until the supplied time.
func WithScheduledAt(at time.Time) EnqueueOption { return taskqueue.WithScheduledAt(at) }

// WithRetryAfter overrides the retry delay for a nacked task.
func WithRetryAfter(d time.Duration) NackOption { return taskqueue.WithRetryAfter(d) }
