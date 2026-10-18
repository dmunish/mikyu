package mikyu

import (
	"time"

	"github.com/dmunish/mikyu/internal/client"
	"github.com/dmunish/mikyu/internal/queue/logqueue"
	"github.com/dmunish/mikyu/internal/queue/taskqueue"
)

// The option types are aliases of the internal func types; the With
// constructors below follow the naming rule documented in the package
// overview: no prefix means a queue-level setting, the Task prefix a
// per-task value, Key and ScheduledAt the remaining per-task values,
// the Record prefix a per-record append value, and RetryAfter the
// per-nack delay.

// ClientOption configures Open or Dial. Options are plain values, so a
// caller can build a slice of them once and reuse it.
type ClientOption = client.Option

// QueueOption sets one field of a queue's spec. Each option names its
// field, so a patch is sparse by construction: only what you set is
// sent. Pass them to OpenLog, OpenTask, CreateLog, CreateTask, or
// UpdateQueue.
type QueueOption = client.QueueOption

// AppendOption configures one record being appended to a log queue.
type AppendOption = logqueue.AppendOption

// EnqueueOption configures one task being enqueued.
type EnqueueOption = taskqueue.EnqueueOption

// NackOption configures one nack's retry delay.
type NackOption = taskqueue.NackOption

// Client-level options: the session an embedded node or remote client
// runs with.

// WithSessionSpec sets the session defaults that new queues on this
// node inherit. Session defaults are ambient: they belong to the node,
// not to the data, so every Open of the same directory must pass
// identical defaults or the second open fails with
// ErrSessionDefaultsMismatch. To make a queue's behavior portable
// across hosts, set it on the queue itself.
func WithSessionSpec(s Spec) ClientOption { return client.WithSessionSpec(s) }

// WithTarget sets the remote target to dial. Open rejects it with
// ErrModeMismatch; pass the target to Dial directly instead.
func WithTarget(target string) ClientOption { return client.WithTarget(target) }

// WithDataDir sets the directory an embedded client opens. Dial
// rejects it with ErrModeMismatch; pass the directory to Open directly
// instead.
func WithDataDir(dir string) ClientOption { return client.WithDataDir(dir) }

// WithMaxConnections caps how many connections a remote client keeps
// in its pool. Values below one are ignored.
func WithMaxConnections(n int) ClientOption { return client.WithMaxConnections(n) }

// WithTLSEnabled turns on TLS for remote tcp:// connections. Unix
// sockets and named pipes are local and do not use it.
func WithTLSEnabled(enabled bool) ClientOption { return client.WithTLSEnabled(enabled) }

// WithTLSCert sets the client certificate presented to a TLS server.
// It also anchors server verification in this client.
func WithTLSCert(path string) ClientOption { return client.WithTLSCert(path) }

// WithTLSKey sets the private key matching the client certificate.
func WithTLSKey(path string) ClientOption { return client.WithTLSKey(path) }

// WithTLSClientCA sets the CA bundle a server uses to verify client
// certificates, for mutual TLS. Server side.
func WithTLSClientCA(path string) ClientOption { return client.WithTLSClientCA(path) }

// WithLogLevel sets the daemon's log verbosity: "debug", "info",
// "warn", or "error". Server side; embedded and remote clients keep no
// log.
func WithLogLevel(level string) ClientOption { return client.WithLogLevel(level) }

// WithAuthMethod sets how this client authenticates: "none",
// "token", or "mtls".
func WithAuthMethod(method string) ClientOption { return client.WithAuthMethod(method) }

// WithAuthToken sets the bearer token presented to a token-mode
// server. The token must be at least 16 bytes. Prefer
// WithAuthTokenFile so the secret stays out of source code.
func WithAuthToken(tok string) ClientOption { return client.WithAuthToken(tok) }

// WithAuthTokenFile sets a file containing the bearer token, one line,
// no trailing newline needed. It takes precedence over WithAuthToken.
func WithAuthTokenFile(path string) ClientOption { return client.WithAuthTokenFile(path) }

// WithEncryptionKeyFile sets the master-key file an embedded node uses
// to unseal queues created with encryption aes. The keys never live in
// the data directory; losing them means losing the data.
func WithEncryptionKeyFile(path string) ClientOption { return client.WithEncryptionKeyFile(path) }

// WithMetricsEnabled turns the embedded node's telemetry collector on
// or off. It is on by default. A remote client cannot configure the
// server's collector.
func WithMetricsEnabled(enabled bool) ClientOption { return client.WithMetricsEnabled(enabled) }

// WithMetricsInterval sets how often the embedded collector takes a
// snapshot, which is also the tick clients see through
// SubscribeSnapshots.
func WithMetricsInterval(d time.Duration) ClientOption { return client.WithMetricsInterval(d) }

// WithMetricsHistory sets how many snapshots the embedded collector
// keeps in memory.
func WithMetricsHistory(n int) ClientOption { return client.WithMetricsHistory(n) }

// WithMetricsListen sets the listen address of a daemon's metrics HTTP
// endpoint. Embedded and remote clients serve no HTTP endpoint.
func WithMetricsListen(addr string) ClientOption { return client.WithMetricsListen(addr) }

// WithMetricsPerQueuePrometheus toggles per-queue series on the
// daemon's Prometheus endpoint. Without it, only node-level series are
// exported.
func WithMetricsPerQueuePrometheus(enabled bool) ClientOption {
	return client.WithMetricsPerQueuePrometheus(enabled)
}

// WithMetricsHistograms toggles latency histograms in the collected
// snapshots. Off keeps snapshots small.
func WithMetricsHistograms(enabled bool) ClientOption { return client.WithMetricsHistograms(enabled) }

// Queue-level options: one field of a queue's spec. Pass them at
// create time, or to UpdateQueue for the mutable ones.

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

// Per-call options: one task, one record, one nack.

// WithTaskLease sets how long this task stays hidden from other
// workers after a dequeue, overriding the queue's lease setting for
// this task alone. Long work wants a generous lease, or regular
// Extend calls.
func WithTaskLease(d time.Duration) EnqueueOption { return taskqueue.WithLease(d) }

// WithTaskMaxAttempts sets the delivery budget for this task,
// overriding the queue's max_attempts setting. A task past its budget
// lands in the dead-letter queue instead of being redelivered.
func WithTaskMaxAttempts(n uint32) EnqueueOption { return taskqueue.WithMaxAttempts(n) }

// WithKey associates a key with this task. It travels with the task
// through its whole life, including into the archive.
func WithKey(k []byte) EnqueueOption { return taskqueue.WithKey(k) }

// WithScheduledAt delays this task's first delivery until at. Use it
// for work that should not start immediately; Dequeue simply will not
// hand the task out before then.
func WithScheduledAt(at time.Time) EnqueueOption { return taskqueue.WithScheduledAt(at) }

// WithRetryAfter sets exactly when a nacked task becomes deliverable
// again, overriding the default exponential backoff. A zero delay
// makes it due immediately.
func WithRetryAfter(d time.Duration) NackOption { return taskqueue.WithRetryAfter(d) }

// WithRecordKey attaches a compaction key to one appended record.
// Records that share a key are versions of one thing: when a queue
// compacts with CompactionKeys, only the newest version of each key
// survives.
func WithRecordKey(k []byte) AppendOption { return logqueue.WithKey(k) }
