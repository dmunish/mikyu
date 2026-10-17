package client

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/dmunish/mikyu/internal/node"
	"github.com/dmunish/mikyu/internal/spec"
)

// SessionConfig carries client-wide defaults and remote connection behavior.
type SessionConfig struct {
	Spec           spec.Spec
	Target         string
	DataDir        string
	MaxConnections int
	TLSEnabled     bool
	TLSCert        string
	TLSKey         string

	AuthMethod    string
	AuthToken     string
	AuthTokenFile string
	TLSClientCA   string

	LogLevel string

	EncryptionKeyFile string

	Durability spec.Durability
	Encryption spec.Encryption
	Compaction spec.Compaction

	MetricsEnabled            bool
	MetricsInterval           time.Duration
	MetricsHistory            int
	MetricsListen             string
	MetricsPerQueuePrometheus bool
	MetricsHistograms         bool

	// testFrameSent runs after a request frame is written and before the response is read; test-only.
	testFrameSent func(op byte)
}

// withFrameHook installs a test frame-sent callback.
func withFrameHook(fn func(op byte)) Option {
	return func(c *SessionConfig) { c.testFrameSent = fn }
}

// DefaultSessionConfig returns the built-in connection defaults.
func DefaultSessionConfig() SessionConfig {
	return SessionConfig{MaxConnections: 10}
}

// Option configures a client.
type Option func(*SessionConfig)

// WithSessionSpec replaces the session-default spec inherited by new queues.
func WithSessionSpec(s spec.Spec) Option {
	return func(c *SessionConfig) { c.Spec = s }
}

// WithTarget sets the remote target to dial.
func WithTarget(target string) Option {
	return func(c *SessionConfig) { c.Target = target }
}

// WithDataDir sets the directory an embedded client opens.
func WithDataDir(dir string) Option {
	return func(c *SessionConfig) { c.DataDir = dir }
}

// WithMaxConnections caps the remote connection pool.
func WithMaxConnections(n int) Option {
	return func(c *SessionConfig) {
		if n > 0 {
			c.MaxConnections = n
		}
	}
}

// WithTLSEnabled turns on TLS for remote tcp:// connections.
func WithTLSEnabled(enabled bool) Option {
	return func(c *SessionConfig) { c.TLSEnabled = enabled }
}

// WithTLSCert sets the client certificate presented to a TLS server.
func WithTLSCert(path string) Option {
	return func(c *SessionConfig) { c.TLSCert = path }
}

// WithTLSKey sets the private key matching the client certificate.
func WithTLSKey(path string) Option {
	return func(c *SessionConfig) { c.TLSKey = path }
}

// WithAuthMethod sets the auth method the session presents to a server.
func WithAuthMethod(method string) Option {
	return func(c *SessionConfig) { c.AuthMethod = method }
}

// WithAuthToken sets the bearer token presented to a token-mode server.
func WithAuthToken(tok string) Option {
	return func(c *SessionConfig) { c.AuthToken = tok }
}

// WithAuthTokenFile sets a file containing the bearer token.
func WithAuthTokenFile(path string) Option {
	return func(c *SessionConfig) { c.AuthTokenFile = path }
}

// WithTLSClientCA sets the client CA bundle for mTLS servers.
func WithTLSClientCA(path string) Option {
	return func(c *SessionConfig) { c.TLSClientCA = path }
}

// WithLogLevel sets the daemon's log verbosity (debug, info, warn, or error); embedded and remote clients have no log.
func WithLogLevel(level string) Option {
	return func(c *SessionConfig) { c.LogLevel = level }
}

// WithEncryptionKeyFile sets the master-key file an embedded node uses to unseal encryption: aes queues; a remote client cannot configure the server's keys.
func WithEncryptionKeyFile(path string) Option {
	return func(c *SessionConfig) { c.EncryptionKeyFile = path }
}

// WithMetricsEnabled turns the embedded node's live collector on or off; a remote client cannot configure the server's collector.
func WithMetricsEnabled(enabled bool) Option {
	return func(c *SessionConfig) { c.MetricsEnabled = enabled }
}

// WithMetricsInterval sets the embedded collector's tick interval; a remote client cannot configure the server's collector.
func WithMetricsInterval(d time.Duration) Option {
	return func(c *SessionConfig) { c.MetricsInterval = d }
}

// WithMetricsHistory sets how many snapshots the embedded collector retains in memory; a remote client cannot configure the server's collector.
func WithMetricsHistory(n int) Option {
	return func(c *SessionConfig) { c.MetricsHistory = n }
}

// WithMetricsListen sets the daemon's metrics HTTP listen address; embedded and remote clients have no HTTP endpoint.
func WithMetricsListen(addr string) Option {
	return func(c *SessionConfig) { c.MetricsListen = addr }
}

// WithMetricsPerQueuePrometheus toggles per-queue series on the daemon's Prometheus endpoint; embedded and remote clients have no HTTP endpoint.
func WithMetricsPerQueuePrometheus(enabled bool) Option {
	return func(c *SessionConfig) { c.MetricsPerQueuePrometheus = enabled }
}

// WithMetricsHistograms toggles latency histograms in the embedded collector's snapshot; a remote client cannot configure the server's collector.
func WithMetricsHistograms(enabled bool) Option {
	return func(c *SessionConfig) { c.MetricsHistograms = enabled }
}

// metricsConfig builds the node collector settings this session requests.
func (c SessionConfig) metricsConfig() node.MetricsConfig {
	return node.MetricsConfig{
		Enabled:    c.MetricsEnabled,
		Interval:   c.MetricsInterval,
		History:    c.MetricsHistory,
		Histograms: c.MetricsHistograms,
	}
}

// TLSConfig builds the TLS configuration, or nil when TLS is disabled; the cert pair authenticates this client and is trusted as the server's root.
func (c SessionConfig) TLSConfig() (*tls.Config, error) {
	if !c.TLSEnabled {
		return nil, nil
	}
	if c.TLSCert == "" || c.TLSKey == "" {
		return nil, errors.New("tls_enabled needs tls_cert and tls_key")
	}
	pair, err := tls.LoadX509KeyPair(c.TLSCert, c.TLSKey)
	if err != nil {
		return nil, err
	}
	pem, err := os.ReadFile(c.TLSCert)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("no certificate found in %s", c.TLSCert)
	}
	return &tls.Config{
		Certificates: []tls.Certificate{pair},
		RootCAs:      pool,
		MinVersion:   tls.VersionTLS13,
	}, nil
}

// QueueOption sets one field of a queue's spec; each option names its field, so the result is sparse by construction.
type QueueOption func(*spec.Spec)

// WithDurability selects the append ack policy (create-only).
func WithDurability(d spec.Durability) QueueOption {
	return func(s *spec.Spec) { s.Storage.Durability = new(d) }
}

// WithEncryption selects at-rest protection (create-only).
func WithEncryption(e spec.Encryption) QueueOption {
	return func(s *spec.Spec) { s.Storage.Encryption = new(e) }
}

// WithCompaction selects how closed segments shed superseded keys (create-only, log queues only).
func WithCompaction(c spec.Compaction) QueueOption {
	return func(s *spec.Spec) { s.Storage.Compaction = new(c) }
}

// WithMaxSegment sets the bytes at which a segment rolls (create-only).
func WithMaxSegment(n int64) QueueOption {
	return func(s *spec.Spec) { s.Storage.MaxSegment = new(n) }
}

// WithMaxSegmentAge sets the age at which the active segment rolls.
func WithMaxSegmentAge(d time.Duration) QueueOption {
	return func(s *spec.Spec) { s.Storage.MaxSegmentAge = new(spec.Duration(d)) }
}

// WithMaxPayload sets the producer payload contract (create-only).
func WithMaxPayload(n int64) QueueOption {
	return func(s *spec.Spec) { s.Storage.MaxPayload = new(n) }
}

// WithMaxCache sets the tail cache byte budget.
func WithMaxCache(n int64) QueueOption {
	return func(s *spec.Spec) { s.Storage.MaxCache = new(n) }
}

// WithMaxFlushBytes sets the pending WAL size that triggers a flush.
func WithMaxFlushBytes(n int64) QueueOption {
	return func(s *spec.Spec) { s.Flush.MaxBytes = new(n) }
}

// WithMaxFlushWait sets the longest a batch waits before flushing.
func WithMaxFlushWait(d time.Duration) QueueOption {
	return func(s *spec.Spec) { s.Flush.MaxWait = new(spec.Duration(d)) }
}

// WithMaxRetentionAge deletes records older than d, capped by consumer lag.
func WithMaxRetentionAge(d time.Duration) QueueOption {
	return func(s *spec.Spec) { s.Retention.MaxAge = new(spec.Duration(d)) }
}

// WithMaxRetentionBytes caps on-disk size, dropping whole oldest segments.
func WithMaxRetentionBytes(n int64) QueueOption {
	return func(s *spec.Spec) { s.Retention.MaxBytes = new(n) }
}

// WithLease sets the queue's fallback lease for optionless enqueues.
func WithLease(d time.Duration) QueueOption {
	return func(s *spec.Spec) { s.Task.Lease = new(spec.Duration(d)) }
}

// WithMaxAttempts sets the queue's fallback retry budget.
func WithMaxAttempts(n uint32) QueueOption {
	return func(s *spec.Spec) { s.Task.MaxAttempts = new(n) }
}

// WithMaxInFlight caps the number of tasks that can be in flight at once.
func WithMaxInFlight(n uint32) QueueOption {
	return func(s *spec.Spec) { s.Task.MaxInFlight = new(n) }
}

// WithMaxDeadLetters caps the durable dead-letter queue; overflow drops the oldest entry.
func WithMaxDeadLetters(n uint32) QueueOption {
	return func(s *spec.Spec) { s.Task.MaxDeadLetters = new(n) }
}

// applyQueueOptions folds options into a sparse spec.
func applyQueueOptions(opts []QueueOption) spec.Spec {
	var s spec.Spec
	for _, opt := range opts {
		if opt != nil {
			opt(&s)
		}
	}
	return s
}
