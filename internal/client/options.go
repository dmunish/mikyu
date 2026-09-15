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
	Spec        spec.Spec
	Target      string
	DataDir     string
	MaxConns    int
	TLSEnabled  bool
	TLSCertFile string
	TLSKeyFile  string

	MetricsEnabled            bool
	MetricsInterval           time.Duration
	MetricsHistory            int
	MetricsListen             string
	MetricsPerQueuePrometheus bool
	MetricsHistograms         bool
}

// DefaultSessionConfig returns the built-in connection defaults.
func DefaultSessionConfig() SessionConfig {
	return SessionConfig{MaxConns: 10}
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

// WithMaxConns caps the remote connection pool.
func WithMaxConns(n int) Option {
	return func(c *SessionConfig) {
		if n > 0 {
			c.MaxConns = n
		}
	}
}

// WithTLSEnabled turns on TLS for remote tcp:// connections.
func WithTLSEnabled(enabled bool) Option {
	return func(c *SessionConfig) { c.TLSEnabled = enabled }
}

// WithTLSCertFile sets the client certificate presented to a TLS server.
func WithTLSCertFile(path string) Option {
	return func(c *SessionConfig) { c.TLSCertFile = path }
}

// WithTLSKeyFile sets the private key matching the client certificate.
func WithTLSKeyFile(path string) Option {
	return func(c *SessionConfig) { c.TLSKeyFile = path }
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
	if c.TLSCertFile == "" || c.TLSKeyFile == "" {
		return nil, errors.New("tls_enabled needs tls_cert_file and tls_key_file")
	}
	pair, err := tls.LoadX509KeyPair(c.TLSCertFile, c.TLSKeyFile)
	if err != nil {
		return nil, err
	}
	pem, err := os.ReadFile(c.TLSCertFile)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("no certificate found in %s", c.TLSCertFile)
	}
	return &tls.Config{
		Certificates: []tls.Certificate{pair},
		RootCAs:      pool,
		MinVersion:   tls.VersionTLS13,
	}, nil
}

// QueueOption sets one field of a queue's spec; each option names its field, so the result is sparse by construction.
type QueueOption func(*spec.Spec)

// WithBatchSize sets the max pending appends before a WAL flush.
func WithBatchSize(n int) QueueOption {
	return func(s *spec.Spec) { s.Engine.BatchSize = new(n) }
}

// WithMaxInterval sets the max delay forcing a WAL flush.
func WithMaxInterval(d time.Duration) QueueOption {
	return func(s *spec.Spec) { s.Engine.MaxInterval = new(spec.Duration(d)) }
}

// WithMinInterval sets the micro-linger window grouping concurrent appends.
func WithMinInterval(d time.Duration) QueueOption {
	return func(s *spec.Spec) { s.Engine.MinInterval = new(spec.Duration(d)) }
}

// WithSegmentSize sets the bytes at which a segment rolls (create-only).
func WithSegmentSize(b int64) QueueOption {
	return func(s *spec.Spec) { s.Engine.SegmentSize = new(b) }
}

// WithMaxMessageBytes sets the producer payload contract (create-only).
func WithMaxMessageBytes(b int64) QueueOption {
	return func(s *spec.Spec) { s.Engine.MaxMessageBytes = new(b) }
}

// WithSegmentAge sets the age at which an inactive segment rolls.
func WithSegmentAge(d time.Duration) QueueOption {
	return func(s *spec.Spec) { s.Engine.SegmentAge = new(spec.Duration(d)) }
}

// WithEncrypted enables OS transparent directory encryption (create-only).
func WithEncrypted(enabled bool) QueueOption {
	return func(s *spec.Spec) { s.Engine.Encrypted = new(enabled) }
}

// WithFsync selects physical-disk durability on produce (create-only).
func WithFsync(enabled bool) QueueOption {
	return func(s *spec.Spec) { s.Engine.Fsync = new(enabled) }
}

// WithRetentionAge deletes records older than d, capped by consumer lag.
func WithRetentionAge(d time.Duration) QueueOption {
	return func(s *spec.Spec) { s.Retention.Age = new(spec.Duration(d)) }
}

// WithRetentionBytes caps on-disk size, dropping whole oldest segments.
func WithRetentionBytes(b int64) QueueOption {
	return func(s *spec.Spec) { s.Retention.Bytes = new(b) }
}

// WithDefaultVisibility sets the queue's fallback lease for optionless enqueues.
func WithDefaultVisibility(d time.Duration) QueueOption {
	return func(s *spec.Spec) { s.Task.DefaultVisibility = new(spec.Duration(d)) }
}

// WithDefaultMaxAttempts sets the queue's fallback retry budget.
func WithDefaultMaxAttempts(n uint32) QueueOption {
	return func(s *spec.Spec) { s.Task.DefaultMaxAttempts = new(n) }
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
