package mikyu

import (
	"time"

	"github.com/dmunish/mikyu/internal/client"
)

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

// WithAutoStart makes Dial spawn a daemon with the session's settings
// when nothing is serving the target yet.
func WithAutoStart(enabled bool) ClientOption { return client.WithAutoStart(enabled) }

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
