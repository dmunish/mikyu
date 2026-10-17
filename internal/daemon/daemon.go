// Package daemon implements the background server daemon.
package daemon

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/dmunish/mikyu/internal/config"
	"github.com/dmunish/mikyu/internal/engine"
	"github.com/dmunish/mikyu/internal/manager"
	"github.com/dmunish/mikyu/internal/node"
	"github.com/dmunish/mikyu/internal/server"
	"github.com/dmunish/mikyu/internal/spec"
	"github.com/dmunish/mikyu/internal/transport/protocol"
)

// ErrUsage marks a command-line mistake: an unknown flag, a stray positional argument, or an invalid flag value.
var ErrUsage = errors.New("usage error")

// spawnReadyTimeout is how long a caller waits for a freshly spawned daemon to accept connections.
const spawnReadyTimeout = 3 * time.Second

// Run parses the daemon subcommand and dispatches it. The bare form is an
// alias for run, so `mikyu daemon [flags]` and `mikyu daemon run [flags]`
// start the same foreground server.
func Run(args []string, stderr io.Writer) error {
	sub, rest := splitSubcommand(args)
	switch sub {
	case "", "run":
		return runDaemon(rest)
	default:
		fmt.Fprintf(stderr, "mikyu daemon: unknown command %q; usage: mikyu daemon run|status|stop|logs\n", sub)
		return fmt.Errorf("%w: unknown command %q", ErrUsage, sub)
	}
}

// splitSubcommand peels the leading subcommand off args: a leading flag or
// an empty argument list leaves the bare form, which means run.
func splitSubcommand(args []string) (string, []string) {
	if len(args) == 0 || strings.HasPrefix(args[0], "-") {
		return "", args
	}
	return args[0], args[1:]
}

// runDaemon starts the foreground server (flags layered over MIKYU_* environment variables and the config file: flags > env > file > defaults) and blocks until an interrupt signal or a graceful client stop ends it.
func runDaemon(args []string) error {
	cfg, resolvedPath, err := resolveConfig(args, os.Stdout, os.Stderr)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil // usage was printed to stdout; nothing else to do
		}
		return err
	}
	setupLogging(cfg.Server)
	slog.Info("starting",
		"spec_version", spec.CurrentSpecVersion,
		"go", runtime.Version(),
		"os", runtime.GOOS,
		"arch", runtime.GOARCH,
	)
	if resolvedPath != "" {
		slog.Info("config file", "path", resolvedPath)
	}
	openedAt := time.Now()

	n, release, err := node.Acquire(cfg.Server.DataDir, cfg.Defaults,
		node.WithMetrics(node.MetricsFromSession(cfg.Server)),
		node.WithEncryptionKeyFile(node.EncryptionKeyFileFromSession(cfg.Server)),
	)
	if err != nil {
		return fmt.Errorf("open node at %s: %w", cfg.Server.DataDir, err)
	}
	defer func() {
		if err := release(); err != nil {
			slog.Warn("node release failed", "err", err)
		}
	}()

	// Reconcile declared queues.
	bgCtx := context.Background()
	for name, qc := range cfg.Queues {
		var err error
		switch qc.Kind {
		case manager.KindLog:
			_, err = n.CreateLogQueue(bgCtx, name, qc.Spec)
		case manager.KindTask:
			_, err = n.CreateTaskQueue(bgCtx, name, qc.Spec)
		default:
			return fmt.Errorf("reconcile queue %q: unknown kind %q", name, qc.Kind)
		}
		if err != nil && !errors.Is(err, engine.ErrQueueExists) {
			return fmt.Errorf("reconcile queue %q: %w", name, err)
		}
	}

	var srvOpts []server.ServerOption
	if cfg.Server.TLS.Enabled {
		// TLS only wraps tcp:// listeners (see Server.ListenAndServe).
		if network, _, perr := protocol.ParseTarget(cfg.Server.Target); perr == nil && network != "tcp" {
			slog.Warn("tls enabled but the target is not tcp; tls will not apply", "target", cfg.Server.Target)
		}
		if cfg.Server.TLS.Cert == "" || cfg.Server.TLS.Key == "" {
			slog.Warn("tls enabled without both cert and key files; generating an ephemeral self-signed certificate")
		}
		tlsCfg, err := server.DefaultTLSConfig(cfg.Server.TLS.Cert, cfg.Server.TLS.Key, cfg.Server.TLS.ClientCA)
		if err != nil {
			return fmt.Errorf("TLS setup: %w", err)
		}
		srvOpts = append(srvOpts, server.WithTLS(tlsCfg))
	}

	mode, err := server.NewAuthMode(cfg.Server)
	if err != nil {
		return fmt.Errorf("auth setup: %w", err)
	}

	srvOpts = append(srvOpts, server.WithAuth(mode), server.WithMaxConnections(cfg.Server.MaxConnections))

	srv := server.New(n, srvOpts...)

	// Off by default; enabled with --metrics-listen. Started before the wire
	// server listens so a bind failure below can still stop srv cleanly.
	var httpSrv *metricsServer
	if cfg.Server.Metrics.Listen != "" {
		s, err := startMetricsHTTP(n, cfg.Server.Metrics.PerQueuePrometheus, cfg.Server.Metrics.Histograms, cfg.Server.Metrics.Listen)
		if err != nil {
			srv.Stop()
			return fmt.Errorf("metrics http: %w", err)
		}
		httpSrv = s
		slog.Info("metrics listening", "addr", cfg.Server.Metrics.Listen)
	}
	// LIFO: this defer runs after the wire server has stopped, so scrapers racing shutdown are still answered.
	defer func() {
		if httpSrv == nil {
			return
		}
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := httpSrv.Stop(stopCtx); err != nil {
			slog.Warn("metrics http stop failed", "err", err)
		}
	}()

	// Surface the serve result through a buffered channel so a bind failure
	// (e.g. port already in use) ends Run with an error instead of calling
	// os.Exit from the goroutine, which would skip the deferred cleanups
	// above and could leave Run blocked forever.
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- srv.ListenAndServe(cfg.Server.Target)
	}()

	slog.Info("serving",
		"target", cfg.Server.Target,
		"data_dir", cfg.Server.DataDir,
		"tls", cfg.Server.TLS.Enabled,
		"auth", authModeName(cfg.Server.Auth.Method),
		"max_connections", cfg.Server.MaxConnections,
		"queues", len(cfg.Queues),
	)
	slog.Debug("defaults", "json", defaultsJSON(cfg.Defaults))

	ctx, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals() // restore default handling: a second Ctrl+C force-kills

	select {
	case err := <-serveErr:
		// The server stopped on its own: a bind failure, an accept error,
		// or the node released by a graceful remote stop request.
		srv.Stop() // idempotent; releases the node
		if err != nil {
			return fmt.Errorf("server: %w", err)
		}
		slog.Info("stopped by client", "uptime", uptime(openedAt))
		return nil
	case <-ctx.Done():
		slog.Info("shutdown signal received")
	}

	srv.Stop()
	if err := <-serveErr; err != nil { // join the serve goroutine
		return fmt.Errorf("server: %w", err)
	}
	slog.Info("stopped", "uptime", uptime(openedAt))
	return nil
}

// authModeName renders the auth method for the serving line; an unset
// method means anonymous.
func authModeName(method string) string {
	if method == "" {
		return "none"
	}
	return method
}

// uptime renders the time the daemon ran for the stopping line.
func uptime(since time.Time) string {
	return time.Since(since).Round(time.Millisecond).String()
}

// defaultsJSON renders the session defaults for the debug line; a marshal
// failure degrades to an empty object rather than skipping the line.
func defaultsJSON(s spec.Spec) string {
	data, err := json.Marshal(s)
	if err != nil {
		return "{}"
	}
	return string(data)
}

// resolveConfig parses daemon flags and layers them over the config file and
// environment: CLI flags > MIKYU_* env vars > config file > compiled
// defaults. Only flags explicitly set on the command line override, so file
// and env values survive for everything else. --help is written to stdout
// and returned as flag.ErrHelp; any other input mistake is written to stderr
// with usage text and wrapped in ErrUsage.
func resolveConfig(args []string, stdout, stderr io.Writer) (*config.Config, string, error) {
	var out bytes.Buffer
	fs := flag.NewFlagSet("mikyu daemon run", flag.ContinueOnError)
	fs.SetOutput(&out)
	fs.Usage = func() {
		w := fs.Output()
		fmt.Fprint(w, "Usage: mikyu daemon run [flags]\n\nStarts the Mikyu server daemon in the foreground.\n\nFlags:\n")
		fs.PrintDefaults()
		fmt.Fprint(w, "\nPrecedence: flags > MIKYU_* environment variables > config file > defaults.\nThe config file's \"queues:\" list cannot be set from the command line.\n")
	}

	// Server
	configPath := fs.String("config", "", "Path to mikyu.yaml (optional)")
	target := fs.String("target", "", "Bind target: tcp://, unix://, or pipe://")
	dataDir := fs.String("data-dir", "", "Data directory for WAL segments and offset store")
	tlsEnabled := fs.Bool("tls-enabled", false, "Enable TLS on tcp:// listeners")
	tlsCert := fs.String("tls-cert", "", "TLS certificate file (PEM)")
	tlsKey := fs.String("tls-key", "", "TLS key file (PEM)")
	maxConnections := fs.Int("max-connections", 0, "Max concurrent client connections")

	// Spec defaults
	durability := newEnumValue("durability")
	encryption := newEnumValue("encryption")
	compaction := newEnumValue("compaction")
	fs.Var(durability, "durability", "Append ack policy: sync|async (create-only)")
	fs.Var(encryption, "encryption", "At-rest protection: none|os|aes (create-only)")
	fs.Var(compaction, "compaction", "Compaction mode: none|keys (create-only)")
	maxSegment := fs.Int64("max-segment", 0, "Bytes per segment file before rolling (create-only)")
	maxSegmentAge := fs.Duration("max-segment-age", 0, "Max age before rolling the active segment")
	maxPayload := fs.Int64("max-payload", 0, "Largest payload an append accepts (create-only)")
	maxCache := fs.Int64("max-cache", 0, "Tail cache budget in bytes")
	maxFlushBytes := fs.Int64("max-flush-bytes", 0, "Pending WAL size that triggers a flush")
	maxFlushWait := fs.Duration("max-flush-wait", 0, "Longest a batch waits before flushing")
	maxRetentionAge := fs.Duration("max-retention-age", 0, "Drop closed segments older than this")
	maxRetentionBytes := fs.Int64("max-retention-bytes", 0, "Keep at most this many bytes of history")
	lease := fs.Duration("lease", 0, "Lease length assigned to a dequeued task")
	maxAttempts := fs.Uint("max-attempts", 0, "Deliveries before a task is dead-lettered")
	maxInFlight := fs.Uint("max-in-flight", 0, "Concurrent-lease upper bound")
	maxDeadLetters := fs.Uint("max-dead-letters", 0, "Dead-letter upper bound")

	// Metrics
	metricsEnabled := fs.Bool("metrics-enabled", false, "Enable the live metrics collector")
	metricsInterval := fs.Duration("metrics-interval", 0, "Metrics tick interval")
	metricsHistory := fs.Int("metrics-history", 0, "Number of snapshots kept in memory")
	metricsListen := fs.String("metrics-listen", "", "Listen address for the metrics HTTP server (off when empty)")
	metricsPerQueuePrometheus := fs.Bool("metrics-per-queue-prometheus", false, "Emit per-queue series on the Prometheus endpoint")
	metricsHistograms := fs.Bool("metrics-histograms", false, "Enable latency histograms in the metrics snapshot")
	logLevel := fs.String("log-level", "", "Log verbosity: debug|info|warn|error")

	// Auth. --auth-token is registered so --help lists it, but rejected on use:
	// a token on argv leaks via ps.
	authMethod := fs.String("auth-method", "", "Auth mode: none|none-insecure|token|mtls")
	authTokenFile := fs.String("auth-token-file", "", "File containing the bearer token")
	fs.String("auth-token", "", "REJECTED on the daemon; use --auth-token-file")
	tlsClientCA := fs.String("tls-client-ca", "", "Client CA bundle for mtls")

	// At-rest keys. --encryption-key is registered so --help lists it, but
	// rejected on use: a key on argv leaks via ps.
	fs.String("encryption-key", "", "REJECTED on the daemon; use --encryption-key-file")
	encryptionKeyFile := fs.String("encryption-key-file", "", "File of hex AES-256 master keys, one per line")

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			_, _ = stdout.Write(out.Bytes())
			return nil, "", flag.ErrHelp
		}
		_, _ = stderr.Write(out.Bytes())
		return nil, "", fmt.Errorf("%w: %v", ErrUsage, err)
	}

	if rest := fs.Args(); len(rest) > 0 {
		arg := rest[0]
		fmt.Fprintf(stderr, "mikyu daemon run: unexpected argument %q", arg)
		if lower := strings.ToLower(arg); strings.HasSuffix(lower, ".yaml") || strings.HasSuffix(lower, ".yml") {
			fmt.Fprint(stderr, " (pass a config file with --config)")
		}
		fmt.Fprintln(stderr)
		out.Reset()
		fs.Usage()
		_, _ = stderr.Write(out.Bytes())
		return nil, "", fmt.Errorf("%w: unexpected argument %q", ErrUsage, arg)
	}

	cfg, resolvedPath, err := config.Load(*configPath)
	if err != nil {
		return nil, "", err
	}

	// Apply only explicitly-set flags (fs.Visit) so defaults from the config
	// file and environment are preserved when a flag is omitted. Bool flags
	// honor explicit false values: "--tls-enabled=false" overrides a true
	// config value, "--tls-enabled" alone sets it true.
	var flagErr error
	fs.Visit(func(f *flag.Flag) {
		if flagErr != nil {
			return
		}
		switch f.Name {
		case "target":
			if *target == "" {
				flagErr = errors.New("--target requires a value such as tcp://127.0.0.1:7420")
				return
			}
			if _, _, perr := protocol.ParseTarget(*target); perr != nil {
				flagErr = fmt.Errorf("--target: %v", perr)
				return
			}
			cfg.Server.Target = *target
		case "data-dir":
			if *dataDir == "" {
				flagErr = errors.New("--data-dir requires a non-empty path")
				return
			}
			cfg.Server.DataDir = *dataDir
		case "tls-enabled":
			cfg.Server.TLS.Enabled = *tlsEnabled
		case "tls-cert":
			cfg.Server.TLS.Cert = *tlsCert
		case "tls-key":
			cfg.Server.TLS.Key = *tlsKey
		case "tls-client-ca":
			cfg.Server.TLS.ClientCA = *tlsClientCA
		case "max-connections":
			if *maxConnections <= 0 {
				flagErr = fmt.Errorf("--max-connections must be > 0, got %d", *maxConnections)
				return
			}
			cfg.Server.MaxConnections = *maxConnections
		case "durability":
			cfg.Defaults.Storage.Durability = new(spec.Durability(durability.String()))
		case "encryption":
			cfg.Defaults.Storage.Encryption = new(spec.Encryption(encryption.String()))
		case "encryption-key-file":
			cfg.Server.Encryption.KeyFile = *encryptionKeyFile
		case "compaction":
			cfg.Defaults.Storage.Compaction = new(spec.Compaction(compaction.String()))
		case "max-segment":
			if *maxSegment <= 0 {
				flagErr = fmt.Errorf("--max-segment must be > 0, got %d", *maxSegment)
				return
			}
			cfg.Defaults.Storage.MaxSegment = new(*maxSegment)
		case "max-segment-age":
			if *maxSegmentAge <= 0 {
				flagErr = fmt.Errorf("--max-segment-age must be > 0, got %s", *maxSegmentAge)
				return
			}
			cfg.Defaults.Storage.MaxSegmentAge = new(spec.Duration(*maxSegmentAge))
		case "max-payload":
			if *maxPayload <= 0 {
				flagErr = fmt.Errorf("--max-payload must be > 0, got %d", *maxPayload)
				return
			}
			cfg.Defaults.Storage.MaxPayload = new(*maxPayload)
		case "max-cache":
			if *maxCache <= 0 {
				flagErr = fmt.Errorf("--max-cache must be > 0, got %d", *maxCache)
				return
			}
			cfg.Defaults.Storage.MaxCache = new(*maxCache)
		case "max-flush-bytes":
			if *maxFlushBytes <= 0 {
				flagErr = fmt.Errorf("--max-flush-bytes must be > 0, got %d", *maxFlushBytes)
				return
			}
			cfg.Defaults.Flush.MaxBytes = new(*maxFlushBytes)
		case "max-flush-wait":
			if *maxFlushWait <= 0 {
				flagErr = fmt.Errorf("--max-flush-wait must be > 0, got %s", *maxFlushWait)
				return
			}
			cfg.Defaults.Flush.MaxWait = new(spec.Duration(*maxFlushWait))
		case "max-retention-age":
			if *maxRetentionAge < 0 {
				flagErr = fmt.Errorf("--max-retention-age cannot be negative, got %s", *maxRetentionAge)
				return
			}
			cfg.Defaults.Retention.MaxAge = new(spec.Duration(*maxRetentionAge))
		case "max-retention-bytes":
			if *maxRetentionBytes < 0 {
				flagErr = fmt.Errorf("--max-retention-bytes cannot be negative, got %d", *maxRetentionBytes)
				return
			}
			cfg.Defaults.Retention.MaxBytes = new(*maxRetentionBytes)
		case "lease":
			if *lease <= 0 {
				flagErr = fmt.Errorf("--lease must be > 0, got %s", *lease)
				return
			}
			cfg.Defaults.Task.Lease = new(spec.Duration(*lease))
		case "max-attempts":
			if *maxAttempts <= 0 {
				flagErr = fmt.Errorf("--max-attempts must be > 0, got %d", *maxAttempts)
				return
			}
			cfg.Defaults.Task.MaxAttempts = new(uint32(*maxAttempts))
		case "max-in-flight":
			if *maxInFlight <= 0 {
				flagErr = fmt.Errorf("--max-in-flight must be > 0, got %d", *maxInFlight)
				return
			}
			cfg.Defaults.Task.MaxInFlight = new(uint32(*maxInFlight))
		case "max-dead-letters":
			if *maxDeadLetters <= 0 {
				flagErr = fmt.Errorf("--max-dead-letters must be > 0, got %d", *maxDeadLetters)
				return
			}
			cfg.Defaults.Task.MaxDeadLetters = new(uint32(*maxDeadLetters))
		case "metrics-enabled":
			cfg.Server.Metrics.Enabled = *metricsEnabled
		case "metrics-interval":
			cfg.Server.Metrics.Interval = spec.Duration(*metricsInterval)
		case "metrics-history":
			cfg.Server.Metrics.History = *metricsHistory
		case "metrics-listen":
			cfg.Server.Metrics.Listen = *metricsListen
		case "metrics-per-queue-prometheus":
			cfg.Server.Metrics.PerQueuePrometheus = *metricsPerQueuePrometheus
		case "metrics-histograms":
			cfg.Server.Metrics.Histograms = *metricsHistograms
		case "log-level":
			p, ok := spec.Lookup("log_level")
			if !ok {
				flagErr = errors.New("--log-level: missing from spec.Table")
				return
			}
			if _, err := p.Parse(*logLevel); err != nil {
				flagErr = err
				return
			}
			cfg.Server.LogLevel = *logLevel
		case "auth-method":
			cfg.Server.Auth.Method = *authMethod
		case "auth-token-file":
			cfg.Server.Auth.TokenFile = *authTokenFile
		case "auth-token":
			flagErr = errors.New("--auth-token is rejected on the daemon: a token on argv leaks via ps; use --auth-token-file")
			return
		case "encryption-key":
			flagErr = errors.New("--encryption-key is rejected on the daemon: a key on argv leaks via ps; use --encryption-key-file")
			return
		}
	})
	if flagErr != nil {
		fmt.Fprintf(stderr, "mikyu daemon: %v\n", flagErr)
		return nil, "", fmt.Errorf("%w: %v", ErrUsage, flagErr)
	}

	if err := spec.ValidateSession(cfg.Server); err != nil {
		fmt.Fprintf(stderr, "mikyu daemon: %v\n", err)
		return nil, "", fmt.Errorf("%w: %v", ErrUsage, err)
	}
	if err := spec.ValidateCreate(cfg.Defaults); err != nil {
		fmt.Fprintf(stderr, "mikyu daemon: %v\n", err)
		return nil, "", fmt.Errorf("%w: %v", ErrUsage, err)
	}

	if network, addr, perr := protocol.ParseTarget(cfg.Server.Target); perr == nil && network == "tcp" && !isLoopback(addr) {
		switch cfg.Server.Auth.Method {
		case "token":
			if !cfg.Server.TLS.Enabled {
				return nil, "", fmt.Errorf("%w: --auth-method token requires --tls-enabled on non-loopback targets", ErrUsage)
			}
		case "mtls":
			if !cfg.Server.TLS.Enabled || cfg.Server.TLS.ClientCA == "" {
				return nil, "", fmt.Errorf("%w: --auth-method mtls requires --tls-enabled and --tls-client-ca", ErrUsage)
			}
		case "", "none":
			return nil, "", fmt.Errorf("%w: refusing non-loopback %s without auth; pass --auth-method token|mtls, or --auth-method none-insecure to opt out", ErrUsage, cfg.Server.Target)
		}
	}

	return cfg, resolvedPath, nil
}

// isLoopback reports whether a tcp host:port binds to the local host.
func isLoopback(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// SpawnOptions carries the session settings a spawned daemon must inherit. AuthToken is deliberately absent: the daemon rejects it.
type SpawnOptions struct {
	Target            string
	DataDir           string
	TLSEnabled        bool
	TLSCert           string
	TLSKey            string
	AuthMethod        string
	AuthTokenFile     string
	TLSClientCA       string
	EncryptionKeyFile string
	ExtraArgs         []string
}

// EnsureDaemonRunning spawns a detached daemon with opts when opts.Target is not already serving.
func EnsureDaemonRunning(opts SpawnOptions) error {
	if opts.Target == "" {
		return errors.New("EnsureDaemonRunning: target is required")
	}
	if IsDaemonAlive(opts.Target) {
		return nil
	}
	proc, err := Spawn(opts)
	if err != nil {
		return err
	}
	if WaitForAlive(opts.Target, spawnReadyTimeout) {
		return nil
	}
	_ = proc.Kill()
	return fmt.Errorf("daemon did not start within %s", spawnReadyTimeout)
}

// IsDaemonAlive attempts to connect to the target.
func IsDaemonAlive(target string) bool {
	network, addr, err := protocol.ParseTarget(target)
	if err != nil {
		return false
	}
	conn, err := net.DialTimeout(network, addr, 150*time.Millisecond)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

// SendShutdownSignal authenticates against the daemon with the session's credential and sends an OpShutdown frame.
func SendShutdownSignal(target string, sess spec.Session) error {
	network, addr, err := protocol.ParseTarget(target)
	if err != nil {
		return err
	}
	dialer := &net.Dialer{Timeout: 2 * time.Second}
	var conn net.Conn
	if network == "tcp" && sess.TLS.Enabled {
		tlsCfg, err := clientTLSConfig(sess)
		if err != nil {
			return err
		}
		conn, err = (&tls.Dialer{Config: tlsCfg, NetDialer: dialer}).Dial(network, addr)
	} else {
		conn, err = dialer.Dial(network, addr)
	}
	if err != nil {
		return err
	}
	defer conn.Close()

	method, cred, err := resolveShutdownCredential(sess)
	if err != nil {
		return err
	}
	var ab protocol.Buffer
	protocol.EncodeAuth(&ab, method, cred)
	if err := protocol.WriteFrame(conn, protocol.OpAuth, ab.Bytes()); err != nil {
		return err
	}
	respOp, respBody, err := protocol.ReadFrame(bufio.NewReader(conn))
	if err != nil {
		return err
	}
	if respOp == protocol.OpError {
		code, msg := protocol.DecodeError(respBody)
		if code == protocol.ErrCodeUnauthorized {
			return fmt.Errorf("auth for shutdown: %w: %s", protocol.ErrUnauthorized, msg)
		}
		return fmt.Errorf("shutdown handshake failed: %s (code 0x%02x)", msg, code)
	}
	return protocol.WriteFrame(conn, protocol.OpShutdown, nil)
}

// resolveShutdownCredential picks the credential a shutdown signal presents: token file over token over mTLS over anonymous.
func resolveShutdownCredential(sess spec.Session) (byte, []byte, error) {
	if sess.Auth.TokenFile != "" {
		data, err := os.ReadFile(sess.Auth.TokenFile)
		if err != nil {
			return 0, nil, err
		}
		return protocol.AuthMethodToken, []byte(strings.TrimSpace(string(data))), nil
	}
	if sess.Auth.Token != "" {
		return protocol.AuthMethodToken, []byte(sess.Auth.Token), nil
	}
	if sess.TLS.Enabled {
		return protocol.AuthMethodMTLS, nil, nil
	}
	return protocol.AuthMethodNone, nil, nil
}

// clientTLSConfig builds the shutdown dialer's TLS setup: the client cert authenticates against the CA bundle and is trusted as the server's root.
func clientTLSConfig(sess spec.Session) (*tls.Config, error) {
	if sess.TLS.Cert == "" || sess.TLS.Key == "" {
		return nil, fmt.Errorf("tls_enabled needs tls_cert and tls_key")
	}
	pair, err := tls.LoadX509KeyPair(sess.TLS.Cert, sess.TLS.Key)
	if err != nil {
		return nil, err
	}
	caFile := sess.TLS.ClientCA
	if caFile == "" {
		caFile = sess.TLS.Cert
	}
	pem, err := os.ReadFile(caFile)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("no certificate found in %s", caFile)
	}
	return &tls.Config{
		Certificates: []tls.Certificate{pair},
		RootCAs:      pool,
		MinVersion:   tls.VersionTLS13,
	}, nil
}
