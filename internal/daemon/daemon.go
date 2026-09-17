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
	"log"
	"net"
	"os"
	"os/exec"
	"os/signal"
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

// Run parses args (CLI flags layered over MIKYU_* environment variables and the config file: flags > env > file > defaults), starts the daemon, and blocks until an interrupt signal or a graceful "mikyu shutdown" ends it.
func Run(args []string) error {
	cfg, resolvedPath, err := resolveConfig(args, os.Stdout, os.Stderr)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil // usage was printed to stdout; nothing else to do
		}
		return err
	}
	logEffectiveConfig(cfg, resolvedPath)

	n, release, err := node.Acquire(cfg.Server.DataDir, cfg.Defaults, node.WithMetrics(node.MetricsFromSession(cfg.Server)))
	if err != nil {
		return fmt.Errorf("open node at %s: %w", cfg.Server.DataDir, err)
	}
	defer func() {
		if err := release(); err != nil {
			log.Printf("mikyu daemon: node release: %v", err)
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
	if cfg.Server.TLSEnabled {
		// TLS only wraps tcp:// listeners (see Server.ListenAndServe).
		if network, _, perr := protocol.ParseTarget(cfg.Server.Target); perr == nil && network != "tcp" {
			log.Printf("mikyu daemon: warning: TLS is enabled but target %q is not tcp://; TLS will not apply", cfg.Server.Target)
		}
		if cfg.Server.TLSCertFile == "" || cfg.Server.TLSKeyFile == "" {
			log.Printf("mikyu daemon: warning: TLS enabled without both cert and key files; generating an ephemeral self-signed certificate")
		}
		tlsCfg, err := server.DefaultTLSConfig(cfg.Server.TLSCertFile, cfg.Server.TLSKeyFile, cfg.Server.TLSClientCaFile)
		if err != nil {
			return fmt.Errorf("TLS setup: %w", err)
		}
		srvOpts = append(srvOpts, server.WithTLS(tlsCfg))
	}

	mode, err := server.NewAuthMode(cfg.Server)
	if err != nil {
		return fmt.Errorf("auth setup: %w", err)
	}

	srvOpts = append(srvOpts, server.WithAuth(mode), server.WithMaxConns(cfg.Server.MaxConns))

	srv := server.New(n, srvOpts...)

	// Off by default; enabled with --metrics-listen. Started before the wire
	// server listens so a bind failure below can still stop srv cleanly.
	var httpSrv *metricsServer
	if cfg.Server.MetricsListen != "" {
		s, err := startMetricsHTTP(n, cfg.Server.MetricsPerQueuePrometheus, cfg.Server.MetricsHistograms, cfg.Server.MetricsListen)
		if err != nil {
			srv.Stop()
			return fmt.Errorf("metrics http: %w", err)
		}
		httpSrv = s
	}
	// LIFO: this defer runs after the wire server has stopped, so scrapers racing shutdown are still answered.
	defer func() {
		if httpSrv == nil {
			return
		}
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := httpSrv.Stop(stopCtx); err != nil {
			log.Printf("mikyu daemon: metrics http stop: %v", err)
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

	ctx, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals() // restore default handling: a second Ctrl+C force-kills

	select {
	case err := <-serveErr:
		// The server stopped on its own: a bind failure, an accept error,
		// or the node released by a graceful remote shutdown request.
		srv.Stop() // idempotent; releases the node
		if err != nil {
			return fmt.Errorf("server: %w", err)
		}
		log.Printf("mikyu daemon: stopped")
		return nil
	case <-ctx.Done():
		log.Printf("mikyu daemon: shutdown signal received")
	}

	srv.Stop()
	if err := <-serveErr; err != nil { // join the serve goroutine
		return fmt.Errorf("server: %w", err)
	}
	log.Printf("mikyu daemon: stopped")
	return nil
}

// resolveConfig parses daemon flags and layers them over the config file and
// environment: CLI flags > MIKYU_* env vars > config file > compiled
// defaults. Only flags explicitly set on the command line override, so file
// and env values survive for everything else. --help is written to stdout
// and returned as flag.ErrHelp; any other input mistake is written to stderr
// with usage text and wrapped in ErrUsage.
func resolveConfig(args []string, stdout, stderr io.Writer) (*config.Config, string, error) {
	var out bytes.Buffer
	fs := flag.NewFlagSet("mikyu daemon", flag.ContinueOnError)
	fs.SetOutput(&out)
	fs.Usage = func() {
		w := fs.Output()
		fmt.Fprint(w, "Usage: mikyu daemon [flags]\n\nStarts the Mikyu server daemon.\n\nFlags:\n")
		fs.PrintDefaults()
		fmt.Fprint(w, "\nPrecedence: flags > MIKYU_* environment variables > config file > defaults.\nThe config file's \"queues:\" list cannot be set from the command line.\n")
	}

	// Server
	configPath := fs.String("config", "", "Path to mikyu.yaml (optional)")
	target := fs.String("target", "", "Bind target: tcp://, unix://, or pipe://")
	dataDir := fs.String("data-dir", "", "Data directory for WAL segments and offset store")
	tlsEnabled := fs.Bool("tls-enabled", false, "Enable TLS on tcp:// listeners")
	tlsCertFile := fs.String("tls-cert-file", "", "TLS certificate file (PEM)")
	tlsKeyFile := fs.String("tls-key-file", "", "TLS key file (PEM)")
	maxConns := fs.Int("max-conns", 0, "Max concurrent client connections")

	// Session defaults
	batchSize := fs.Int("batch-size", 0, "Max pending messages before WAL fsync")
	maxInterval := fs.Duration("max-interval", 0, "Max wait before forcing WAL fsync")
	minInterval := fs.Duration("min-interval", 0, "Micro-linger window to group concurrent produces")
	segmentSize := fs.Int64("segment-size", 0, "Max size per log segment before rolling")
	maxMessageBytes := fs.Int64("max-message-bytes", 0, "Max payload size per produce call")
	segmentAge := fs.Duration("segment-age", 0, "Max age before rolling an inactive segment")
	encrypted := fs.Bool("encrypted", false, "Enable OS transparent directory encryption")
	fsync := fs.Bool("fsync", false, "Physical disk fsync on produce (use --fsync=false for OS page cache)")

	// Metrics
	metricsEnabled := fs.Bool("metrics-enabled", false, "Enable the live metrics collector")
	metricsInterval := fs.Duration("metrics-interval", 0, "Metrics tick interval")
	metricsHistory := fs.Int("metrics-history", 0, "Number of snapshots kept in memory")
	metricsListen := fs.String("metrics-listen", "", "Listen address for the metrics HTTP server (off when empty)")
	metricsPerQueuePrometheus := fs.Bool("metrics-per-queue-prometheus", false, "Emit per-queue series on the Prometheus endpoint")
	metricsHistograms := fs.Bool("metrics-histograms", false, "Enable latency histograms in the metrics snapshot")

	// Auth. --auth-token is registered so --help lists it, but rejected on use:
	// a token on argv leaks via ps.
	authMethod := fs.String("auth-method", "", "Auth mode: none|none-insecure|token|mtls")
	authTokenFile := fs.String("auth-token-file", "", "File containing the bearer token")
	fs.String("auth-token", "", "REJECTED on the daemon; use --auth-token-file")
	tlsClientCAFile := fs.String("tls-client-ca-file", "", "Client CA bundle for mtls")

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
		fmt.Fprintf(stderr, "mikyu daemon: unexpected argument %q", arg)
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
	// honor explicit false values: "--fsync=false" overrides a true config
	// value, "--fsync" alone sets it true.
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
			cfg.Server.TLSEnabled = *tlsEnabled
		case "tls-cert-file":
			cfg.Server.TLSCertFile = *tlsCertFile
		case "tls-key-file":
			cfg.Server.TLSKeyFile = *tlsKeyFile
		case "max-conns":
			if *maxConns <= 0 {
				flagErr = fmt.Errorf("--max-conns must be > 0, got %d", *maxConns)
				return
			}
			cfg.Server.MaxConns = *maxConns
		case "batch-size":
			if *batchSize <= 0 {
				flagErr = fmt.Errorf("--batch-size must be > 0, got %d", *batchSize)
				return
			}
			cfg.Defaults.Engine.BatchSize = new(*batchSize)
		case "max-interval":
			if *maxInterval <= 0 {
				flagErr = fmt.Errorf("--max-interval must be > 0, got %s", *maxInterval)
				return
			}
			cfg.Defaults.Engine.MaxInterval = new(spec.Duration(*maxInterval))
		case "min-interval":
			if *minInterval < 0 {
				flagErr = fmt.Errorf("--min-interval cannot be negative, got %s", *minInterval)
				return
			}
			cfg.Defaults.Engine.MinInterval = new(spec.Duration(*minInterval))
		case "segment-size":
			if *segmentSize <= 0 {
				flagErr = fmt.Errorf("--segment-size must be > 0, got %d", *segmentSize)
				return
			}
			cfg.Defaults.Engine.SegmentSize = new(*segmentSize)
		case "max-message-bytes":
			if *maxMessageBytes <= 0 {
				flagErr = fmt.Errorf("--max-message-bytes must be > 0, got %d", *maxMessageBytes)
				return
			}
			cfg.Defaults.Engine.MaxMessageBytes = new(*maxMessageBytes)
		case "segment-age":
			if *segmentAge <= 0 {
				flagErr = fmt.Errorf("--segment-age must be > 0, got %s", *segmentAge)
				return
			}
			cfg.Defaults.Engine.SegmentAge = new(spec.Duration(*segmentAge))
		case "encrypted":
			cfg.Defaults.Engine.Encrypted = new(*encrypted)
		case "fsync":
			cfg.Defaults.Engine.Fsync = new(*fsync)
		case "metrics-enabled":
			cfg.Server.MetricsEnabled = *metricsEnabled
		case "metrics-interval":
			cfg.Server.MetricsInterval = spec.Duration(*metricsInterval)
		case "metrics-history":
			cfg.Server.MetricsHistory = *metricsHistory
		case "metrics-listen":
			cfg.Server.MetricsListen = *metricsListen
		case "metrics-per-queue-prometheus":
			cfg.Server.MetricsPerQueuePrometheus = *metricsPerQueuePrometheus
		case "metrics-histograms":
			cfg.Server.MetricsHistograms = *metricsHistograms
		case "auth-method":
			cfg.Server.AuthMethod = *authMethod
		case "auth-token-file":
			cfg.Server.AuthTokenFile = *authTokenFile
		case "auth-token":
			flagErr = errors.New("--auth-token is rejected on the daemon: a token on argv leaks via ps; use --auth-token-file")
			return
		case "tls-client-ca-file":
			cfg.Server.TLSClientCaFile = *tlsClientCAFile
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

	if network, addr, perr := protocol.ParseTarget(cfg.Server.Target); perr == nil && network == "tcp" && !isLoopback(addr) {
		switch cfg.Server.AuthMethod {
		case "token":
			if !cfg.Server.TLSEnabled {
				return nil, "", fmt.Errorf("%w: --auth-method token requires --tls-enabled on non-loopback targets", ErrUsage)
			}
		case "mtls":
			if !cfg.Server.TLSEnabled || cfg.Server.TLSClientCaFile == "" {
				return nil, "", fmt.Errorf("%w: --auth-method mtls requires --tls-enabled and --tls-client-ca-file", ErrUsage)
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

// logEffectiveConfig records the fully-resolved settings so the log of a
// running daemon always shows what it actually started with, regardless of
// which layer (flag, env, file, default) each value came from.
func logEffectiveConfig(cfg *config.Config, resolvedPath string) {
	log.Printf("mikyu daemon: config file %s", resolvedPath)
	log.Printf("mikyu daemon: server target=%s data_dir=%s tls_enabled=%t max_conns=%d queues=%d",
		cfg.Server.Target, cfg.Server.DataDir, cfg.Server.TLSEnabled, cfg.Server.MaxConns, len(cfg.Queues))
	defaults, err := json.Marshal(cfg.Defaults)
	if err != nil {
		defaults = []byte("{}")
	}
	log.Printf("mikyu daemon: defaults %s", defaults)
}

// SpawnOptions carries the session settings a spawned daemon must inherit. AuthToken is deliberately absent: the daemon rejects it.
type SpawnOptions struct {
	Target          string
	DataDir         string
	TLSEnabled      bool
	TLSCertFile     string
	TLSKeyFile      string
	AuthMethod      string
	AuthTokenFile   string
	TLSClientCaFile string
	ExtraArgs       []string
}

// EnsureDaemonRunning spawns a detached daemon with opts when opts.Target is not already serving.
func EnsureDaemonRunning(opts SpawnOptions) error {
	if opts.Target == "" {
		return errors.New("EnsureDaemonRunning: target is required")
	}
	if IsDaemonAlive(opts.Target) {
		return nil
	}
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	args := []string{"daemon", "--target", opts.Target}
	if opts.DataDir != "" {
		args = append(args, "--data-dir", opts.DataDir)
	}
	if opts.TLSEnabled {
		args = append(args, "--tls-enabled")
	}
	if opts.TLSCertFile != "" {
		args = append(args, "--tls-cert-file", opts.TLSCertFile)
	}
	if opts.TLSKeyFile != "" {
		args = append(args, "--tls-key-file", opts.TLSKeyFile)
	}
	if opts.AuthMethod != "" {
		args = append(args, "--auth-method", opts.AuthMethod)
	}
	if opts.AuthTokenFile != "" {
		args = append(args, "--auth-token-file", opts.AuthTokenFile)
	}
	if opts.TLSClientCaFile != "" {
		args = append(args, "--tls-client-ca-file", opts.TLSClientCaFile)
	}
	args = append(args, opts.ExtraArgs...)
	cmd := exec.Command(executable, args...)
	setDetachedProcessAttr(cmd)
	if err := cmd.Start(); err != nil {
		return err
	}
	// Wait up to 3 seconds for daemon to come online.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if IsDaemonAlive(opts.Target) {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("daemon did not start within 3 seconds")
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
	if network == "tcp" && sess.TLSEnabled {
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
	if sess.AuthTokenFile != "" {
		data, err := os.ReadFile(sess.AuthTokenFile)
		if err != nil {
			return 0, nil, err
		}
		return protocol.AuthMethodToken, []byte(strings.TrimSpace(string(data))), nil
	}
	if sess.AuthToken != "" {
		return protocol.AuthMethodToken, []byte(sess.AuthToken), nil
	}
	if sess.TLSEnabled {
		return protocol.AuthMethodMTLS, nil, nil
	}
	return protocol.AuthMethodNone, nil, nil
}

// clientTLSConfig builds the shutdown dialer's TLS setup: the client cert authenticates against the CA bundle and is trusted as the server's root.
func clientTLSConfig(sess spec.Session) (*tls.Config, error) {
	if sess.TLSCertFile == "" || sess.TLSKeyFile == "" {
		return nil, fmt.Errorf("tls_enabled needs tls_cert_file and tls_key_file")
	}
	pair, err := tls.LoadX509KeyPair(sess.TLSCertFile, sess.TLSKeyFile)
	if err != nil {
		return nil, err
	}
	caFile := sess.TLSClientCaFile
	if caFile == "" {
		caFile = sess.TLSCertFile
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
