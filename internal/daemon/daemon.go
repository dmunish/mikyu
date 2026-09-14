// Package daemon implements the background server daemon.
package daemon

import (
	"bytes"
	"context"
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

	n, release, err := node.Acquire(cfg.Server.DataDir, cfg.Defaults)
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
	for name, qSpec := range cfg.Queues {
		if err := n.CreateQueue(bgCtx, name, qSpec); err != nil && !errors.Is(err, engine.ErrQueueExists) {
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
		tlsCfg, err := server.DefaultTLSConfig(cfg.Server.TLSCertFile, cfg.Server.TLSKeyFile)
		if err != nil {
			return fmt.Errorf("TLS setup: %w", err)
		}
		srvOpts = append(srvOpts, server.WithTLS(tlsCfg))
	}

	srv := server.New(n, srvOpts...)

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

	return cfg, resolvedPath, nil
}

// logEffectiveConfig records the fully-resolved settings so the log of a
// running daemon always shows what it actually started with, regardless of
// which layer (flag, env, file, default) each value came from.
func logEffectiveConfig(cfg *config.Config, resolvedPath string) {
	log.Printf("mikyu daemon: config file %s", resolvedPath)
	log.Printf("mikyu daemon: server target=%s data_dir=%s tls_enabled=%t queues=%d",
		cfg.Server.Target, cfg.Server.DataDir, cfg.Server.TLSEnabled, len(cfg.Queues))
	defaults, err := json.Marshal(cfg.Defaults)
	if err != nil {
		defaults = []byte("{}")
	}
	log.Printf("mikyu daemon: defaults %s", defaults)
}

// EnsureDaemonRunning checks if a daemon is alive, and if not, spawns one.
func EnsureDaemonRunning(target string) error {
	if IsDaemonAlive(target) {
		return nil
	}
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	cmd := exec.Command(executable, "daemon")
	setDetachedProcessAttr(cmd)
	if err := cmd.Start(); err != nil {
		return err
	}
	// Wait up to 3 seconds for daemon to come online.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if IsDaemonAlive(target) {
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

// SendShutdownSignal sends an OpShutdown frame to the daemon.
func SendShutdownSignal(target string) error {
	network, addr, err := protocol.ParseTarget(target)
	if err != nil {
		return err
	}
	conn, err := net.DialTimeout(network, addr, 2*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()
	if err := protocol.WriteFrame(conn, protocol.OpShutdown, nil); err != nil {
		return err
	}
	return nil
}
