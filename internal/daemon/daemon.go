// Package daemon implements the background server daemon.
package daemon

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"time"

	"github.com/dmunish/mikyu/internal/config"
	"github.com/dmunish/mikyu/internal/engine"
	"github.com/dmunish/mikyu/internal/protocol"
	"github.com/dmunish/mikyu/internal/server"
)

// Run starts the daemon server and blocks until shutdown.
func Run(configPath string) error {
	cfg, _, err := config.Load(configPath)
	if err != nil {
		return err
	}

	engineCfg := config.ToEngineConfig(cfg.Engine)
	mgr, err := engine.OpenManager(cfg.Server.DataDir, engineCfg)
	if err != nil {
		return err
	}
	defer mgr.Close()

	// Reconcile declared queues.
	for _, qc := range cfg.Queues {
		qCfg := config.ToQueueEngineConfig(cfg.Engine, qc)
		if _, err := mgr.CreateQueue(qc.Name, qCfg); err != nil {
			return fmt.Errorf("reconcile queue %q: %w", qc.Name, err)
		}
	}

	var srvOpts []server.ServerOption
	if cfg.Server.TLSEnabled {
		tlsCfg, err := server.DefaultTLSConfig(cfg.Server.TLSCertFile, cfg.Server.TLSKeyFile)
		if err != nil {
			return err
		}
		srvOpts = append(srvOpts, server.WithTLS(tlsCfg))
	}

	srv := server.New(mgr, srvOpts...)
	go func() {
		// Serve on the configured target.
		if err := srv.ListenAndServe(cfg.Server.Target); err != nil {
			fmt.Fprintf(os.Stderr, "server error: %v\n", err)
			os.Exit(1)
		}
	}()

	// Wait for signals or server shutdown.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-sigCh:
	case <-srv.Done():
	}

	srv.Stop()
	return nil
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
