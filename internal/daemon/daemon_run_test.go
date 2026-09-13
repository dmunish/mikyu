package daemon

import (
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// freeTCPAddr binds an ephemeral port and releases it. Like every port-based
// test there is a theoretical reuse race; failing here is a loud error, not
// a hang, because Run reports bind failures as plain errors.
func freeTCPAddr(tb testing.TB) string {
	tb.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(tb, err)
	addr := ln.Addr().String()
	require.NoError(tb, ln.Close())
	return addr
}

// startDaemon launches Run in the background with the given args, waits for
// the daemon to accept connections on target, and returns a finish function
// that sends the OpShutdown frame and asserts a clean Run return. finish is
// also registered as a cleanup, so a test failure never strands the daemon.
func startDaemon(tb *testing.T, target string, args ...string) (finish func()) {
	tb.Helper()
	exited := make(chan error, 1)
	go func() { exited <- Run(args) }()

	dead := false // Run returned before ready; its result was already consumed.
	deadline := time.Now().Add(10 * time.Second)
	for !IsDaemonAlive(target) {
		select {
		case err := <-exited:
			dead = true
			tb.Fatalf("daemon exited before serving: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			tb.Fatal("daemon never came online")
		}
		time.Sleep(25 * time.Millisecond)
	}

	finished := false
	finish = func() {
		tb.Helper()
		if finished || dead {
			return
		}
		finished = true
		require.NoError(tb, SendShutdownSignal(target), "sending shutdown signal")
		select {
		case err := <-exited:
			assert.NoError(tb, err, "graceful shutdown must return nil, letting deferred cleanup run")
		case <-time.After(10 * time.Second):
			tb.Fatal("daemon did not shut down after the shutdown signal")
		}
	}
	tb.Cleanup(finish)
	return finish
}

// TestDaemonRunServesUntilShutdownSignal exercises the whole Run lifecycle:
// CLI flags layered over a config file, node open, listen, serve, and a
// graceful exit through an OpShutdown frame (the path "mikyu shutdown"
// drives).
func TestDaemonRunServesUntilShutdownSignal(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := writeConfig(t, tmp, "mikyu.yaml", "defaults:\n  engine:\n    batch_size: 100\n")
	target := "tcp://" + freeTCPAddr(t)

	startDaemon(t, target, "--config", cfgPath, "--target", target, "--data-dir", filepath.Join(tmp, "data"))()
}

// TestDaemonRunStartsWithoutConfigFile is the §7.5 bootstrap check: a bare
// daemon with no mikyu.yaml anywhere must still start and serve. OS config
// locations are redirected into the test dir so the bootstrap writes nothing
// to the real user profile.
func TestDaemonRunStartsWithoutConfigFile(t *testing.T) {
	tmp := t.TempDir()
	t.Chdir(tmp)
	t.Setenv("APPDATA", filepath.Join(tmp, "appdata"))
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(tmp, "xdg"))
	t.Setenv("HOME", filepath.Join(tmp, "home"))

	target := "tcp://" + freeTCPAddr(t)
	startDaemon(t, target, "--target", target, "--data-dir", filepath.Join(tmp, "data"))()
}

// TestDaemonRunBindFailureReturnsError guards the lifecycle bug in the
// original design: a failed bind must end Run with an error (after node
// cleanup) instead of exiting the process mid-defer or blocking forever.
func TestDaemonRunBindFailureReturnsError(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := writeConfig(t, tmp, "mikyu.yaml", "defaults:\n  engine:\n    batch_size: 100\n")

	// Hold the port so the daemon's bind fails.
	busy, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer busy.Close()

	done := make(chan error, 1)
	go func() {
		done <- Run([]string{"--config", cfgPath, "--target", "tcp://" + busy.Addr().String(), "--data-dir", filepath.Join(tmp, "data")})
	}()

	select {
	case err := <-done:
		require.Error(t, err)
		assert.Contains(t, err.Error(), "listen")
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after a bind failure")
	}
}

// TestDaemonRunRejectsUsageErrors keeps the fast paths fast: bad CLI input
// must fail before any node or listener is touched.
func TestDaemonRunRejectsUsageErrors(t *testing.T) {
	err := Run([]string{"--bogus-flag"})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrUsage)

	err = Run([]string{"--help"})
	assert.NoError(t, err, "--help is a successful no-op")
}
