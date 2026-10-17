package daemon

import (
	"bytes"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dmunish/mikyu/internal/spec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLogsReplaysTheRing(t *testing.T) {
	target := "tcp://" + freeTCPAddr(t)
	finish := startDaemon(t, target, "--target", target, "--data-dir", filepath.Join(t.TempDir(), "data"))
	defer finish()

	// setupLogging happens before the daemon reports serving, so a caller
	// past IsDaemonAlive logs straight into the node's ring.
	slog.Info("ring line one")
	slog.Info("ring line two")

	var stdout, stderr bytes.Buffer
	require.NoError(t, runLogs(nil, []string{"--target", target}, &stdout, &stderr))
	assert.Contains(t, stdout.String(), "ring line one")
	assert.Contains(t, stdout.String(), "ring line two")
}

func TestLogsFollowsUntilTheDaemonStops(t *testing.T) {
	target := "tcp://" + freeTCPAddr(t)
	finish := startDaemon(t, target, "--target", target, "--data-dir", filepath.Join(t.TempDir(), "data"))

	stdout := &syncBuffer{}
	done := make(chan error, 1)
	go func() { done <- runLogs(nil, []string{"--target", target, "--follow"}, stdout, io.Discard) }()

	// Wait for the follower to attach, then log a line the client must see.
	time.Sleep(200 * time.Millisecond)
	slog.Info("followed line")

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && !strings.Contains(stdout.String(), "followed line") {
		time.Sleep(25 * time.Millisecond)
	}
	assert.True(t, strings.Contains(stdout.String(), "followed line"), "the followed line never reached the client")

	// Stopping the daemon ends the follow stream cleanly.
	require.NoError(t, SendShutdownSignal(target, spec.BuiltinSession()))
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("runLogs did not return after the daemon stopped")
	}
	finish()
}

func TestLogsDeadTarget(t *testing.T) {
	var stdout, stderr bytes.Buffer
	err := runLogs(nil, []string{"--target", "tcp://127.0.0.1:1"}, &stdout, &stderr)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no daemon serving tcp://127.0.0.1:1")
}

func TestLogsHelpPrintsUsage(t *testing.T) {
	var stdout, stderr bytes.Buffer
	err := runLogs(nil, []string{"--help"}, &stdout, &stderr)
	require.NoError(t, err)
	assert.Contains(t, stdout.String(), "Usage: mikyu daemon logs [flags]")
	assert.Contains(t, stdout.String(), "-follow")
}

// syncBuffer is a concurrency-safe byte sink for a streaming goroutine.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (w *syncBuffer) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}

func (w *syncBuffer) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.String()
}
