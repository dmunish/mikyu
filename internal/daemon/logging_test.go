package daemon

import (
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dmunish/mikyu/internal/logtap"
	"github.com/dmunish/mikyu/internal/spec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// captureStderr redirects the process stderr to a temp file for the duration
// of fn and returns what was written.
func captureStderr(t *testing.T, fn func()) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "stderr")
	f, err := os.Create(path)
	require.NoError(t, err)
	old := os.Stderr
	os.Stderr = f
	fn()
	os.Stderr = old
	_ = f.Close()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	return string(data)
}

func TestSetupLoggingFansOutToTap(t *testing.T) {
	tap := logtap.New(8)
	t.Cleanup(func() { setupLogging(spec.Session{}, nil) })

	captureStderr(t, func() {
		setupLogging(spec.Session{LogLevel: "debug"}, tap)
		slog.Info("hello world", "key", "value")
		slog.Debug("debug detail")
	})

	lines := tap.Snapshot()
	require.Len(t, lines, 2, "both records reach the ring in log order")
	assert.Contains(t, string(lines[0]), "hello world")
	assert.Contains(t, string(lines[0]), "key=value", "the plain line carries the fields")
	assert.Contains(t, string(lines[1]), "debug detail")
}

func TestSetupLoggingRespectsLevel(t *testing.T) {
	tap := logtap.New(8)
	t.Cleanup(func() { setupLogging(spec.Session{}, nil) })

	setupLogging(spec.Session{LogLevel: "warn"}, tap)
	slog.Info("not this one")
	slog.Warn("but this one")

	lines := tap.Snapshot()
	require.Len(t, lines, 1)
	assert.True(t, strings.Contains(string(lines[0]), "but this one"))
	assert.False(t, strings.Contains(string(lines[0]), "not this one"))
}

func TestSetupLoggingNilTapKeepsTerminalOnly(t *testing.T) {
	t.Cleanup(func() { setupLogging(spec.Session{}, nil) })

	stderr := captureStderr(t, func() {
		setupLogging(spec.Session{}, nil)
		slog.Info("terminal line")
	})
	assert.Contains(t, stderr, "terminal line")
}
