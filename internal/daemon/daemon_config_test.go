package daemon

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// writeConfig drops a YAML file into dir and returns its path. Fields left
// out keep their compiled defaults: config.Load unmarshals onto them.
func writeConfig(tb testing.TB, dir, name, body string) string {
	tb.Helper()
	path := filepath.Join(dir, name)
	require.NoError(tb, os.WriteFile(path, []byte(body), 0o644))
	return path
}

// batchConfig writes a config whose session-default batch size is the given value.
func batchConfig(tb testing.TB, batch int) string {
	tb.Helper()
	return writeConfig(tb, tb.TempDir(), "mikyu.yaml", fmt.Sprintf("defaults:\n  engine:\n    batch_size: %d\n", batch))
}

func TestDaemonFlagsOverrideConfig(t *testing.T) {
	path := batchConfig(t, 100)

	cfg, _, err := resolveConfig([]string{"--config", path, "--batch-size", "200"}, io.Discard, io.Discard)
	require.NoError(t, err)
	assert.Equal(t, 200, *cfg.Defaults.Engine.BatchSize)
}

func TestDaemonFlagsRespectConfigDefaults(t *testing.T) {
	path := batchConfig(t, 100)

	cfg, resolvedPath, err := resolveConfig([]string{"--config", path}, io.Discard, io.Discard)
	require.NoError(t, err)
	assert.Equal(t, path, resolvedPath)
	assert.Equal(t, 100, *cfg.Defaults.Engine.BatchSize)
	// Untouched fields stay unset; the built-in layer supplies them at resolve time.
	assert.Nil(t, cfg.Defaults.Engine.MaxInterval)
	assert.Nil(t, cfg.Defaults.Engine.Fsync)
	assert.Equal(t, "tcp://127.0.0.1:7420", cfg.Server.Target)
}

func TestDaemonEnvThenFlagPrecedence(t *testing.T) {
	path := batchConfig(t, 100)
	t.Setenv("MIKYU_BATCH_SIZE", "150")

	// Environment overrides the config file...
	cfg, _, err := resolveConfig([]string{"--config", path}, io.Discard, io.Discard)
	require.NoError(t, err)
	assert.Equal(t, 150, *cfg.Defaults.Engine.BatchSize)

	// ...and explicit flags override the environment.
	cfg, _, err = resolveConfig([]string{"--config", path, "--batch-size", "200"}, io.Discard, io.Discard)
	require.NoError(t, err)
	assert.Equal(t, 200, *cfg.Defaults.Engine.BatchSize)
}

func TestDaemonBooleanFlagsOverrideBothDirections(t *testing.T) {
	dir := t.TempDir()
	offFile := writeConfig(t, dir, "off.yaml", "defaults:\n  engine:\n    fsync: false\n")
	onFile := writeConfig(t, dir, "on.yaml", "defaults:\n  engine:\n    fsync: true\n")

	// A bare --fsync sets true even when the file says false.
	cfg, _, err := resolveConfig([]string{"--config", offFile, "--fsync"}, io.Discard, io.Discard)
	require.NoError(t, err)
	require.NotNil(t, cfg.Defaults.Engine.Fsync)
	assert.True(t, *cfg.Defaults.Engine.Fsync)

	// --fsync=false overrides a true config value.
	cfg, _, err = resolveConfig([]string{"--config", onFile, "--fsync=false"}, io.Discard, io.Discard)
	require.NoError(t, err)
	require.NotNil(t, cfg.Defaults.Engine.Fsync)
	assert.False(t, *cfg.Defaults.Engine.Fsync)
}

func TestDaemonAllEngineFlagsReachConfig(t *testing.T) {
	path := batchConfig(t, 100)

	cfg, _, err := resolveConfig([]string{
		"--config", path,
		"--target", "tcp://127.0.0.1:9999",
		"--data-dir", "D:/elsewhere",
		"--tls-enabled",
		"--tls-cert-file", "cert.pem",
		"--tls-key-file", "key.pem",
		"--batch-size", "7",
		"--max-interval", "5s",
		"--min-interval", "50us",
		"--segment-size", "4096",
		"--max-message-bytes", "1024",
		"--segment-age", "2h",
		"--encrypted",
		"--fsync=false",
	}, io.Discard, io.Discard)
	require.NoError(t, err)

	assert.Equal(t, "tcp://127.0.0.1:9999", cfg.Server.Target)
	assert.Equal(t, "D:/elsewhere", cfg.Server.DataDir)
	assert.True(t, cfg.Server.TLSEnabled)
	assert.Equal(t, "cert.pem", cfg.Server.TLSCertFile)
	assert.Equal(t, "key.pem", cfg.Server.TLSKeyFile)
	assert.Equal(t, 7, *cfg.Defaults.Engine.BatchSize)
	assert.Equal(t, 5*time.Second, cfg.Defaults.Engine.MaxInterval.Duration())
	assert.Equal(t, 50*time.Microsecond, cfg.Defaults.Engine.MinInterval.Duration())
	assert.Equal(t, int64(4096), *cfg.Defaults.Engine.SegmentSize)
	assert.Equal(t, int64(1024), *cfg.Defaults.Engine.MaxMessageBytes)
	assert.Equal(t, 2*time.Hour, cfg.Defaults.Engine.SegmentAge.Duration())
	assert.True(t, *cfg.Defaults.Engine.Encrypted)
	assert.False(t, *cfg.Defaults.Engine.Fsync)
}

func TestMetricsFlagOverride(t *testing.T) {
	path := batchConfig(t, 100)

	cfg, _, err := resolveConfig([]string{
		"--config", path,
		"--metrics-enabled=false",
		"--metrics-interval", "5s",
		"--metrics-history", "42",
		"--metrics-listen", "127.0.0.1:9090",
		"--metrics-per-queue-prometheus",
		"--metrics-histograms",
	}, io.Discard, io.Discard)
	require.NoError(t, err)

	assert.False(t, cfg.Server.MetricsEnabled)
	assert.Equal(t, 5*time.Second, cfg.Server.MetricsInterval.Duration())
	assert.Equal(t, 42, cfg.Server.MetricsHistory)
	assert.Equal(t, "127.0.0.1:9090", cfg.Server.MetricsListen)
	assert.True(t, cfg.Server.MetricsPerQueuePrometheus)
	assert.True(t, cfg.Server.MetricsHistograms)
}

func TestMetricsListenFromConfigFile(t *testing.T) {
	cfgPath := writeConfig(t, t.TempDir(), "mikyu.yaml", "server:\n  metrics_listen: 127.0.0.1:9090\n")

	cfg, _, err := resolveConfig([]string{"--config", cfgPath}, io.Discard, io.Discard)
	require.NoError(t, err)
	assert.Equal(t, "127.0.0.1:9090", cfg.Server.MetricsListen)

	// An explicit flag still wins over the file.
	cfg, _, err = resolveConfig([]string{"--config", cfgPath, "--metrics-listen", "127.0.0.1:9091"}, io.Discard, io.Discard)
	require.NoError(t, err)
	assert.Equal(t, "127.0.0.1:9091", cfg.Server.MetricsListen)
}

func TestDaemonRejectsUnknownFlag(t *testing.T) {
	_, _, err := resolveConfig([]string{"--nope"}, io.Discard, io.Discard)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrUsage)
	assert.Contains(t, err.Error(), "nope")
}

func TestDaemonRejectsPositionalConfigPath(t *testing.T) {
	// Step 7 moved the config path from a positional argument to --config;
	// the old invocation must fail loudly with a hint, not silently ignore.
	var stderr bytes.Buffer
	_, _, err := resolveConfig([]string{"./mikyu.yaml"}, io.Discard, &stderr)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrUsage)
	assert.Contains(t, err.Error(), "./mikyu.yaml")
	// The recovery hint lives in the terminal report, alongside the usage
	// block — the wrapped error carries the strict fact.
	assert.Contains(t, stderr.String(), "unexpected argument")
	assert.Contains(t, stderr.String(), "--config")
	assert.Contains(t, stderr.String(), "Usage: mikyu daemon")
}

func TestDaemonRejectsInvalidFlagValues(t *testing.T) {
	path := batchConfig(t, 100)

	tests := []struct {
		name string
		args []string
	}{
		{"zero batch", []string{"--config", path, "--batch-size", "0"}},
		{"negative batch", []string{"--config", path, "--batch-size", "-5"}},
		{"zero max-interval", []string{"--config", path, "--max-interval", "0s"}},
		{"negative max-interval", []string{"--config", path, "--max-interval", "-1s"}},
		{"negative min-interval", []string{"--config", path, "--min-interval", "-1s"}},
		{"zero segment bytes", []string{"--config", path, "--segment-size", "0"}},
		{"negative message bytes", []string{"--config", path, "--max-message-bytes", "-1"}},
		{"zero segment age", []string{"--config", path, "--segment-age", "0s"}},
		{"bad target scheme", []string{"--config", path, "--target", "http://x"}},
		{"empty target", []string{"--config", path, "--target", ""}},
		{"empty data dir", []string{"--config", path, "--data-dir", ""}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := resolveConfig(tt.args, io.Discard, io.Discard)
			require.Error(t, err)
			assert.ErrorIs(t, err, ErrUsage)
		})
	}
}

func TestDaemonHelpListsEveryFlag(t *testing.T) {
	var stdout, stderr bytes.Buffer

	_, _, err := resolveConfig([]string{"--help"}, &stdout, &stderr)
	assert.ErrorIs(t, err, flag.ErrHelp)
	assert.Empty(t, stderr.String())

	help := stdout.String()
	assert.Contains(t, help, "Usage: mikyu daemon")
	for _, name := range []string{
		"config", "target", "data-dir",
		"tls-enabled", "tls-cert-file", "tls-key-file", "max-conns",
		"batch-size", "max-interval", "min-interval",
		"segment-size", "max-message-bytes", "segment-age", "max-conns",
		"encrypted", "fsync",
		"metrics-enabled", "metrics-interval", "metrics-history", "metrics-listen",
		"metrics-per-queue-prometheus", "metrics-histograms",
	} {
		assert.Contains(t, help, "-"+name, "help should list --%s", name)
	}
}

func TestDaemonBootstrapWithoutConfigFile(t *testing.T) {
	// The default config resolves next to the binary; under go test that is a
	// throwaway temp dir, so nothing user-visible is ever touched.
	exe, err := os.Executable()
	require.NoError(t, err)
	wantPath := filepath.Join(filepath.Dir(exe), "mikyu.yaml")
	dataDir := filepath.Join(t.TempDir(), "data")

	cfg, resolvedPath, err := resolveConfig([]string{"--data-dir", dataDir}, io.Discard, io.Discard)
	require.NoError(t, err)
	assert.FileExists(t, resolvedPath)
	assert.Equal(t, wantPath, resolvedPath)
	assert.Equal(t, 256, *cfg.Defaults.Engine.BatchSize) // bootstrapped default survived
	assert.Equal(t, dataDir, cfg.Server.DataDir)

	// Flags still win over the freshly bootstrapped file.
	cfg, _, err = resolveConfig([]string{"--data-dir", dataDir, "--batch-size", "777"}, io.Discard, io.Discard)
	require.NoError(t, err)
	assert.Equal(t, 777, *cfg.Defaults.Engine.BatchSize)
}
