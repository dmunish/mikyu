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

	"github.com/dmunish/mikyu/internal/spec"
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

// flushConfig writes a config whose session-default flush budget is the given value.
func flushConfig(tb testing.TB, n int) string {
	tb.Helper()
	return writeConfig(tb, tb.TempDir(), "mikyu.yaml", fmt.Sprintf("defaults:\n  flush:\n    max_bytes: %d\n", n))
}

func TestDaemonFlagsOverrideConfig(t *testing.T) {
	path := flushConfig(t, 100)

	cfg, _, err := resolveConfig([]string{"--config", path, "--max-flush-bytes", "200"}, io.Discard, io.Discard)
	require.NoError(t, err)
	assert.Equal(t, int64(200), *cfg.Defaults.Flush.MaxBytes)
}

func TestDaemonFlagsRespectConfigDefaults(t *testing.T) {
	path := flushConfig(t, 100)

	cfg, resolvedPath, err := resolveConfig([]string{"--config", path}, io.Discard, io.Discard)
	require.NoError(t, err)
	assert.Equal(t, path, resolvedPath)
	assert.Equal(t, int64(100), *cfg.Defaults.Flush.MaxBytes)
	// Untouched fields stay unset; the built-in layer supplies them at resolve time.
	assert.Nil(t, cfg.Defaults.Flush.MaxWait)
	assert.Nil(t, cfg.Defaults.Storage.Durability)
	assert.Equal(t, "tcp://127.0.0.1:7420", cfg.Server.Target)
}

func TestDaemonEnvThenFlagPrecedence(t *testing.T) {
	path := flushConfig(t, 100)
	t.Setenv("MIKYU_MAX_FLUSH_BYTES", "150")

	// Environment overrides the config file...
	cfg, _, err := resolveConfig([]string{"--config", path}, io.Discard, io.Discard)
	require.NoError(t, err)
	assert.Equal(t, int64(150), *cfg.Defaults.Flush.MaxBytes)

	// ...and explicit flags override the environment.
	cfg, _, err = resolveConfig([]string{"--config", path, "--max-flush-bytes", "200"}, io.Discard, io.Discard)
	require.NoError(t, err)
	assert.Equal(t, int64(200), *cfg.Defaults.Flush.MaxBytes)
}

func TestDaemonEnumFlagsReachConfig(t *testing.T) {
	path := flushConfig(t, 100)

	cfg, _, err := resolveConfig([]string{
		"--config", path,
		"--durability", "async",
		"--encryption", "os",
		"--compaction", "keys",
	}, io.Discard, io.Discard)
	require.NoError(t, err)
	assert.Equal(t, spec.DurabilityAsync, *cfg.Defaults.Storage.Durability)
	assert.Equal(t, spec.EncryptionOS, *cfg.Defaults.Storage.Encryption)
	assert.Equal(t, spec.CompactionKeys, *cfg.Defaults.Storage.Compaction)

	cfg, _, err = resolveConfig([]string{"--config", path, "--encryption", "aes"}, io.Discard, io.Discard)
	require.NoError(t, err)
	assert.Equal(t, spec.EncryptionAES, *cfg.Defaults.Storage.Encryption)

	_, _, err = resolveConfig([]string{"--config", path, "--durability", "eventual"}, io.Discard, io.Discard)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrUsage)
}

func TestDaemonAllStorageFlagsReachConfig(t *testing.T) {
	path := flushConfig(t, 100)

	cfg, _, err := resolveConfig([]string{
		"--config", path,
		"--target", "tcp://127.0.0.1:9999",
		"--data-dir", "D:/elsewhere",
		"--tls-enabled",
		"--tls-cert", "cert.pem",
		"--tls-key", "key.pem",
		"--max-segment", "4096",
		"--max-segment-age", "2h",
		"--max-payload", "1024",
		"--max-cache", "8192",
		"--max-flush-bytes", "7",
		"--max-flush-wait", "5s",
	}, io.Discard, io.Discard)
	require.NoError(t, err)

	assert.Equal(t, "tcp://127.0.0.1:9999", cfg.Server.Target)
	assert.Equal(t, "D:/elsewhere", cfg.Server.DataDir)
	assert.True(t, cfg.Server.TLS.Enabled)
	assert.Equal(t, "cert.pem", cfg.Server.TLS.Cert)
	assert.Equal(t, "key.pem", cfg.Server.TLS.Key)
	assert.Equal(t, int64(4096), *cfg.Defaults.Storage.MaxSegment)
	assert.Equal(t, 2*time.Hour, cfg.Defaults.Storage.MaxSegmentAge.Duration())
	assert.Equal(t, int64(1024), *cfg.Defaults.Storage.MaxPayload)
	assert.Equal(t, int64(8192), *cfg.Defaults.Storage.MaxCache)
	assert.Equal(t, int64(7), *cfg.Defaults.Flush.MaxBytes)
	assert.Equal(t, 5*time.Second, cfg.Defaults.Flush.MaxWait.Duration())
}

func TestMetricsFlagOverride(t *testing.T) {
	path := flushConfig(t, 100)

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

	assert.False(t, cfg.Server.Metrics.Enabled)
	assert.Equal(t, 5*time.Second, cfg.Server.Metrics.Interval.Duration())
	assert.Equal(t, 42, cfg.Server.Metrics.History)
	assert.Equal(t, "127.0.0.1:9090", cfg.Server.Metrics.Listen)
	assert.True(t, cfg.Server.Metrics.PerQueuePrometheus)
	assert.True(t, cfg.Server.Metrics.Histograms)
}

func TestMetricsListenFromConfigFile(t *testing.T) {
	cfgPath := writeConfig(t, t.TempDir(), "mikyu.yaml", "server:\n  metrics:\n    listen: 127.0.0.1:9090\n")

	cfg, _, err := resolveConfig([]string{"--config", cfgPath}, io.Discard, io.Discard)
	require.NoError(t, err)
	assert.Equal(t, "127.0.0.1:9090", cfg.Server.Metrics.Listen)

	// An explicit flag still wins over the file.
	cfg, _, err = resolveConfig([]string{"--config", cfgPath, "--metrics-listen", "127.0.0.1:9091"}, io.Discard, io.Discard)
	require.NoError(t, err)
	assert.Equal(t, "127.0.0.1:9091", cfg.Server.Metrics.Listen)
}

func TestDaemonRejectsUnknownFlag(t *testing.T) {
	_, _, err := resolveConfig([]string{"--nope"}, io.Discard, io.Discard)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrUsage)
	assert.Contains(t, err.Error(), "nope")
}

func TestDaemonEncryptionKeyFile(t *testing.T) {
	cfg, _, err := resolveConfig([]string{"--encryption-key-file", "keys"}, io.Discard, io.Discard)
	require.NoError(t, err)
	assert.Equal(t, "keys", cfg.Server.Encryption.KeyFile)

	var stderr bytes.Buffer
	_, _, err = resolveConfig([]string{"--encryption-key", "0123456789abcdef"}, io.Discard, &stderr)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrUsage)
	assert.Contains(t, stderr.String(), "--encryption-key-file")
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
	path := flushConfig(t, 100)

	tests := []struct {
		name string
		args []string
	}{
		{"zero flush bytes", []string{"--config", path, "--max-flush-bytes", "0"}},
		{"negative flush bytes", []string{"--config", path, "--max-flush-bytes", "-5"}},
		{"zero max-flush-wait", []string{"--config", path, "--max-flush-wait", "0s"}},
		{"negative max-flush-wait", []string{"--config", path, "--max-flush-wait", "-1s"}},
		{"zero segment bytes", []string{"--config", path, "--max-segment", "0"}},
		{"negative payload bytes", []string{"--config", path, "--max-payload", "-1"}},
		{"zero segment age", []string{"--config", path, "--max-segment-age", "0s"}},
		{"bad durability", []string{"--config", path, "--durability", "eventual"}},
		{"bad target scheme", []string{"--config", path, "--target", "http://x"}},
		{"empty target", []string{"--config", path, "--target", ""}},
		{"empty data dir", []string{"--config", path, "--data-dir", ""}},
		{"zero max-connections", []string{"--config", path, "--max-connections", "0"}},
		{"negative max-connections", []string{"--config", path, "--max-connections", "-1"}},
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
		"tls-enabled", "tls-cert", "tls-key", "max-connections",
		"durability", "encryption", "compaction",
		"max-segment", "max-segment-age", "max-payload", "max-cache",
		"max-flush-bytes", "max-flush-wait",
		"max-retention-age", "max-retention-bytes",
		"lease", "max-attempts", "max-in-flight", "max-dead-letters",
		"metrics-enabled", "metrics-interval", "metrics-history", "metrics-listen",
		"metrics-per-queue-prometheus", "metrics-histograms",
		"auth-method", "auth-token-file",
		"encryption-key-file",
	} {
		assert.Contains(t, help, "-"+name, "help should list --%s", name)
	}
}

func TestDaemonAuthFailClosedNonLoopbackNoAuth(t *testing.T) {
	_, _, err := resolveConfig([]string{"--target", "tcp://192.0.2.1:7420"}, io.Discard, io.Discard)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrUsage)
}

func TestDaemonAuthFailClosedTokenWithoutTLS(t *testing.T) {
	_, _, err := resolveConfig([]string{"--target", "tcp://192.0.2.1:7420", "--auth-method", "token"}, io.Discard, io.Discard)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrUsage)
}

func TestDaemonAuthRejectsTokenFlag(t *testing.T) {
	_, _, err := resolveConfig([]string{"--auth-token", "secret"}, io.Discard, io.Discard)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrUsage)
}

func TestDaemonAuthMTLSRequiresCAFile(t *testing.T) {
	_, _, err := resolveConfig([]string{"--target", "tcp://192.0.2.1:7420", "--auth-method", "mtls", "--tls-enabled"}, io.Discard, io.Discard)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrUsage)
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
	assert.Equal(t, int64(1<<20), *cfg.Defaults.Flush.MaxBytes) // bootstrapped default survived
	assert.Equal(t, dataDir, cfg.Server.DataDir)

	// Flags still win over the freshly bootstrapped file.
	cfg, _, err = resolveConfig([]string{"--data-dir", dataDir, "--max-flush-bytes", "777"}, io.Discard, io.Discard)
	require.NoError(t, err)
	assert.Equal(t, int64(777), *cfg.Defaults.Flush.MaxBytes)
}

func TestSplitSubcommand(t *testing.T) {
	for _, tc := range []struct {
		args []string
		sub  string
		rest []string
	}{
		{args: nil, sub: "", rest: nil},
		{args: []string{"--help"}, sub: "", rest: []string{"--help"}},
		{args: []string{"--data-dir", "d"}, sub: "", rest: []string{"--data-dir", "d"}},
		{args: []string{"run"}, sub: "run", rest: []string{}},
		{args: []string{"status", "--json"}, sub: "status", rest: []string{"--json"}},
		{args: []string{"stop", "--yes"}, sub: "stop", rest: []string{"--yes"}},
	} {
		sub, rest := splitSubcommand(tc.args)
		assert.Equal(t, tc.sub, sub, "args: %v", tc.args)
		assert.Equal(t, tc.rest, rest, "args: %v", tc.args)
	}
}

func TestRunRejectsUnknownSubcommand(t *testing.T) {
	var stderr bytes.Buffer
	err := Run([]string{"frobnicate"}, &stderr)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrUsage)
	assert.Contains(t, stderr.String(), "unknown command")
	assert.Contains(t, stderr.String(), "frobnicate")
}
