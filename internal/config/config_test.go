package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dmunish/mikyu/internal/spec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	assert.Equal(t, "tcp://127.0.0.1:7420", cfg.Server.Target)
	assert.Equal(t, "./data", cfg.Server.DataDir)
	assert.Equal(t, 10, cfg.Server.MaxConns)
	assert.Equal(t, spec.Spec{}, cfg.Defaults)
	assert.Empty(t, cfg.Queues)
}

func TestLoadDefaultsAndQueues(t *testing.T) {
	cfgPath := filepath.Join(t.TempDir(), "mikyu.yaml")
	require.NoError(t, os.WriteFile(cfgPath, []byte(`
server:
  target: "tcp://127.0.0.1:9999"
  max_conns: 4
defaults:
  engine:
    batch_size: 100
  retention:
    age: "72h"
queues:
  payments:
    engine:
      fsync: false
`), 0o644))

	cfg, path, err := Load(cfgPath)
	require.NoError(t, err)
	assert.Equal(t, cfgPath, path)
	assert.Equal(t, "tcp://127.0.0.1:9999", cfg.Server.Target)
	assert.Equal(t, 4, cfg.Server.MaxConns)

	require.NotNil(t, cfg.Defaults.Engine.BatchSize)
	assert.Equal(t, 100, *cfg.Defaults.Engine.BatchSize)
	require.NotNil(t, cfg.Defaults.Retention.Age)
	assert.Equal(t, 72*time.Hour, cfg.Defaults.Retention.Age.Duration())

	payments, ok := cfg.Queues["payments"]
	require.True(t, ok)
	require.NotNil(t, payments.Engine.Fsync)
	assert.False(t, *payments.Engine.Fsync)
	// The queue entry is sparse: unset fields inherit the session layer.
	assert.Nil(t, payments.Engine.BatchSize)
}

func TestLoadAutoBootstrapWhenMissing(t *testing.T) {
	// The default config resolves next to the binary; under go test that is a
	// throwaway temp dir, so nothing user-visible is ever touched.
	exe, err := os.Executable()
	require.NoError(t, err)
	wantPath := filepath.Join(filepath.Dir(exe), "mikyu.yaml")

	cfg, path, err := Load("")
	require.NoError(t, err)
	assert.Equal(t, wantPath, path)
	assert.NotNil(t, cfg)
	assert.Equal(t, 256, *cfg.Defaults.Engine.BatchSize)
}

func TestEnvOverrides(t *testing.T) {
	t.Setenv("MIKYU_TARGET", "tcp://0.0.0.0:1234")
	t.Setenv("MIKYU_BATCH_SIZE", "150")

	cfg := DefaultConfig()
	require.NoError(t, applyEnvOverrides(&cfg))
	assert.Equal(t, "tcp://0.0.0.0:1234", cfg.Server.Target)
	require.NotNil(t, cfg.Defaults.Engine.BatchSize)
	assert.Equal(t, 150, *cfg.Defaults.Engine.BatchSize)
}

func TestLegacyEnvVarsHardError(t *testing.T) {
	t.Setenv("MIKYU_ENGINE_BATCH_SIZE", "150")

	cfg := DefaultConfig()
	err := applyEnvOverrides(&cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "MIKYU_ENGINE_BATCH_SIZE")
	assert.Contains(t, err.Error(), "MIKYU_BATCH_SIZE")
}

func TestConfigOldKeyHardError(t *testing.T) {
	cfgPath := filepath.Join(t.TempDir(), "mikyu.yaml")
	require.NoError(t, os.WriteFile(cfgPath, []byte("engine:\n  max_segment_bytes: 1024\n"), 0o644))

	_, _, err := Load(cfgPath)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "max_segment_bytes")
	assert.Contains(t, err.Error(), "segment_size")
}

// Every session-layer parameter in spec.Table must be reachable from its env var.
func TestConfigEnvOverridesFromTable(t *testing.T) {
	values := map[string]string{
		"target":                       "tcp://10.0.0.1:7000",
		"data_dir":                     "D:/mikyu-data",
		"tls_enabled":                  "true",
		"tls_cert_file":                "cert.pem",
		"tls_key_file":                 "key.pem",
		"max_conns":                    "7",
		"metrics_enabled":              "true",
		"metrics_interval":             "2s",
		"metrics_history":              "50",
		"metrics_listen":               "127.0.0.1:9090",
		"metrics_per_queue_prometheus": "true",
		"metrics_histograms":           "true",
	}
	for _, p := range spec.Params(spec.LayerSession) {
		t.Run(p.Key, func(t *testing.T) {
			t.Setenv(p.EnvName, values[p.Key])

			cfg := DefaultConfig()
			require.NoError(t, applyEnvOverrides(&cfg))

			switch p.Key {
			case "target":
				assert.Equal(t, values["target"], cfg.Server.Target)
			case "data_dir":
				assert.Equal(t, values["data_dir"], cfg.Server.DataDir)
			case "tls_enabled":
				assert.True(t, cfg.Server.TLSEnabled)
			case "tls_cert_file":
				assert.Equal(t, values["tls_cert_file"], cfg.Server.TLSCertFile)
			case "tls_key_file":
				assert.Equal(t, values["tls_key_file"], cfg.Server.TLSKeyFile)
			case "max_conns":
				assert.Equal(t, 7, cfg.Server.MaxConns)
			case "metrics_enabled":
				assert.True(t, cfg.Server.MetricsEnabled)
			case "metrics_interval":
				assert.Equal(t, 2*time.Second, cfg.Server.MetricsInterval.Duration())
			case "metrics_history":
				assert.Equal(t, 50, cfg.Server.MetricsHistory)
			case "metrics_listen":
				assert.Equal(t, values["metrics_listen"], cfg.Server.MetricsListen)
			case "metrics_per_queue_prometheus":
				assert.True(t, cfg.Server.MetricsPerQueuePrometheus)
			case "metrics_histograms":
				assert.True(t, cfg.Server.MetricsHistograms)
			default:
				t.Fatalf("session parameter %q has no assertion", p.Key)
			}
		})
	}
}
