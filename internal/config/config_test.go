package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dmunish/mikyu/internal/manager"
	"github.com/dmunish/mikyu/internal/spec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	assert.Equal(t, "tcp://127.0.0.1:7420", cfg.Server.Target)
	assert.Equal(t, "./data", cfg.Server.DataDir)
	assert.Equal(t, 10, cfg.Server.MaxConnections)
	assert.Equal(t, spec.Spec{}, cfg.Defaults)
	assert.Empty(t, cfg.Queues)
}

func TestLoadDefaultsAndQueues(t *testing.T) {
	cfgPath := filepath.Join(t.TempDir(), "mikyu.yaml")
	require.NoError(t, os.WriteFile(cfgPath, []byte(`
server:
  target: "tcp://127.0.0.1:9999"
  max_connections: 4
defaults:
  flush:
    max_bytes: 100
  retention:
    max_age: "72h"
queues:
  payments:
    kind: log
    spec:
      storage:
        durability: async
`), 0o644))

	cfg, path, err := Load(cfgPath)
	require.NoError(t, err)
	assert.Equal(t, cfgPath, path)
	assert.Equal(t, "tcp://127.0.0.1:9999", cfg.Server.Target)
	assert.Equal(t, 4, cfg.Server.MaxConnections)

	require.NotNil(t, cfg.Defaults.Flush.MaxBytes)
	assert.Equal(t, int64(100), *cfg.Defaults.Flush.MaxBytes)
	require.NotNil(t, cfg.Defaults.Retention.MaxAge)
	assert.Equal(t, 72*time.Hour, cfg.Defaults.Retention.MaxAge.Duration())

	payments, ok := cfg.Queues["payments"]
	require.True(t, ok)
	assert.Equal(t, manager.KindLog, payments.Kind)
	require.NotNil(t, payments.Spec.Storage.Durability)
	assert.Equal(t, spec.DurabilityAsync, *payments.Spec.Storage.Durability)
	// The queue entry is sparse: unset fields inherit the session layer.
	assert.Nil(t, payments.Spec.Flush.MaxBytes)
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
	require.NotNil(t, cfg)
	assert.Equal(t, int64(1<<20), *cfg.Defaults.Flush.MaxBytes)
}

func TestEnvOverrides(t *testing.T) {
	t.Setenv("MIKYU_TARGET", "tcp://0.0.0.0:1234")
	t.Setenv("MIKYU_MAX_FLUSH_BYTES", "150")

	cfg := DefaultConfig()
	require.NoError(t, applyEnvOverrides(&cfg))
	assert.Equal(t, "tcp://0.0.0.0:1234", cfg.Server.Target)
	require.NotNil(t, cfg.Defaults.Flush.MaxBytes)
	assert.Equal(t, int64(150), *cfg.Defaults.Flush.MaxBytes)
}

// TestLegacyEnvVarsHardError walks every entry of legacyEnvVars: setting the
// old variable must fail the load with a message naming the replacement.
func TestLegacyEnvVarsHardError(t *testing.T) {
	for legacy, next := range legacyEnvVars {
		t.Run(legacy, func(t *testing.T) {
			t.Setenv(legacy, "150")

			cfg := DefaultConfig()
			err := applyEnvOverrides(&cfg)
			require.Error(t, err)
			assert.Contains(t, err.Error(), legacy)
			assert.Contains(t, err.Error(), "no longer supported")
			assert.Contains(t, err.Error(), strings.Fields(next)[0])
		})
	}
}

// TestConfigOldKeyHardError walks every entry of legacyYAMLKeys: a config file
// carrying the old key must fail the load with a message naming the replacement.
func TestConfigOldKeyHardError(t *testing.T) {
	for old, next := range legacyYAMLKeys {
		t.Run(old, func(t *testing.T) {
			cfgPath := filepath.Join(t.TempDir(), "mikyu.yaml")
			body := "defaults:\n  storage:\n    " + old + ": 1024\n"
			require.NoError(t, os.WriteFile(cfgPath, []byte(body), 0o644))

			_, _, err := Load(cfgPath)
			require.Error(t, err)
			assert.Contains(t, err.Error(), old)
			assert.Contains(t, err.Error(), "was renamed to")
			assert.Contains(t, err.Error(), strings.Fields(next)[0])
		})
	}
}

// Every session-layer parameter in spec.Table must be reachable from its env var.
func TestConfigEnvOverridesFromTable(t *testing.T) {
	values := map[string]string{
		"target":                       "tcp://10.0.0.1:7000",
		"data_dir":                     "D:/mikyu-data",
		"tls_enabled":                  "true",
		"tls_cert":                     "cert.pem",
		"tls_key":                      "key.pem",
		"max_connections":              "7",
		"metrics_enabled":              "true",
		"metrics_interval":             "2s",
		"metrics_history":              "50",
		"metrics_listen":               "127.0.0.1:9090",
		"metrics_per_queue_prometheus": "true",
		"metrics_histograms":           "true",
		"auth_method":                  "none",
		"auth_token_file":              "token.txt",
		"auth_token":                   "0123456789abcdef",
		"tls_client_ca":                "ca.pem",
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
			case "max_connections":
				assert.Equal(t, 7, cfg.Server.MaxConnections)
			case "tls_enabled":
				assert.True(t, cfg.Server.TLS.Enabled)
			case "tls_cert":
				assert.Equal(t, values["tls_cert"], cfg.Server.TLS.Cert)
			case "tls_key":
				assert.Equal(t, values["tls_key"], cfg.Server.TLS.Key)
			case "tls_client_ca":
				assert.Equal(t, values["tls_client_ca"], cfg.Server.TLS.ClientCA)
			case "auth_method":
				assert.Equal(t, values["auth_method"], cfg.Server.Auth.Method)
			case "auth_token":
				assert.Equal(t, values["auth_token"], cfg.Server.Auth.Token)
			case "auth_token_file":
				assert.Equal(t, values["auth_token_file"], cfg.Server.Auth.TokenFile)
			case "metrics_enabled":
				assert.True(t, cfg.Server.Metrics.Enabled)
			case "metrics_interval":
				assert.Equal(t, 2*time.Second, cfg.Server.Metrics.Interval.Duration())
			case "metrics_history":
				assert.Equal(t, 50, cfg.Server.Metrics.History)
			case "metrics_listen":
				assert.Equal(t, values["metrics_listen"], cfg.Server.Metrics.Listen)
			case "metrics_per_queue_prometheus":
				assert.True(t, cfg.Server.Metrics.PerQueuePrometheus)
			case "metrics_histograms":
				assert.True(t, cfg.Server.Metrics.Histograms)
			default:
				t.Fatalf("session parameter %q has no assertion", p.Key)
			}
		})
	}
}

func TestLoadRejectsInvalidSession(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mikyu.yaml")
	require.NoError(t, os.WriteFile(path, []byte("server:\n  metrics:\n    enabled: true\n    interval: \"0s\"\n"), 0o644))

	_, _, err := Load(path)
	require.ErrorIs(t, err, spec.ErrInvalidConfig)
	assert.Contains(t, err.Error(), "metrics_interval")
}

func TestLoadRejectsInvalidDefaults(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mikyu.yaml")
	body := "defaults:\n  storage:\n    max_payload: 2000000\n    max_segment: 1000000\n"
	require.NoError(t, os.WriteFile(path, []byte(body), 0o644))

	_, _, err := Load(path)
	require.ErrorIs(t, err, spec.ErrInvalidConfig)
	assert.Contains(t, err.Error(), "max_payload")
	assert.Contains(t, err.Error(), "max_segment")
}

func TestBootstrapWriteFailureHintsConfigFlag(t *testing.T) {
	orig := executablePath
	t.Cleanup(func() { executablePath = orig })
	executablePath = func() (string, error) {
		return filepath.Join(t.TempDir(), "missing-dir", "mikyu"), nil
	}

	_, _, err := Load("")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--config")
}
