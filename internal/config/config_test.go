package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	assert.Equal(t, "tcp://127.0.0.1:7420", cfg.Server.Target)
	assert.Equal(t, "./data", cfg.Server.DataDir)
	assert.Equal(t, 256, cfg.Engine.BatchSize)
	assert.Equal(t, 10*time.Millisecond, cfg.Engine.FlushInterval)
	assert.Equal(t, int64(64*1024*1024), cfg.Engine.MaxSegmentBytes)
}

func TestLoadBootstrapConfig(t *testing.T) {
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "mikyu.yaml")
	os.WriteFile(cfgPath, []byte(`
server:
  target: "tcp://127.0.0.1:9999"
engine:
  batch_size: 100
`), 0644)

	cfg, path, err := Load(cfgPath)
	require.NoError(t, err)
	assert.Equal(t, "tcp://127.0.0.1:9999", cfg.Server.Target)
	assert.Equal(t, 100, cfg.Engine.BatchSize)
	assert.Equal(t, cfgPath, path)
}

func TestLoadAutoBootstrapWhenMissing(t *testing.T) {
	oldHome := os.Getenv("HOME")
	os.Setenv("HOME", t.TempDir())
	defer os.Setenv("HOME", oldHome)

	cfg, path, err := Load("")
	require.NoError(t, err)
	assert.NotEmpty(t, path)
	assert.NotNil(t, cfg)
}

func TestEnvOverrides(t *testing.T) {
	oldVal := os.Getenv("MIKYU_SERVER_TARGET")
	os.Setenv("MIKYU_SERVER_TARGET", "tcp://0.0.0.0:1234")
	defer os.Setenv("MIKYU_SERVER_TARGET", oldVal)

	cfg := DefaultConfig()
	applyEnvOverrides(&cfg)
	assert.Equal(t, "tcp://0.0.0.0:1234", cfg.Server.Target)
}

func TestToQueueEngineConfig(t *testing.T) {
	defaults := EngineConfig{
		BatchSize:       100,
		FlushInterval:   5 * time.Millisecond,
		MaxSegmentBytes: 1024,
	}
	q := QueueConfig{
		MaxSegmentBytes: 2048,
	}
	cfg := ToQueueEngineConfig(defaults, q)
	assert.Equal(t, 100, cfg.BatchSize)               // inherited
	assert.Equal(t, int64(2048), cfg.MaxSegmentBytes) // overridden
}
