package node

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/dmunish/mikyu/internal/spec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRegistryRoundTrip(t *testing.T) {
	dir := t.TempDir()
	r := &registry{
		Version: 1,
		Queues: map[string]spec.Spec{
			"payments": {Engine: spec.EngineSpec{BatchSize: new(7)}},
		},
	}
	require.NoError(t, saveRegistry(dir, r))

	loaded, err := loadRegistry(dir)
	require.NoError(t, err)
	require.NotNil(t, loaded.Queues["payments"].Engine.BatchSize)
	assert.Equal(t, 7, *loaded.Queues["payments"].Engine.BatchSize)
}

func TestRegistryMissingFileIsEmpty(t *testing.T) {
	r, err := loadRegistry(t.TempDir())
	require.NoError(t, err)
	assert.Empty(t, r.Queues)
}

func TestRegistryCorruptRejects(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, registryFileName), []byte("{not json"), 0o644))
	_, err := loadRegistry(dir)
	require.ErrorIs(t, err, ErrRegistryCorrupt)
}

func TestRegistryRejectsUnknownFields(t *testing.T) {
	dir := t.TempDir()
	body, _ := json.Marshal(map[string]any{
		"version": 1,
		"queues":  map[string]any{},
		"extra":   "field",
	})
	require.NoError(t, os.WriteFile(filepath.Join(dir, registryFileName), body, 0o644))
	_, err := loadRegistry(dir)
	require.ErrorIs(t, err, ErrRegistryCorrupt)
}
