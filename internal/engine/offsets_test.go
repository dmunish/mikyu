package engine

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOffsetStoreCommitFetch(t *testing.T) {
	dir := t.TempDir()
	store, err := openOffsetStore(dir)
	require.NoError(t, err)
	defer store.Close()

	err = store.Commit("queue1", "group1", 42)
	require.NoError(t, err)
	err = store.Commit("queue1", "group2", 100)
	require.NoError(t, err)

	assert.Equal(t, uint64(42), store.Fetch("queue1", "group1"))
	assert.Equal(t, uint64(100), store.Fetch("queue1", "group2"))
	assert.Equal(t, uint64(0), store.Fetch("queue1", "nonexistent"))
}

func TestOffsetStoreRecovery(t *testing.T) {
	dir := t.TempDir()
	store, err := openOffsetStore(dir)
	require.NoError(t, err)
	_ = store.Commit("q", "g", 5)
	_ = store.Close()

	store2, err := openOffsetStore(dir)
	require.NoError(t, err)
	defer store2.Close()
	assert.Equal(t, uint64(5), store2.Fetch("q", "g"))
}

func TestOffsetStoreTornRecord(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, managerOffsetFileName)
	// Write a valid record, then a torn one
	content := make([]byte, 0)
	// valid: key "q:g" offset 5
	key := []byte("q:g")
	content = append(content, byte(len(key)>>8), byte(len(key)))
	content = append(content, key...)
	content = append(content, 0, 0, 0, 0, 0, 0, 0, 5)
	// torn: just partial header
	content = append(content, 0, 2)
	os.WriteFile(path, content, 0644)

	store, err := openOffsetStore(dir)
	require.NoError(t, err)
	defer store.Close()
	// Only valid record should be loaded
	assert.Equal(t, uint64(5), store.Fetch("q", "g"))
}
