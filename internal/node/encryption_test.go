package node

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"github.com/dmunish/mikyu/internal/engine"
	"github.com/dmunish/mikyu/internal/spec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func masterKeyFile(t *testing.T) string {
	t.Helper()
	var raw [32]byte
	_, err := rand.Read(raw[:])
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "keys")
	require.NoError(t, os.WriteFile(path, []byte(hex.EncodeToString(raw[:])+"\n"), 0o600))
	return path
}

func encryptedSpec() spec.Spec {
	return spec.Spec{
		Storage: spec.StorageSpec{
			Encryption: new(spec.EncryptionAES),
		},
	}
}

func TestEncryptedQueueEndToEnd(t *testing.T) {
	dir := t.TempDir()
	keyFile := masterKeyFile(t)

	n, release, err := Acquire(dir, spec.Spec{}, WithEncryptionKeyFile(keyFile))
	require.NoError(t, err)
	q, err := n.CreateLogQueue(t.Context(), "events", encryptedSpec())
	require.NoError(t, err)

	payload := []byte("sealed payload")
	off, err := q.Append(context.Background(), payload)
	require.NoError(t, err)

	// Task checkpoints are engine records, so sealed task queues must survive a restart too.
	tq, err := n.CreateTaskQueue(t.Context(), "jobs", encryptedSpec())
	require.NoError(t, err)
	task, err := tq.Enqueue(context.Background(), []byte("sealed task"))
	require.NoError(t, err)
	require.NoError(t, release())

	// Reopen with the same key file: records decrypt.
	n, release, err = Acquire(dir, spec.Spec{}, WithEncryptionKeyFile(keyFile))
	require.NoError(t, err)
	defer func() { require.NoError(t, release()) }()
	q, err = n.LogQueue("events")
	require.NoError(t, err)
	rec, err := q.Read(context.Background(), off)
	require.NoError(t, err)
	assert.Equal(t, payload, rec.Payload)

	tq, err = n.TaskQueue("jobs")
	require.NoError(t, err)
	got, err := tq.Dequeue(context.Background())
	require.NoError(t, err)
	assert.Equal(t, task.ID, got.ID)
	assert.Equal(t, []byte("sealed task"), got.Payload)
}

func TestEncryptedQueueRequiresKeyFile(t *testing.T) {
	dir := t.TempDir()
	keyFile := masterKeyFile(t)

	n, release, err := Acquire(dir, spec.Spec{}, WithEncryptionKeyFile(keyFile))
	require.NoError(t, err)
	_, err = n.CreateLogQueue(t.Context(), "events", encryptedSpec())
	require.NoError(t, err)
	require.NoError(t, release())

	// Without the key file the sealed queue refuses to open.
	_, _, err = Acquire(dir, spec.Spec{})
	require.ErrorIs(t, err, engine.ErrEncryptionKey)

	// A fresh create without keys fails closed too.
	n2, release2, err := Acquire(t.TempDir(), spec.Spec{})
	require.NoError(t, err)
	defer func() { require.NoError(t, release2()) }()
	_, err = n2.CreateLogQueue(t.Context(), "events", encryptedSpec())
	require.ErrorIs(t, err, engine.ErrEncryptionKey)
}

func TestAcquireRejectsKeyFileMismatch(t *testing.T) {
	dir := t.TempDir()
	_, release, err := Acquire(dir, spec.Spec{}, WithEncryptionKeyFile(masterKeyFile(t)))
	require.NoError(t, err)
	defer func() { require.NoError(t, release()) }()

	_, _, err = Acquire(dir, spec.Spec{}, WithEncryptionKeyFile(masterKeyFile(t)))
	require.ErrorIs(t, err, ErrSessionDefaultsMismatch)
}
