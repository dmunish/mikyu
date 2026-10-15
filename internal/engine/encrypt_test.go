package engine

import (
	"bytes"
	"context"
	"crypto/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dmunish/mikyu/internal/spec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func encryptedTestConfig(masters MasterKeySet) Config {
	cfg := testConfig()
	cfg.Encryption = spec.EncryptionAES
	cfg.MasterKeys = masters
	return cfg
}

func TestEncryptedQueueRoundTrip(t *testing.T) {
	dir := t.TempDir()
	masters, err := LoadMasterKeys(testMasterKey(t))
	require.NoError(t, err)

	mgr, err := OpenManager(dir, encryptedTestConfig(masters), nil)
	require.NoError(t, err)
	q, err := mgr.Create("events")
	require.NoError(t, err)

	payloads := [][]byte{[]byte("attack at dawn"), []byte(""), bytesN(t, 4096)}
	offs, err := q.AppendBatch(context.Background(), payloads)
	require.NoError(t, err)
	require.Len(t, offs, len(payloads))

	for i, payload := range payloads {
		rec, err := q.Read(context.Background(), offs[i])
		require.NoError(t, err)
		assert.True(t, bytes.Equal(payload, rec.Payload), "payload %d round-trips", i)
	}
	recs, err := q.ReadBatch(context.Background(), offs[0], len(payloads))
	require.NoError(t, err)
	require.Len(t, recs, len(payloads))
	for i, rec := range recs {
		assert.True(t, bytes.Equal(payloads[i], rec.Payload), "batch payload %d round-trips", i)
	}

	// The plaintext never reaches the segment files.
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".log") {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(dir, e.Name()))
		require.NoError(t, err)
		assert.NotContains(t, string(raw), "attack at dawn")
	}

	// Reopen: the key store unwraps and every record reads back.
	require.NoError(t, mgr.Close())
	mgr, err = OpenManager(dir, encryptedTestConfig(masters), nil)
	require.NoError(t, err)
	defer mgr.Close()
	q, err = mgr.Get("events")
	require.NoError(t, err)
	for i, payload := range payloads {
		rec, err := q.Read(context.Background(), offs[i])
		require.NoError(t, err)
		assert.True(t, bytes.Equal(payload, rec.Payload), "reopened payload %d round-trips", i)
	}
}

func TestEncryptedQueueAsyncCacheServesPlaintext(t *testing.T) {
	dir := t.TempDir()
	masters, err := LoadMasterKeys(testMasterKey(t))
	require.NoError(t, err)

	cfg := encryptedTestConfig(masters)
	cfg.Durability = spec.DurabilityAsync
	cfg.GateCache = false
	mgr, err := OpenManager(dir, cfg, nil)
	require.NoError(t, err)
	defer mgr.Close()
	q, err := mgr.Create("events")
	require.NoError(t, err)

	payload := []byte("open-loop payload")
	off, err := q.Append(context.Background(), payload)
	require.NoError(t, err)

	// The append populated the ring cache; a hit must serve plaintext.
	rec, err := q.Read(context.Background(), off)
	require.NoError(t, err)
	assert.Equal(t, payload, rec.Payload)
}

func TestEncryptedAppendLimit(t *testing.T) {
	dir := t.TempDir()
	masters, err := LoadMasterKeys(testMasterKey(t))
	require.NoError(t, err)

	cfg := encryptedTestConfig(masters)
	cfg.MaxPayload = 512
	mgr, err := OpenManager(dir, cfg, nil)
	require.NoError(t, err)
	defer mgr.Close()
	q, err := mgr.Create("events")
	require.NoError(t, err)

	assert.Equal(t, int64(512-gcmOverhead), q.MaxAppendPayloadBytes())
	_, err = q.Append(context.Background(), bytesN(t, 512-gcmOverhead))
	require.NoError(t, err)
	_, err = q.Append(context.Background(), bytesN(t, 512-gcmOverhead+1))
	assert.ErrorIs(t, err, ErrMessageTooLarge)
}

func TestEncryptedQueueDeclinesSpans(t *testing.T) {
	dir := t.TempDir()
	masters, err := LoadMasterKeys(testMasterKey(t))
	require.NoError(t, err)

	mgr, err := OpenManager(dir, encryptedTestConfig(masters), nil)
	require.NoError(t, err)
	defer mgr.Close()
	q, err := mgr.Create("events")
	require.NoError(t, err)
	_, err = q.Append(context.Background(), bytesN(t, 32<<10))
	require.NoError(t, err)
	spans, err := q.(SpanSource).PayloadSpans(0, 1)
	require.NoError(t, err)
	assert.Nil(t, spans)
}

func TestEncryptedQueueWithoutKeys(t *testing.T) {
	masters, err := LoadMasterKeys(testMasterKey(t))
	require.NoError(t, err)

	// Creating an encrypted queue without masters fails closed.
	fresh, err := OpenManager(t.TempDir(), encryptedTestConfig(nil), nil)
	require.NoError(t, err)
	_, err = fresh.Create("events")
	require.ErrorIs(t, err, ErrEncryptionKey)
	require.NoError(t, fresh.Close())

	// An existing sealed queue without masters refuses to open.
	dir := t.TempDir()
	mgr, err := OpenManager(dir, encryptedTestConfig(masters), nil)
	require.NoError(t, err)
	q, err := mgr.Create("events")
	require.NoError(t, err)
	_, err = q.Append(context.Background(), []byte("sealed"))
	require.NoError(t, err)
	require.NoError(t, mgr.Close())

	_, err = OpenManager(dir, encryptedTestConfig(nil), nil)
	require.ErrorIs(t, err, ErrEncryptionKey)
}

func bytesN(t *testing.T, n int) []byte {
	t.Helper()
	raw := make([]byte, n)
	_, err := rand.Read(raw)
	require.NoError(t, err)
	return raw
}
