package engine

import (
	"crypto/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testKeyFile(t *testing.T, keys ...string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "keys")
	require.NoError(t, os.WriteFile(path, []byte(strings.Join(keys, "\n")+"\n"), 0o600))
	return path
}

func testMasterKey(t *testing.T) string {
	t.Helper()
	var raw [32]byte
	_, err := rand.Read(raw[:])
	require.NoError(t, err)
	return testKeyFile(t, hexKey(raw))
}

func hexKey(key [32]byte) string {
	const digits = "0123456789abcdef"
	out := make([]byte, 64)
	for i, b := range key {
		out[2*i] = digits[b>>4]
		out[2*i+1] = digits[b&0x0f]
	}
	return string(out)
}

func TestParseMasterKeys(t *testing.T) {
	raw := "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"
	keys, err := parseMasterKeys(strings.NewReader("# comment\n\n" + raw + "\n"))
	require.NoError(t, err)
	require.Len(t, keys, 1)
	assert.NotZero(t, keys[0].ID)

	_, err = parseMasterKeys(strings.NewReader("zz"))
	require.ErrorIs(t, err, ErrEncryptionKey)

	_, err = parseMasterKeys(strings.NewReader("# only a comment"))
	require.ErrorIs(t, err, ErrEncryptionKey)
}

func TestMasterKeysRoundTrip(t *testing.T) {
	masters, err := LoadMasterKeys(testMasterKey(t))
	require.NoError(t, err)

	wrapped, err := masters.wrap([]byte("log/events"), []byte("data key"))
	require.NoError(t, err)
	opened, err := masters.unwrap([]byte("log/events"), wrapped)
	require.NoError(t, err)
	assert.Equal(t, "data key", string(opened))

	_, err = masters.unwrap([]byte("task/events"), wrapped)
	assert.ErrorIs(t, err, ErrEncryptionKey)

	_, err = masters.unwrap([]byte("log/events"), wrapped[:len(wrapped)-1])
	assert.ErrorIs(t, err, ErrEncryptionKey)
}

func TestMasterKeyRotation(t *testing.T) {
	oldKey, newKey := hexKey(randomKey(t)), hexKey(randomKey(t))
	first, err := LoadMasterKeys(testKeyFile(t, oldKey))
	require.NoError(t, err)
	wrapped, err := first.wrap(nil, []byte("data key"))
	require.NoError(t, err)

	// Rotation appends the new master; the old line stays so wrapped keys open.
	rotated, err := LoadMasterKeys(testKeyFile(t, oldKey, newKey))
	require.NoError(t, err)
	opened, err := rotated.unwrap(nil, wrapped)
	require.NoError(t, err)
	assert.Equal(t, "data key", string(opened))
}

func randomKey(t *testing.T) [32]byte {
	t.Helper()
	var key [32]byte
	_, err := rand.Read(key[:])
	require.NoError(t, err)
	return key
}

func TestQueueCipherSealOpen(t *testing.T) {
	dir := t.TempDir()
	root, err := os.OpenRoot(dir)
	require.NoError(t, err)
	defer root.Close()
	masters, err := LoadMasterKeys(testMasterKey(t))
	require.NoError(t, err)

	c, err := mintQueueCipher(root, "events", masters)
	require.NoError(t, err)

	payload := []byte("attack at dawn")
	ct := make([]byte, len(payload)+gcmOverhead)
	c.seal(ct, payload)
	assert.NotEqual(t, payload, ct[:len(payload)], "ciphertext must not leak the plaintext")

	pt, err := c.open(ct)
	require.NoError(t, err)
	assert.Equal(t, payload, pt)

	reopened, err := decodeKeyStore("events", masters, readFile(t, filepath.Join(dir, keysFileName)))
	require.NoError(t, err)
	pt, err = reopened.open(ct)
	require.NoError(t, err)
	assert.Equal(t, payload, pt)
}

func TestQueueCipherRejectsForeignQueue(t *testing.T) {
	dir := t.TempDir()
	root, err := os.OpenRoot(dir)
	require.NoError(t, err)
	defer root.Close()
	masters, err := LoadMasterKeys(testMasterKey(t))
	require.NoError(t, err)

	_, err = mintQueueCipher(root, "events", masters)
	require.NoError(t, err)
	raw := readFile(t, filepath.Join(dir, keysFileName))

	_, err = decodeKeyStore("other", masters, raw)
	assert.ErrorIs(t, err, ErrEncryptionKey)
}

func TestQueueCipherKeyStoreLayout(t *testing.T) {
	dir := t.TempDir()
	root, err := os.OpenRoot(dir)
	require.NoError(t, err)
	defer root.Close()
	masters, err := LoadMasterKeys(testMasterKey(t))
	require.NoError(t, err)

	c, err := mintQueueCipher(root, "events", masters)
	require.NoError(t, err)
	require.NoError(t, c.rotate(root, "events", masters))

	raw := readFile(t, filepath.Join(dir, keysFileName))
	assert.Equal(t, keysMagic, string(raw[:4]))

	reopened, err := decodeKeyStore("events", masters, raw)
	require.NoError(t, err)
	require.Len(t, reopened.gens, 2)

	payload := []byte("second generation")
	ct := make([]byte, len(payload)+gcmOverhead)
	reopened.seal(ct, payload)
	pt, err := reopened.open(ct)
	require.NoError(t, err)
	assert.Equal(t, payload, pt)

	// The first generation's records stay readable after rotation.
	first := &queueCipher{gens: reopened.gens[:1]}
	ct = make([]byte, len(payload)+gcmOverhead)
	first.seal(ct, payload)
	pt, err = reopened.open(ct)
	require.NoError(t, err)
	assert.Equal(t, payload, pt)
}

func TestQueueCipherCorruptCiphertext(t *testing.T) {
	dir := t.TempDir()
	root, err := os.OpenRoot(dir)
	require.NoError(t, err)
	defer root.Close()
	masters, err := LoadMasterKeys(testMasterKey(t))
	require.NoError(t, err)

	c, err := mintQueueCipher(root, "events", masters)
	require.NoError(t, err)

	ct := make([]byte, 128)
	_, err = rand.Read(ct)
	require.NoError(t, err)
	_, err = c.open(ct)
	require.ErrorIs(t, err, ErrCorruptRecord)
}

func TestKeyStoreMissingOnSealedData(t *testing.T) {
	dir := t.TempDir()
	root, err := os.OpenRoot(dir)
	require.NoError(t, err)
	defer root.Close()
	masters, err := LoadMasterKeys(testMasterKey(t))
	require.NoError(t, err)

	_, err = mintQueueCipher(root, "events", masters)
	require.NoError(t, err)
	require.NoError(t, os.Remove(filepath.Join(dir, keysFileName)))

	_, err = openQueueCipher(root, "events", masters, true)
	assert.ErrorIs(t, err, ErrEncryptionKey)

	// A fresh queue mints a new key store.
	c, err := openQueueCipher(root, "events", masters, false)
	require.NoError(t, err)
	assert.Len(t, c.gens, 1)
}

func TestKeyStoreWrongMaster(t *testing.T) {
	dir := t.TempDir()
	root, err := os.OpenRoot(dir)
	require.NoError(t, err)
	defer root.Close()
	masters, err := LoadMasterKeys(testMasterKey(t))
	require.NoError(t, err)

	_, err = mintQueueCipher(root, "events", masters)
	require.NoError(t, err)

	other, err := LoadMasterKeys(testMasterKey(t))
	require.NoError(t, err)
	raw := readFile(t, filepath.Join(dir, keysFileName))
	_, err = decodeKeyStore("events", other, raw)
	assert.ErrorIs(t, err, ErrEncryptionKey)
}

func readFile(t *testing.T, path string) []byte {
	t.Helper()
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	return raw
}
