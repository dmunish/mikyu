package credentials

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStoreRoundTrip(t *testing.T) {
	dir := t.TempDir()
	restore := TestUserConfigDir(func() (string, error) { return dir, nil })
	defer restore()

	_, ok := Load("tcp://127.0.0.1:7420")
	assert.False(t, ok, "an empty store holds no token")

	require.NoError(t, Save("tcp://127.0.0.1:7420", "token-a"))
	require.NoError(t, Save("unix:///tmp/s.sock", "token-b"))

	tok, ok := Load("tcp://127.0.0.1:7420")
	require.True(t, ok)
	assert.Equal(t, "token-a", tok)

	require.NoError(t, Delete("tcp://127.0.0.1:7420"))
	_, ok = Load("tcp://127.0.0.1:7420")
	assert.False(t, ok)
	assert.NoFileExists(t, dir+"\\mikyu\\credentials.tmp", "the temp file never survives a write")

	tok, ok = Load("unix:///tmp/s.sock")
	require.True(t, ok, "an unrelated entry survives a delete")
	assert.Equal(t, "token-b", tok)

	require.NoError(t, Delete("tcp://127.0.0.1:7420"), "deleting a missing entry is a no-op")
}
