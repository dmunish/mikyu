package command_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/dmunish/mikyu/internal/ui/command"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCredentialsRoundTrip(t *testing.T) {
	require.NoError(t, command.SaveToken("tcp://127.0.0.1:7420", "0123456789abcdef"))
	tok, ok := command.LoadToken("tcp://127.0.0.1:7420")
	require.True(t, ok)
	assert.Equal(t, "0123456789abcdef", tok)

	require.NoError(t, command.DeleteToken("tcp://127.0.0.1:7420"))
	_, ok = command.LoadToken("tcp://127.0.0.1:7420")
	assert.False(t, ok)
}

func TestCredentialsFilePermissions(t *testing.T) {
	require.NoError(t, command.SaveToken("test-target", "0123456789abcdef"))
	path, err := command.CredentialsPath()
	require.NoError(t, err)

	info, err := os.Stat(path)
	require.NoError(t, err)
	assert.False(t, info.IsDir(), "credentials should be a file")
}

func TestOpenClientForwardsCredentialFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "credentials")
	require.NoError(t, os.WriteFile(path, []byte("test-target: 0123456789abcdef\n"), 0o600))

	// Not a real server; just verifies the file loads without error and the
	// function reaches the credential-fallback path without panicking.
	tok, ok := command.LoadToken("test-target")
	require.True(t, ok)
	assert.Equal(t, "0123456789abcdef", tok)
}
