package cli

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResolvePayload(t *testing.T) {
	payload, err := resolvePayload("hello")
	assert.NoError(t, err)
	assert.Equal(t, []byte("hello"), payload)

	tmp := filepath.Join(t.TempDir(), "data.txt")
	require.NoError(t, os.WriteFile(tmp, []byte("file content"), 0644))
	payload, err = resolvePayload("@" + tmp)
	assert.NoError(t, err)
	assert.Equal(t, []byte("file content"), payload)
}

func TestPrintUsage(t *testing.T) {
	printUsage()
}
