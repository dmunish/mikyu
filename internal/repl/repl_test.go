package repl

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseREPLArgs(t *testing.T) {
	args := parseREPLArgs(`produce "hello world" @file.txt`)
	assert.Equal(t, []string{"produce", "hello world", "@file.txt"}, args)
}

func TestParseREPLArgsEmptyQuotes(t *testing.T) {
	args := parseREPLArgs(`produce "" @file`)
	assert.Equal(t, []string{"produce", "", "@file"}, args)
}

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
