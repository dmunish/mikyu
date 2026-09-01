package ipc

import (
	"net"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestListenUnix(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.sock")
	ln, err := Listen("unix://" + path)
	require.NoError(t, err)
	defer ln.Close()

	go func() {
		conn, err := net.Dial("unix", path)
		if err == nil {
			conn.Close()
		}
	}()
	conn, err := ln.Accept()
	require.NoError(t, err)
	conn.Close()
}

func TestListenUnixRemovesStaleSocket(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "stale.sock")
	ln1, _ := net.Listen("unix", path)
	ln1.Close()

	ln2, err := Listen("unix://" + path)
	require.NoError(t, err)
	defer ln2.Close()
}

func TestListenerCloseRemovesSocket(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.sock")
	ln, _ := Listen("unix://" + path)
	ln.Close()
	_, err := net.Dial("unix", path)
	assert.Error(t, err)
}
