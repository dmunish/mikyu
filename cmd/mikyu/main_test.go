package main

import (
	"net"
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

func TestIsDaemonAlive(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()

	// Accept loop that exits when listener is closed.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			conn.Close()
		}
	}()

	target := "tcp://" + ln.Addr().String()
	assert.True(t, IsDaemonAlive(target))
	assert.False(t, IsDaemonAlive("tcp://127.0.0.1:1"))

	ln.Close()
	<-done
}

func TestPrintUsage(t *testing.T) {
	printUsage()
}
