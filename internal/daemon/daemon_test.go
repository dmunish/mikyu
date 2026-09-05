package daemon

import (
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsDaemonAlive(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()

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
