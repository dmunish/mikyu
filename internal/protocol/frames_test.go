package protocol

import (
	"bufio"
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriteReadFrame(t *testing.T) {
	var buf bytes.Buffer
	payload := []byte("hello")
	err := WriteFrame(&buf, OpProduce, payload)
	require.NoError(t, err)

	r := bufio.NewReader(&buf)
	op, data, err := ReadFrame(r)
	require.NoError(t, err)
	assert.Equal(t, OpProduce, op)
	assert.Equal(t, payload, data)
}

func TestReadFrameZeroLength(t *testing.T) {
	var buf bytes.Buffer
	buf.Write([]byte{0, 0, 0, 0})
	r := bufio.NewReader(&buf)
	_, _, err := ReadFrame(r)
	assert.Error(t, err)
}

func TestWriteFramePoolReuse(t *testing.T) {
	var buf bytes.Buffer
	payload := []byte("reuse")
	for i := 0; i < 100; i++ {
		buf.Reset()
		err := WriteFrame(&buf, OpStats, payload)
		require.NoError(t, err)
	}
}

func TestWriteFrameEmptyPayload(t *testing.T) {
	var buf bytes.Buffer
	err := WriteFrame(&buf, OpShutdown, nil)
	require.NoError(t, err)
	r := bufio.NewReader(&buf)
	op, data, err := ReadFrame(r)
	require.NoError(t, err)
	assert.Equal(t, OpShutdown, op)
	assert.Empty(t, data)
}

func BenchmarkWriteFrame(b *testing.B) {
	payload := make([]byte, 64*1024) // 64 KB frame

	b.ReportAllocs()

	for b.Loop() {
		if err := WriteFrame(io.Discard, OpProduce, payload); err != nil {
			b.Fatal(err)
		}
	}
}
