package protocol

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReader(t *testing.T) {
	var b Buffer
	b.WriteU32(42)
	b.WriteU64(1234567890)
	b.WriteBytes([]byte("test"))

	r := NewReader(b.Bytes())
	v32, err := r.ReadU32()
	require.NoError(t, err)
	assert.Equal(t, uint32(42), v32)
	v64, err := r.ReadU64()
	require.NoError(t, err)
	assert.Equal(t, uint64(1234567890), v64)
	bs, err := r.ReadBytes()
	require.NoError(t, err)
	assert.Equal(t, []byte("test"), bs)
}

func TestReaderTruncated(t *testing.T) {
	r := NewReader([]byte{0x01})
	_, err := r.ReadU32()
	assert.Error(t, err)
}

func TestReaderInsufficientBytes(t *testing.T) {
	r := NewReader([]byte{0, 0, 0}) // only 3 bytes for a U32
	_, err := r.ReadU32()
	assert.Error(t, err)
}

func TestWriterOverflow(t *testing.T) {
	var b Buffer
	b.WriteU32(1)
	b.WriteU64(2)
	assert.Len(t, b.Bytes(), 12)
}
