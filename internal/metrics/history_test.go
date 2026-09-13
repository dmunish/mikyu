package metrics

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRingBufferWrap(t *testing.T) {
	r := NewRingBuffer[int](3)
	for i := 1; i <= 8; i++ {
		r.Push(i)
	}
	assert.Equal(t, 3, r.Len())
	assert.Equal(t, []int{6, 7, 8}, r.Snapshot())
	latest, ok := r.Latest()
	require.True(t, ok)
	assert.Equal(t, 8, latest)
}

func TestRingBufferUnderCapacity(t *testing.T) {
	r := NewRingBuffer[string](4)
	r.Push("a")
	r.Push("b")
	assert.Equal(t, []string{"a", "b"}, r.Snapshot())
	latest, ok := r.Latest()
	require.True(t, ok)
	assert.Equal(t, "b", latest)
}

func TestRingBufferEmpty(t *testing.T) {
	r := NewRingBuffer[int](2)
	assert.Empty(t, r.Snapshot())
	_, ok := r.Latest()
	assert.False(t, ok)
}
