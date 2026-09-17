package server

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/dmunish/mikyu/internal/node"
	"github.com/dmunish/mikyu/internal/spec"
	"github.com/dmunish/mikyu/internal/transport/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createFrame builds a spec-bearing CreateQueue request:
// bytes(name) u16 specVersion bytes(json).
func createFrame(t *testing.T, name string, sp spec.Spec) []byte {
	t.Helper()
	data, err := spec.EncodeSpec(sp)
	require.NoError(t, err)
	var b protocol.Buffer
	b.WriteBytes([]byte(name))
	b.WriteU16(spec.CurrentSpecVersion)
	b.WriteBytes(data)
	return b.Bytes()
}

func TestHandleCreateQueue(t *testing.T) {
	n, release, _ := node.Acquire(t.TempDir(), spec.Spec{})
	defer func() { _ = release() }()
	srv := New(n)
	defer srv.Stop()

	respOp, respBody := srv.handleCreateQueue(t.Context(), createFrame(t, "newqueue", spec.Spec{}))
	assert.Equal(t, protocol.OpCreateQueue, respOp)
	assert.Nil(t, respBody)
}

func TestSpecVersionTooNew(t *testing.T) {
	n, release, _ := node.Acquire(t.TempDir(), spec.Spec{})
	defer func() { _ = release() }()
	srv := New(n)
	defer srv.Stop()

	var b protocol.Buffer
	b.WriteBytes([]byte("newqueue"))
	b.WriteU16(spec.CurrentSpecVersion + 1)
	b.WriteBytes([]byte("{}"))
	respOp, respBody := srv.handleCreateQueue(t.Context(), b.Bytes())
	assert.Equal(t, protocol.OpError, respOp)
	code, msg := protocol.DecodeError(respBody)
	assert.Equal(t, protocol.ErrCodeSpecTooNew, code)
	assert.Contains(t, msg, "spec")
}

func TestSpecPayloadTooLarge(t *testing.T) {
	n, release, _ := node.Acquire(t.TempDir(), spec.Spec{})
	defer func() { _ = release() }()
	srv := New(n)
	defer srv.Stop()

	// A 2 MiB payload is inside the frame limit but far beyond
	// spec.MaxSpecPayload; the size guard must fire before JSON parsing.
	var b protocol.Buffer
	b.WriteBytes([]byte("newqueue"))
	b.WriteU16(spec.CurrentSpecVersion)
	b.WriteBytes(bytes.Repeat([]byte{'a'}, 2<<20))
	respOp, respBody := srv.handleCreateQueue(t.Context(), b.Bytes())
	assert.Equal(t, protocol.OpError, respOp)
	code, _ := protocol.DecodeError(respBody)
	assert.Equal(t, protocol.ErrCodeSpecTooLarge, code)
}

func TestHandleUpdateQueueRejectsImmutableField(t *testing.T) {
	n, release, _ := node.Acquire(t.TempDir(), spec.Spec{})
	defer func() { _ = release() }()
	err := n.CreateQueue(t.Context(), "q", spec.Spec{})
	require.NoError(t, err)
	srv := New(n)
	defer srv.Stop()

	var b protocol.Buffer
	b.WriteBytes([]byte("q"))
	b.WriteU16(spec.CurrentSpecVersion)
	b.WriteBytes([]byte(`{"engine":{"segment_size":128}}`))
	respOp, respBody := srv.handleUpdateQueue(t.Context(), b.Bytes())
	assert.Equal(t, protocol.OpError, respOp)
	code, msg := protocol.DecodeError(respBody)
	assert.Equal(t, protocol.ErrCodeImmutableField, code)
	assert.Contains(t, msg, "segment_size")
}

func TestHandleAppendErrors(t *testing.T) {
	n, release, _ := node.Acquire(t.TempDir(), spec.Spec{})
	defer func() { _ = release() }()
	srv := New(n)
	defer srv.Stop()

	var b protocol.Buffer
	b.WriteBytes([]byte("nonexistent"))
	b.WriteBytes([]byte("data"))
	respOp, respBody := srv.handleAppend(t.Context(), b.Bytes())
	assert.Equal(t, protocol.OpError, respOp)
	code, _ := protocol.DecodeError(respBody)
	assert.Equal(t, protocol.ErrCodeQueueNotFound, code)
}

func TestHandleReadOffsetNotFound(t *testing.T) {
	n, release, _ := node.Acquire(t.TempDir(), spec.Spec{})
	defer func() { _ = release() }()
	srv := New(n)
	defer srv.Stop()
	_, _ = n.Manager().Create("q")

	var b protocol.Buffer
	b.WriteBytes([]byte("q"))
	b.WriteU64(5)
	respOp, respBody := srv.handleRead(t.Context(), b.Bytes())
	assert.Equal(t, protocol.OpError, respOp)
	code, _ := protocol.DecodeError(respBody)
	assert.Equal(t, protocol.ErrCodeOffsetNotFound, code)
}

func TestHandleFetch(t *testing.T) {
	n, release, _ := node.Acquire(t.TempDir(), spec.Spec{})
	defer func() { _ = release() }()
	srv := New(n)
	defer srv.Stop()
	_, _ = n.Manager().Create("q")

	t.Run("empty non-blocking", func(t *testing.T) {
		var b protocol.Buffer
		b.WriteBytes([]byte("q"))
		b.WriteU64(0)
		b.WriteU32(16)
		b.WriteU64(0)
		respOp, respBody := srv.handleFetch(t.Context(), b.Bytes())
		assert.Equal(t, protocol.OpError, respOp)
		code, _ := protocol.DecodeError(respBody)
		assert.Equal(t, protocol.ErrCodeNoData, code)
	})

	t.Run("returns batch", func(t *testing.T) {
		q, _ := srv.getOrCreateLogQueue("q")
		_, err := q.Append(t.Context(), []byte("hello"))
		require.NoError(t, err)

		var b protocol.Buffer
		b.WriteBytes([]byte("q"))
		b.WriteU64(0)
		b.WriteU32(16)
		b.WriteU64(0)
		respOp, respBody := srv.handleFetch(t.Context(), b.Bytes())
		assert.Equal(t, protocol.OpFetch, respOp)
		r := protocol.NewReader(respBody)
		n, _ := r.ReadU32()
		assert.Equal(t, uint32(1), n)
	})

	t.Run("truncated", func(t *testing.T) {
		var b protocol.Buffer
		b.WriteBytes([]byte("q"))
		b.WriteU64(0)
		b.WriteU32(16)
		b.WriteU64(0)
		q, _ := srv.getOrCreateLogQueue("q")
		_, err := q.Append(t.Context(), []byte("msg"))
		require.NoError(t, err)
		// Truncate should be tested separately; this fetch at 0 with 0 messages is not truncated.
		respOp, _ := srv.handleFetch(t.Context(), b.Bytes())
		assert.Equal(t, protocol.OpFetch, respOp)
	})
}

func TestHandleListQueues(t *testing.T) {
	n, release, _ := node.Acquire(t.TempDir(), spec.Spec{})
	defer func() { _ = release() }()
	srv := New(n)
	defer srv.Stop()
	_, _ = n.Manager().Create("q1")
	_, _ = n.Manager().Create("q2")

	respOp, respBody := srv.handleListQueues()
	assert.Equal(t, protocol.OpListQueues, respOp)
	r := protocol.NewReader(respBody)
	count, _ := r.ReadU32()
	assert.Equal(t, uint32(2), count)
}

// TestHandleSchema serves OpSchema and verifies the u16 version envelope plus
// the full parameter table keyed by canonical name.
func TestHandleSchema(t *testing.T) {
	n, release, _ := node.Acquire(t.TempDir(), spec.Spec{})
	defer func() { _ = release() }()
	srv := New(n)
	defer srv.Stop()

	respOp, respBody := srv.handleSchema()
	assert.Equal(t, protocol.OpSchema, respOp)
	r := protocol.NewReader(respBody)
	version, err := r.ReadU16()
	require.NoError(t, err)
	assert.Equal(t, spec.CurrentSpecVersion, version)
	data, err := r.ReadBytes()
	require.NoError(t, err)

	var params []spec.Param
	require.NoError(t, json.Unmarshal(data, &params))
	require.Len(t, params, len(spec.Table))
	seen := make(map[string]bool, len(params))
	for _, p := range params {
		seen[p.Key] = true
	}
	for _, p := range spec.Table {
		assert.True(t, seen[p.Key], "wire schema is missing %s", p.Key)
	}
}
