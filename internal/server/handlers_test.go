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

// createFrame builds an OpCreate request: u8 kind, bytes(name), u16 specVersion, bytes(json).
func createFrame(t *testing.T, kind node.Kind, name string, sp spec.Spec) []byte {
	t.Helper()
	data, err := spec.EncodeSpec(sp)
	require.NoError(t, err)
	var b protocol.Buffer
	b.WriteU8(byte(kind))
	b.WriteBytes([]byte(name))
	b.WriteU16(spec.CurrentSpecVersion)
	b.WriteBytes(data)
	return b.Bytes()
}

func TestHandleCreate(t *testing.T) {
	n, release, _ := node.Acquire(t.TempDir(), spec.Spec{})
	defer func() { _ = release() }()
	srv := New(n)
	defer srv.Stop()

	respOp, respBody := srv.handleCreate(t.Context(), createFrame(t, node.KindLog, "newqueue", spec.Spec{}))
	assert.Equal(t, protocol.OpCreate, respOp)
	assert.Nil(t, respBody)
}

func TestSpecVersionTooNew(t *testing.T) {
	n, release, _ := node.Acquire(t.TempDir(), spec.Spec{})
	defer func() { _ = release() }()
	srv := New(n)
	defer srv.Stop()

	var b protocol.Buffer
	b.WriteU8(byte(node.KindLog))
	b.WriteBytes([]byte("newqueue"))
	b.WriteU16(spec.CurrentSpecVersion + 1)
	b.WriteBytes([]byte("{}"))
	respOp, respBody := srv.handleCreate(t.Context(), b.Bytes())
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
	b.WriteU8(byte(node.KindLog))
	b.WriteBytes([]byte("newqueue"))
	b.WriteU16(spec.CurrentSpecVersion)
	b.WriteBytes(bytes.Repeat([]byte{'a'}, 2<<20))
	respOp, respBody := srv.handleCreate(t.Context(), b.Bytes())
	assert.Equal(t, protocol.OpError, respOp)
	code, _ := protocol.DecodeError(respBody)
	assert.Equal(t, protocol.ErrCodeSpecTooLarge, code)
}

func TestHandleUpdateRejectsImmutableField(t *testing.T) {
	n, release, _ := node.Acquire(t.TempDir(), spec.Spec{})
	defer func() { _ = release() }()
	_, err := n.CreateLogQueue(t.Context(), "q", spec.Spec{})
	require.NoError(t, err)
	srv := New(n)
	defer srv.Stop()

	var b protocol.Buffer
	b.WriteBytes([]byte("q"))
	b.WriteU16(spec.CurrentSpecVersion)
	b.WriteBytes([]byte(`{"engine":{"segment_size":128}}`))
	respOp, respBody := srv.handleUpdate(t.Context(), b.Bytes())
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
	_, err := n.CreateLogQueue(t.Context(), "q", spec.Spec{})
	require.NoError(t, err)

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
	_, err := n.CreateLogQueue(t.Context(), "q", spec.Spec{})
	require.NoError(t, err)

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
		got, _ := r.ReadU32()
		assert.Equal(t, uint32(1), got)
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
		respOp, _ := srv.handleFetch(t.Context(), b.Bytes())
		assert.Equal(t, protocol.OpFetch, respOp)
	})
}

func TestHandleList(t *testing.T) {
	n, release, _ := node.Acquire(t.TempDir(), spec.Spec{})
	defer func() { _ = release() }()
	srv := New(n)
	defer srv.Stop()
	_, err := n.CreateLogQueue(t.Context(), "q1", spec.Spec{})
	require.NoError(t, err)
	_, err = n.CreateTaskQueue(t.Context(), "q2", spec.Spec{})
	require.NoError(t, err)

	respOp, respBody := srv.handleList(t.Context())
	assert.Equal(t, protocol.OpList, respOp)
	r := protocol.NewReader(respBody)
	count, _ := r.ReadU32()
	assert.Equal(t, uint32(2), count)
	name, err := r.ReadBytes()
	require.NoError(t, err)
	assert.Equal(t, "q1", string(name))
	kind, err := r.ReadU8()
	require.NoError(t, err)
	assert.Equal(t, byte(node.KindLog), kind)
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