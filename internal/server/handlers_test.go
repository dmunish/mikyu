package server

import (
	"context"
	"testing"

	"github.com/dmunish/mikyu/internal/engine"
	"github.com/dmunish/mikyu/internal/protocol"
	"github.com/stretchr/testify/assert"
)

func TestHandleCreateQueue(t *testing.T) {
	mgr, _ := engine.OpenManager(t.TempDir(), engine.Config{})
	defer mgr.Close()
	srv := New(mgr)

	var buf protocol.Buffer
	buf.WriteBytes([]byte("newqueue"))
	respOp, respBody := srv.handleCreateQueue(buf.Bytes())
	assert.Equal(t, protocol.OpCreateQueue, respOp)
	assert.Nil(t, respBody)
}

func TestHandleProduceErrors(t *testing.T) {
	mgr, _ := engine.OpenManager(t.TempDir(), engine.Config{})
	defer mgr.Close()
	srv := New(mgr)

	var b protocol.Buffer
	b.WriteBytes([]byte("nonexistent"))
	b.WriteBytes([]byte("data"))
	respOp, respBody := srv.handleProduce(context.Background(), b.Bytes())
	assert.Equal(t, protocol.OpError, respOp)
	code, _ := protocol.DecodeError(respBody)
	assert.Equal(t, protocol.ErrCodeQueueNotFound, code)
}

func TestHandleReadOffsetNotFound(t *testing.T) {
	mgr, _ := engine.OpenManager(t.TempDir(), engine.Config{})
	defer mgr.Close()
	srv := New(mgr)
	_, _ = mgr.CreateQueue("q")

	var b protocol.Buffer
	b.WriteBytes([]byte("q"))
	b.WriteU64(5)
	respOp, respBody := srv.handleRead(context.Background(), b.Bytes())
	assert.Equal(t, protocol.OpError, respOp)
	code, _ := protocol.DecodeError(respBody)
	assert.Equal(t, protocol.ErrCodeOffsetNotFound, code)
}

func TestHandleListQueues(t *testing.T) {
	mgr, _ := engine.OpenManager(t.TempDir(), engine.Config{})
	defer mgr.Close()
	srv := New(mgr)
	_, _ = mgr.CreateQueue("q1")
	_, _ = mgr.CreateQueue("q2")

	respOp, respBody := srv.handleListQueues()
	assert.Equal(t, protocol.OpListQueues, respOp)
	r := protocol.NewReader(respBody)
	count, _ := r.ReadU32()
	assert.Equal(t, uint32(2), count)
}
