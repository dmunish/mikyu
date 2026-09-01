package server

import (
	"context"
	"errors"
	"fmt"

	"github.com/dmunish/mikyu/internal/engine"
	"github.com/dmunish/mikyu/internal/protocol"
)

func (s *Server) dispatch(ctx context.Context, op byte, body []byte) (byte, []byte, bool) {
	switch op {
	case protocol.OpCreateQueue:
		respOp, respBody := s.handleCreateQueue(body)
		return respOp, respBody, false
	case protocol.OpProduce:
		respOp, respBody := s.handleProduce(ctx, body)
		return respOp, respBody, false
	case protocol.OpProduceBatch:
		respOp, respBody := s.handleProduceBatch(ctx, body)
		return respOp, respBody, false
	case protocol.OpRead:
		respOp, respBody := s.handleRead(ctx, body)
		return respOp, respBody, false
	case protocol.OpReadBatch:
		respOp, respBody := s.handleReadBatch(ctx, body)
		return respOp, respBody, false
	case protocol.OpTail:
		respOp, respBody := s.handleTail(body)
		return respOp, respBody, false
	case protocol.OpCommitOffset:
		respOp, respBody := s.handleCommitOffset(ctx, body)
		return respOp, respBody, false
	case protocol.OpFetchOffset:
		respOp, respBody := s.handleFetchOffset(ctx, body)
		return respOp, respBody, false
	case protocol.OpListQueues:
		respOp, respBody := s.handleListQueues()
		return respOp, respBody, false
	case protocol.OpDeleteQueue:
		respOp, respBody := s.handleDeleteQueue(body)
		return respOp, respBody, false
	case protocol.OpStats:
		respOp, respBody := s.handleStats()
		return respOp, respBody, false
	case protocol.OpShutdown:
		return protocol.OpShutdown, nil, true
	default:
		return protocol.OpError, protocol.EncodeError(
			protocol.ErrCodeUnknown, fmt.Sprintf("unknown opcode 0x%02x", op)), false
	}
}

func (s *Server) handleCreateQueue(body []byte) (byte, []byte) {
	r := protocol.NewReader(body)
	name, err := r.ReadBytes()
	if err != nil {
		return protocol.OpError, protocol.EncodeError(protocol.ErrCodeUnknown, "invalid queue name")
	}

	if _, err := s.mgr.CreateQueue(string(name)); err != nil {
		if errors.Is(err, engine.ErrQueueExists) {
			return protocol.OpError, protocol.EncodeError(protocol.ErrCodeQueueExists, err.Error())
		}
		return protocol.OpError, protocol.EncodeError(protocol.ErrCodeUnknown, "failed to create queue")
	}

	return protocol.OpCreateQueue, nil
}

func (s *Server) handleProduce(ctx context.Context, body []byte) (byte, []byte) {
	r := protocol.NewReader(body)
	name, err := r.ReadBytes()
	if err != nil {
		return protocol.OpError, protocol.EncodeError(protocol.ErrCodeUnknown, "invalid queue name")
	}
	payload, err := r.ReadBytes()
	if err != nil {
		return protocol.OpError, protocol.EncodeError(protocol.ErrCodeUnknown, "invalid message payload")
	}

	q, err := s.mgr.GetQueue(string(name))
	if err != nil {
		return protocol.OpError, protocol.EncodeError(protocol.ErrCodeQueueNotFound, err.Error())
	}

	offset, err := q.Produce(ctx, payload)
	if err != nil {
		return protocol.OpError, protocol.EncodeError(protocol.ErrCodeUnknown, "produce failed")
	}

	var resp protocol.Buffer
	resp.WriteU64(offset)
	return protocol.OpProduce, resp.Bytes()
}

func (s *Server) handleProduceBatch(ctx context.Context, body []byte) (byte, []byte) {
	r := protocol.NewReader(body)
	name, err := r.ReadBytes()
	if err != nil {
		return protocol.OpError, protocol.EncodeError(protocol.ErrCodeUnknown, "invalid queue name")
	}
	count, err := r.ReadU32()
	if err != nil {
		return protocol.OpError, protocol.EncodeError(protocol.ErrCodeUnknown, "invalid payload count")
	}

	payloads := make([][]byte, count)
	for i := range count {
		p, err := r.ReadBytes()
		if err != nil {
			return protocol.OpError, protocol.EncodeError(protocol.ErrCodeUnknown, "invalid payload in batch")
		}
		payloads[i] = p
	}

	q, err := s.mgr.GetQueue(string(name))
	if err != nil {
		return protocol.OpError, protocol.EncodeError(protocol.ErrCodeQueueNotFound, err.Error())
	}

	offsets, err := q.ProduceBatch(ctx, payloads)
	if err != nil {
		return protocol.OpError, protocol.EncodeError(protocol.ErrCodeUnknown, "batch produce failed")
	}

	var resp protocol.Buffer
	resp.WriteU32(uint32(len(offsets)))
	for _, off := range offsets {
		resp.WriteU64(off)
	}
	return protocol.OpProduceBatch, resp.Bytes()
}

func (s *Server) handleRead(ctx context.Context, body []byte) (byte, []byte) {
	r := protocol.NewReader(body)
	name, err := r.ReadBytes()
	if err != nil {
		return protocol.OpError, protocol.EncodeError(protocol.ErrCodeUnknown, "invalid queue name")
	}
	offset, err := r.ReadU64()
	if err != nil {
		return protocol.OpError, protocol.EncodeError(protocol.ErrCodeUnknown, "invalid offset")
	}

	q, err := s.mgr.GetQueue(string(name))
	if err != nil {
		return protocol.OpError, protocol.EncodeError(protocol.ErrCodeQueueNotFound, err.Error())
	}

	msg, err := q.Read(ctx, offset)
	if err != nil {
		if errors.Is(err, engine.ErrOffsetNotFound) {
			return protocol.OpError, protocol.EncodeError(protocol.ErrCodeOffsetNotFound, err.Error())
		}
		return protocol.OpError, protocol.EncodeError(protocol.ErrCodeUnknown, err.Error())
	}

	var resp protocol.Buffer
	resp.WriteU64(msg.Offset)
	resp.WriteBytes([]byte(msg.ID))
	resp.WriteU64(uint64(msg.Timestamp.UnixNano()))
	resp.WriteBytes(msg.Payload)
	return protocol.OpRead, resp.Bytes()
}

func (s *Server) handleReadBatch(ctx context.Context, body []byte) (byte, []byte) {
	r := protocol.NewReader(body)
	name, err := r.ReadBytes()
	if err != nil {
		return protocol.OpError, protocol.EncodeError(protocol.ErrCodeUnknown, "invalid queue name")
	}
	startOffset, err := r.ReadU64()
	if err != nil {
		return protocol.OpError, protocol.EncodeError(protocol.ErrCodeUnknown, "invalid start offset")
	}
	count, err := r.ReadU32()
	if err != nil {
		return protocol.OpError, protocol.EncodeError(protocol.ErrCodeUnknown, "invalid count")
	}

	q, err := s.mgr.GetQueue(string(name))
	if err != nil {
		return protocol.OpError, protocol.EncodeError(protocol.ErrCodeQueueNotFound, err.Error())
	}

	messages, err := q.ReadBatch(ctx, startOffset, int(count))
	if err != nil {
		if errors.Is(err, engine.ErrOffsetNotFound) {
			return protocol.OpError, protocol.EncodeError(protocol.ErrCodeOffsetNotFound, err.Error())
		}
		return protocol.OpError, protocol.EncodeError(protocol.ErrCodeUnknown, err.Error())
	}

	var resp protocol.Buffer
	resp.WriteU32(uint32(len(messages)))
	for _, msg := range messages {
		resp.WriteU64(msg.Offset)
		resp.WriteBytes([]byte(msg.ID))
		resp.WriteU64(uint64(msg.Timestamp.UnixNano()))
		resp.WriteBytes(msg.Payload)
	}
	return protocol.OpReadBatch, resp.Bytes()
}

func (s *Server) handleTail(body []byte) (byte, []byte) {
	r := protocol.NewReader(body)
	name, err := r.ReadBytes()
	if err != nil {
		return protocol.OpError, protocol.EncodeError(protocol.ErrCodeUnknown, "invalid queue name")
	}

	q, err := s.mgr.GetQueue(string(name))
	if err != nil {
		return protocol.OpError, protocol.EncodeError(protocol.ErrCodeQueueNotFound, err.Error())
	}

	var resp protocol.Buffer
	resp.WriteU64(q.LastOffset())
	return protocol.OpTail, resp.Bytes()
}

func (s *Server) handleCommitOffset(ctx context.Context, body []byte) (byte, []byte) {
	r := protocol.NewReader(body)
	name, err := r.ReadBytes()
	if err != nil {
		return protocol.OpError, protocol.EncodeError(protocol.ErrCodeUnknown, "invalid queue name")
	}
	group, err := r.ReadBytes()
	if err != nil {
		return protocol.OpError, protocol.EncodeError(protocol.ErrCodeUnknown, "invalid group name")
	}
	offset, err := r.ReadU64()
	if err != nil {
		return protocol.OpError, protocol.EncodeError(protocol.ErrCodeUnknown, "invalid offset")
	}

	q, err := s.mgr.GetQueue(string(name))
	if err != nil {
		return protocol.OpError, protocol.EncodeError(protocol.ErrCodeQueueNotFound, err.Error())
	}
	if err := q.CommitOffset(ctx, string(group), offset); err != nil {
		return protocol.OpError, protocol.EncodeError(protocol.ErrCodeUnknown, err.Error())
	}
	return protocol.OpCommitOffset, nil
}

func (s *Server) handleFetchOffset(ctx context.Context, body []byte) (byte, []byte) {
	r := protocol.NewReader(body)
	name, err := r.ReadBytes()
	if err != nil {
		return protocol.OpError, protocol.EncodeError(protocol.ErrCodeUnknown, "invalid queue name")
	}
	group, err := r.ReadBytes()
	if err != nil {
		return protocol.OpError, protocol.EncodeError(protocol.ErrCodeUnknown, "invalid group name")
	}

	q, err := s.mgr.GetQueue(string(name))
	if err != nil {
		return protocol.OpError, protocol.EncodeError(protocol.ErrCodeQueueNotFound, err.Error())
	}
	offset, err := q.FetchOffset(ctx, string(group))
	if err != nil {
		return protocol.OpError, protocol.EncodeError(protocol.ErrCodeUnknown, err.Error())
	}
	var resp protocol.Buffer
	resp.WriteU64(offset)
	return protocol.OpFetchOffset, resp.Bytes()
}

func (s *Server) handleListQueues() (byte, []byte) {
	queues := s.mgr.ListQueues()
	var resp protocol.Buffer
	resp.WriteU32(uint32(len(queues)))
	for _, name := range queues {
		resp.WriteBytes([]byte(name))
	}
	return protocol.OpListQueues, resp.Bytes()
}

func (s *Server) handleDeleteQueue(body []byte) (byte, []byte) {
	r := protocol.NewReader(body)
	name, err := r.ReadBytes()
	if err != nil {
		return protocol.OpError, protocol.EncodeError(protocol.ErrCodeUnknown, "invalid queue name")
	}

	if err := s.mgr.DeleteQueue(string(name)); err != nil {
		if errors.Is(err, engine.ErrQueueNotFound) {
			return protocol.OpError, protocol.EncodeError(protocol.ErrCodeQueueNotFound, err.Error())
		}
		return protocol.OpError, protocol.EncodeError(protocol.ErrCodeUnknown, err.Error())
	}
	return protocol.OpDeleteQueue, nil
}

func (s *Server) handleStats() (byte, []byte) {
	st := s.mgr.Stats()
	var resp protocol.Buffer
	resp.WriteU32(uint32(st.ActiveQueues))
	resp.WriteU64(uint64(st.TotalBytes))
	return protocol.OpStats, resp.Bytes()
}
