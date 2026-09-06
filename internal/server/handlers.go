package server

import (
	"context"
	"errors"

	"github.com/dmunish/mikyu/internal/engine"
	"github.com/dmunish/mikyu/internal/logqueue"
	"github.com/dmunish/mikyu/internal/protocol"
)

// dispatch routes opcodes to handlers.
func (s *Server) dispatch(ctx context.Context, op byte, body []byte) (respOp byte, respBody []byte, shouldShutdown bool) {
	switch op {
	case protocol.OpCreateQueue:
		respOp, respBody = s.handleCreateQueue(body)
	case protocol.OpProduce:
		respOp, respBody = s.handleProduce(ctx, body)
	case protocol.OpProduceBatch:
		respOp, respBody = s.handleProduceBatch(ctx, body)
	case protocol.OpRead:
		respOp, respBody = s.handleRead(ctx, body)
	case protocol.OpReadBatch:
		respOp, respBody = s.handleReadBatch(ctx, body)
	case protocol.OpTail:
		respOp, respBody = s.handleTail(ctx, body)
	case protocol.OpCommitOffset:
		respOp, respBody = s.handleCommitOffset(ctx, body)
	case protocol.OpFetchOffset:
		respOp, respBody = s.handleFetchOffset(ctx, body)
	case protocol.OpListQueues:
		respOp, respBody = s.handleListQueues()
	case protocol.OpDeleteQueue:
		respOp, respBody = s.handleDeleteQueue(body)
	case protocol.OpStats:
		respOp, respBody = s.handleStats()
	case protocol.OpTruncate:
		respOp, respBody = s.handleTruncate(ctx, body)
	case protocol.OpShutdown:
		return protocol.OpShutdown, nil, true
	default:
		respOp, respBody = opErr(protocol.ErrCodeUnknown, "unknown opcode")
	}
	return respOp, respBody, false
}

func (s *Server) handleCreateQueue(body []byte) (byte, []byte) {
	r := protocol.NewReader(body)
	name, err := r.ReadBytes()
	if err != nil {
		return opErr(protocol.ErrCodeUnknown, err.Error())
	}
	_, err = s.mgr.CreateQueue(string(name))
	if err != nil {
		if errors.Is(err, engine.ErrQueueExists) {
			return opErr(protocol.ErrCodeQueueExists, err.Error())
		}
		return opErr(protocol.ErrCodeUnknown, err.Error())
	}
	return protocol.OpCreateQueue, nil
}

func (s *Server) handleProduce(ctx context.Context, body []byte) (byte, []byte) {
	r := protocol.NewReader(body)
	name, _ := r.ReadBytes()
	payload, _ := r.ReadBytes()
	storage, err := s.mgr.GetQueue(string(name))
	if err != nil {
		return opErr(protocol.ErrCodeQueueNotFound, err.Error())
	}
	q := logqueue.NewLogQueue(string(name), storage)
	off, err := q.Produce(ctx, payload)
	if err != nil {
		return opErr(protocol.ErrCodeUnknown, err.Error())
	}
	buf := protocol.Buffer{}
	buf.WriteU64(off)
	return protocol.OpProduce, buf.Bytes()
}

func (s *Server) handleProduceBatch(ctx context.Context, body []byte) (byte, []byte) {
	r := protocol.NewReader(body)
	name, _ := r.ReadBytes()
	count, _ := r.ReadU32()
	payloads := make([][]byte, count)
	for i := uint32(0); i < count; i++ {
		payloads[i], _ = r.ReadBytes()
	}
	storage, err := s.mgr.GetQueue(string(name))
	if err != nil {
		return opErr(protocol.ErrCodeQueueNotFound, err.Error())
	}
	q := logqueue.NewLogQueue(string(name), storage)
	offs, err := q.ProduceBatch(ctx, payloads)
	if err != nil {
		return opErr(protocol.ErrCodeUnknown, err.Error())
	}
	buf := protocol.Buffer{}
	buf.WriteU32(uint32(len(offs)))
	for _, off := range offs {
		buf.WriteU64(off)
	}
	return protocol.OpProduceBatch, buf.Bytes()
}

func (s *Server) handleRead(ctx context.Context, body []byte) (byte, []byte) {
	r := protocol.NewReader(body)
	name, _ := r.ReadBytes()
	off, _ := r.ReadU64()
	storage, err := s.mgr.GetQueue(string(name))
	if err != nil {
		return opErr(protocol.ErrCodeQueueNotFound, err.Error())
	}
	q := logqueue.NewLogQueue(string(name), storage)
	msg, err := q.Read(ctx, off)
	if err != nil {
		if errors.Is(err, engine.ErrOffsetNotFound) {
			return opErr(protocol.ErrCodeOffsetNotFound, err.Error())
		}
		if errors.Is(err, engine.ErrOffsetTruncated) {
			return opErr(protocol.ErrCodeOffsetTruncated, err.Error())
		}
		return opErr(protocol.ErrCodeUnknown, err.Error())
	}
	buf := protocol.Buffer{}
	buf.WriteU64(msg.Offset)
	buf.WriteBytes([]byte(msg.ID))
	buf.WriteU64(uint64(msg.Timestamp.UnixNano()))
	buf.WriteBytes(msg.Payload)
	return protocol.OpRead, buf.Bytes()
}

func (s *Server) handleReadBatch(ctx context.Context, body []byte) (byte, []byte) {
	r := protocol.NewReader(body)
	name, _ := r.ReadBytes()
	start, _ := r.ReadU64()
	count, _ := r.ReadU32()
	storage, err := s.mgr.GetQueue(string(name))
	if err != nil {
		return opErr(protocol.ErrCodeQueueNotFound, err.Error())
	}
	q := logqueue.NewLogQueue(string(name), storage)
	msgs, err := q.ReadBatch(ctx, start, int(count))
	if err != nil {
		if errors.Is(err, engine.ErrOffsetNotFound) {
			return opErr(protocol.ErrCodeOffsetNotFound, err.Error())
		}
		if errors.Is(err, engine.ErrOffsetTruncated) {
			return opErr(protocol.ErrCodeOffsetTruncated, err.Error())
		}
		return opErr(protocol.ErrCodeUnknown, err.Error())
	}
	buf := protocol.Buffer{}
	buf.WriteU32(uint32(len(msgs)))
	for _, msg := range msgs {
		buf.WriteU64(msg.Offset)
		buf.WriteBytes([]byte(msg.ID))
		buf.WriteU64(uint64(msg.Timestamp.UnixNano()))
		buf.WriteBytes(msg.Payload)
	}
	return protocol.OpReadBatch, buf.Bytes()
}

func (s *Server) handleTail(ctx context.Context, body []byte) (byte, []byte) {
	r := protocol.NewReader(body)
	name, _ := r.ReadBytes()
	storage, err := s.mgr.GetQueue(string(name))
	if err != nil {
		return opErr(protocol.ErrCodeQueueNotFound, err.Error())
	}
	q := logqueue.NewLogQueue(string(name), storage)
	last, err := q.LastOffset(ctx)
	if err != nil {
		return opErr(protocol.ErrCodeUnknown, err.Error())
	}
	buf := protocol.Buffer{}
	buf.WriteU64(last)
	return protocol.OpTail, buf.Bytes()
}

func (s *Server) handleCommitOffset(ctx context.Context, body []byte) (byte, []byte) {
	r := protocol.NewReader(body)
	qname, _ := r.ReadBytes()
	gname, _ := r.ReadBytes()
	off, _ := r.ReadU64()
	storage, err := s.mgr.GetQueue(string(qname))
	if err != nil {
		return opErr(protocol.ErrCodeQueueNotFound, err.Error())
	}
	q := logqueue.NewLogQueue(string(qname), storage)
	if err := q.CommitOffset(ctx, string(gname), off); err != nil {
		return opErr(protocol.ErrCodeUnknown, err.Error())
	}
	return protocol.OpCommitOffset, nil
}

func (s *Server) handleFetchOffset(ctx context.Context, body []byte) (byte, []byte) {
	r := protocol.NewReader(body)
	qname, _ := r.ReadBytes()
	gname, _ := r.ReadBytes()
	storage, err := s.mgr.GetQueue(string(qname))
	if err != nil {
		return opErr(protocol.ErrCodeQueueNotFound, err.Error())
	}
	q := logqueue.NewLogQueue(string(qname), storage)
	off, err := q.FetchOffset(ctx, string(gname))
	if err != nil {
		return opErr(protocol.ErrCodeUnknown, err.Error())
	}
	buf := protocol.Buffer{}
	buf.WriteU64(off)
	return protocol.OpFetchOffset, buf.Bytes()
}

func (s *Server) handleListQueues() (byte, []byte) {
	names := s.mgr.ListQueues()
	buf := protocol.Buffer{}
	buf.WriteU32(uint32(len(names)))
	for _, n := range names {
		buf.WriteBytes([]byte(n))
	}
	return protocol.OpListQueues, buf.Bytes()
}

func (s *Server) handleDeleteQueue(body []byte) (byte, []byte) {
	r := protocol.NewReader(body)
	name, _ := r.ReadBytes()
	if err := s.mgr.DeleteQueue(string(name)); err != nil {
		return opErr(protocol.ErrCodeQueueNotFound, err.Error())
	}
	return protocol.OpDeleteQueue, nil
}

func (s *Server) handleTruncate(ctx context.Context, body []byte) (byte, []byte) {
	r := protocol.NewReader(body)
	name, err := r.ReadBytes()
	if err != nil {
		return opErr(protocol.ErrCodeUnknown, err.Error())
	}
	minOffset, err := r.ReadU64()
	if err != nil {
		return opErr(protocol.ErrCodeUnknown, err.Error())
	}

	storage, err := s.mgr.GetQueue(string(name))
	if err != nil {
		if errors.Is(err, engine.ErrQueueNotFound) {
			return opErr(protocol.ErrCodeQueueNotFound, err.Error())
		}
		return opErr(protocol.ErrCodeUnknown, err.Error())
	}

	reclaimed, err := storage.TruncateBefore(ctx, minOffset)
	if err != nil {
		return opErr(protocol.ErrCodeUnknown, err.Error())
	}

	buf := protocol.Buffer{}
	buf.WriteU64(uint64(reclaimed))
	return protocol.OpTruncate, buf.Bytes()
}

func (s *Server) handleStats() (byte, []byte) {
	st := s.mgr.Stats()
	buf := protocol.Buffer{}
	buf.WriteU32(uint32(st.ActiveQueues))
	buf.WriteU64(uint64(st.TotalBytes))
	return protocol.OpStats, buf.Bytes()
}

func opErr(code byte, msg string) (byte, []byte) {
	return protocol.OpError, protocol.EncodeError(code, msg)
}
