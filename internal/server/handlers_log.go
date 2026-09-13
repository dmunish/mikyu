package server

import (
	"context"
	"errors"

	"github.com/dmunish/mikyu/internal/engine"
	"github.com/dmunish/mikyu/internal/transport/protocol"
)

func (s *Server) handleAppend(ctx context.Context, body []byte) (byte, []byte) {
	r := protocol.NewReader(body)
	name, _ := r.ReadBytes()
	payload, _ := r.ReadBytes()
	q, err := s.getOrCreateLogQueue(string(name))
	if err != nil {
		return opErr(protocol.ErrCodeQueueNotFound, err.Error())
	}
	off, err := q.Append(ctx, payload)
	if err != nil {
		return taskErr(err)
	}
	buf := protocol.Buffer{}
	buf.WriteU64(off)
	return protocol.OpAppend, buf.Bytes()
}

func (s *Server) handleAppendBatch(ctx context.Context, body []byte) (byte, []byte) {
	r := protocol.NewReader(body)
	name, _ := r.ReadBytes()
	count, err := r.ReadU32()
	if err != nil || count > 65536 {
		return opErr(protocol.ErrCodeUnknown, "invalid batch count")
	}
	payloads := make([][]byte, count)
	for i := range count {
		payloads[i], _ = r.ReadBytes()
	}
	q, err := s.getOrCreateLogQueue(string(name))
	if err != nil {
		return opErr(protocol.ErrCodeQueueNotFound, err.Error())
	}
	offs, err := q.AppendBatch(ctx, payloads)
	if err != nil {
		return taskErr(err)
	}
	buf := protocol.Buffer{}
	buf.WriteU32(uint32(len(offs)))
	for _, off := range offs {
		buf.WriteU64(off)
	}
	return protocol.OpAppendBatch, buf.Bytes()
}

func (s *Server) handleRead(ctx context.Context, body []byte) (byte, []byte) {
	r := protocol.NewReader(body)
	name, _ := r.ReadBytes()
	off, _ := r.ReadU64()
	q, err := s.getOrCreateLogQueue(string(name))
	if err != nil {
		return opErr(protocol.ErrCodeQueueNotFound, err.Error())
	}
	msg, err := q.Read(ctx, off)
	if err != nil {
		return logReadErr(err)
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
	count, err := r.ReadU32()
	if err != nil || count > 65536 {
		return opErr(protocol.ErrCodeUnknown, "invalid batch count")
	}
	q, err := s.getOrCreateLogQueue(string(name))
	if err != nil {
		return opErr(protocol.ErrCodeQueueNotFound, err.Error())
	}
	msgs, err := q.ReadBatch(ctx, start, int(count))
	if err != nil {
		return logReadErr(err)
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

func (s *Server) handleLastOffset(ctx context.Context, body []byte) (byte, []byte) {
	r := protocol.NewReader(body)
	name, _ := r.ReadBytes()
	q, err := s.getOrCreateLogQueue(string(name))
	if err != nil {
		return opErr(protocol.ErrCodeQueueNotFound, err.Error())
	}
	last, err := q.LastOffset(ctx)
	if err != nil {
		return taskErr(err)
	}
	buf := protocol.Buffer{}
	buf.WriteU64(last)
	return protocol.OpLastOffset, buf.Bytes()
}

func (s *Server) handleCommit(ctx context.Context, body []byte) (byte, []byte) {
	r := protocol.NewReader(body)
	qname, _ := r.ReadBytes()
	gname, _ := r.ReadBytes()
	off, _ := r.ReadU64()
	q, err := s.getOrCreateLogQueue(string(qname))
	if err != nil {
		return opErr(protocol.ErrCodeQueueNotFound, err.Error())
	}
	if err := q.CommitOffset(ctx, string(gname), off); err != nil {
		return taskErr(err)
	}
	return protocol.OpCommit, nil
}

func (s *Server) handleCommitted(ctx context.Context, body []byte) (byte, []byte) {
	r := protocol.NewReader(body)
	qname, _ := r.ReadBytes()
	gname, _ := r.ReadBytes()
	q, err := s.getOrCreateLogQueue(string(qname))
	if err != nil {
		return opErr(protocol.ErrCodeQueueNotFound, err.Error())
	}
	off, err := q.Committed(ctx, string(gname))
	if err != nil {
		return taskErr(err)
	}
	buf := protocol.Buffer{}
	buf.WriteU64(off)
	return protocol.OpCommitted, buf.Bytes()
}

// logReadErr maps log read errors, which mix offset problems and generic
// storage failures.
func logReadErr(err error) (byte, []byte) {
	if errors.Is(err, engine.ErrOffsetNotFound) {
		return opErr(protocol.ErrCodeOffsetNotFound, err.Error())
	}
	if errors.Is(err, engine.ErrOffsetTruncated) {
		return opErr(protocol.ErrCodeOffsetTruncated, err.Error())
	}
	return taskErr(err)
}
