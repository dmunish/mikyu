package server

import (
	"context"
	"errors"
	"net"
	"time"

	"github.com/dmunish/mikyu/internal/engine"
	"github.com/dmunish/mikyu/internal/queue/logqueue"
	"github.com/dmunish/mikyu/internal/transport/protocol"
)

func (s *Server) handleAppend(ctx context.Context, body []byte) (byte, []byte) {
	r := protocol.NewReader(body)
	name, _ := r.ReadBytes()
	payload, _ := r.ReadBytes()
	group, _ := r.ReadBytes()
	q, err := s.getOrCreateLogQueue(string(name))
	if err != nil {
		return taskErr(err)
	}
	off, err := q.Append(ctx, payload, logqueue.WithKey(group))
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
	group, _ := r.ReadBytes()
	payloads := make([][]byte, count)
	for i := range count {
		payloads[i], _ = r.ReadBytes()
	}
	q, err := s.getOrCreateLogQueue(string(name))
	if err != nil {
		return taskErr(err)
	}
	offs, err := q.AppendBatch(ctx, payloads, logqueue.WithKey(group))
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

const maxFetchWait = 5 * time.Minute

func (s *Server) handleFetch(ctx context.Context, conn net.Conn, body []byte) (byte, []byte, bool) {
	name, start, count, wait, err := protocol.DecodeFetch(protocol.NewReader(body))
	if err != nil {
		op, resp := opErr(protocol.ErrCodeInvalidConfig, err.Error())
		return op, resp, false
	}
	q, err := s.getOrCreateLogQueue(name)
	if err != nil {
		op, resp := taskErr(err)
		return op, resp, false
	}
	if wait > maxFetchWait {
		wait = maxFetchWait
	}
	if s.streamSpans(conn, q, protocol.OpFetch, start, count) {
		return protocol.OpFetch, nil, true
	}
	msgs, err := q.Fetch(ctx, start, count, wait)
	if err != nil {
		op, resp := logReadErr(err)
		return op, resp, false
	}
	recs := make([]protocol.Record, len(msgs))
	for i, msg := range msgs {
		recs[i] = toWireRecord(msg)
	}
	var buf protocol.Buffer
	protocol.WriteRecordList(&buf, recs)
	return protocol.OpFetch, buf.Bytes(), false
}

// toWireRecord converts an engine record to its wire form.
func toWireRecord(msg engine.Record) protocol.Record {
	return protocol.Record{
		Offset:    msg.Offset,
		ID:        msg.ID,
		Timestamp: msg.Timestamp,
		Group:     msg.Group,
		Payload:   msg.Payload,
	}
}

func (s *Server) handleRead(ctx context.Context, body []byte) (byte, []byte) {
	r := protocol.NewReader(body)
	name, _ := r.ReadBytes()
	off, _ := r.ReadU64()
	q, err := s.getOrCreateLogQueue(string(name))
	if err != nil {
		return taskErr(err)
	}
	msg, err := q.Read(ctx, off)
	if err != nil {
		return logReadErr(err)
	}
	buf := protocol.Buffer{}
	protocol.WriteRecord(&buf, toWireRecord(msg))
	return protocol.OpRead, buf.Bytes()
}

func (s *Server) handleReadBatch(ctx context.Context, conn net.Conn, body []byte) (byte, []byte, bool) {
	r := protocol.NewReader(body)
	name, _ := r.ReadBytes()
	start, _ := r.ReadU64()
	count, err := r.ReadU32()
	if err != nil || count > 65536 {
		op, resp := opErr(protocol.ErrCodeUnknown, "invalid batch count")
		return op, resp, false
	}
	q, err := s.getOrCreateLogQueue(string(name))
	if err != nil {
		op, resp := taskErr(err)
		return op, resp, false
	}
	if s.streamSpans(conn, q, protocol.OpReadBatch, start, int(count)) {
		return protocol.OpReadBatch, nil, true
	}
	msgs, err := q.ReadBatch(ctx, start, int(count))
	if err != nil {
		op, resp := logReadErr(err)
		return op, resp, false
	}
	recs := make([]protocol.Record, len(msgs))
	for i, msg := range msgs {
		recs[i] = toWireRecord(msg)
	}
	buf := protocol.Buffer{}
	protocol.WriteRecordList(&buf, recs)
	return protocol.OpReadBatch, buf.Bytes(), false
}

func (s *Server) handleLastOffset(ctx context.Context, body []byte) (byte, []byte) {
	r := protocol.NewReader(body)
	name, _ := r.ReadBytes()
	q, err := s.getOrCreateLogQueue(string(name))
	if err != nil {
		return taskErr(err)
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
		return taskErr(err)
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
		return taskErr(err)
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
	if errors.Is(err, engine.ErrNoData) {
		return opErr(protocol.ErrCodeNoData, err.Error())
	}
	return taskErr(err)
}
