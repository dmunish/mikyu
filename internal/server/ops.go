package server

import (
	"context"
	"errors"

	"github.com/dmunish/mikyu/internal/protocol"
	"github.com/dmunish/mikyu/queue"
)

func (s *Server) createQueue(body []byte) (byte, []byte) {
	r := protocol.NewReader(body)
	name, err := r.ReadBytes()
	if err != nil {
		return protocol.OpError, protocol.EncodeError(protocol.ErrCodeUnknown, "invalid queue name")
	}

	if _, err := s.mgr.CreateQueue(string(name)); err != nil {
		if errors.Is(err, queue.ErrQueueExists) {
			return protocol.OpError, protocol.EncodeError(protocol.ErrCodeQueueExists, err.Error())
		}
		return protocol.OpError, protocol.EncodeError(protocol.ErrCodeUnknown, "failed to create queue")
	}

	return protocol.OpCreateQueue, nil
}

func (s *Server) produce(ctx context.Context, body []byte) (byte, []byte) {
	r := protocol.NewReader(body)
	name, err := r.ReadBytes()
	if err != nil {
		return protocol.OpError, protocol.EncodeError(protocol.ErrCodeUnknown, "invalid queue name")
	}
	payload, err := r.ReadBytes()
	if err != nil {
		return protocol.OpError, protocol.EncodeError(protocol.ErrCodeUnknown, "invalid message payload")
	}

	q, err := s.mgr.Queue(string(name))
	if err != nil {
		return protocol.OpError, protocol.EncodeError(protocol.ErrCodeQueueNotFound, err.Error())
	}

	offset, err := q.Produce(ctx, payload)
	if err != nil {
		return protocol.OpError, protocol.EncodeError(protocol.ErrCodeUnknown, "push failed")
	}

	var resp protocol.Buffer
	resp.WriteU64(offset)
	return protocol.OpProduce, resp.Bytes()
}

func (s *Server) read(body []byte) (byte, []byte) {
	r := protocol.NewReader(body)
	name, err := r.ReadBytes()
	if err != nil {
		return protocol.OpError, protocol.EncodeError(protocol.ErrCodeUnknown, "invalid queue name")
	}
	offset, err := r.ReadU64()
	if err != nil {
		return protocol.OpError, protocol.EncodeError(protocol.ErrCodeUnknown, "invalid offset")
	}

	q, err := s.mgr.Queue(string(name))
	if err != nil {
		return protocol.OpError, protocol.EncodeError(protocol.ErrCodeQueueNotFound, err.Error())
	}

	msg, err := q.Read(offset)
	if err != nil {
		if errors.Is(err, queue.ErrOffsetNotFound) {
			return protocol.OpError, protocol.EncodeError(protocol.ErrCodeOffsetNotFound, err.Error())
		}
		return protocol.OpError, protocol.EncodeError(protocol.ErrCodeUnknown, err.Error())
	}

	var resp protocol.Buffer
	resp.WriteBytes([]byte(msg.ID))
	resp.WriteBytes(msg.Payload)
	resp.WriteU64(msg.Offset)
	return protocol.OpRead, resp.Bytes()
}

func (s *Server) tail(body []byte) (byte, []byte) {
	r := protocol.NewReader(body)
	name, err := r.ReadBytes()
	if err != nil {
		return protocol.OpError, protocol.EncodeError(protocol.ErrCodeUnknown, "invalid queue name")
	}

	q, err := s.mgr.Queue(string(name))
	if err != nil {
		return protocol.OpError, protocol.EncodeError(protocol.ErrCodeQueueNotFound, err.Error())
	}

	var resp protocol.Buffer
	resp.WriteU64(q.LastOffset())
	return protocol.OpTail, resp.Bytes()
}
