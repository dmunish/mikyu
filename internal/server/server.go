package server

import (
	"bufio"
	"errors"
	"fmt"
	"net"

	"github.com/dmunish/mikyu/internal/protocol"
	"github.com/dmunish/mikyu/internal/queue"
)

const defaultCapacity = 1_000_000

type Server struct {
	mgr *queue.Manager
}

func New(mgr *queue.Manager) *Server {
	return &Server{mgr: mgr}
}

func (s *Server) Start(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer ln.Close()

	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		go s.serve(conn)
	}
}

func (s *Server) serve(conn net.Conn) {
	defer conn.Close()
	r := bufio.NewReader(conn)

	for {
		op, body, err := protocol.ReadFrame(r)
		if err != nil {
			return
		}

		respOp, respBody := s.dispatch(op, body)
		if err := protocol.WriteFrame(conn, respOp, respBody); err != nil {
			return
		}
	}
}

func (s *Server) dispatch(op byte, body []byte) (byte, []byte) {
	switch op {
	case protocol.OpCreateQueue:
		return s.createQueue(body)
	case protocol.OpProduce:
		return s.produce(body)
	case protocol.OpConsume:
		return s.consume(body)
	default:
		return protocol.OpError, fmt.Appendf(nil, "unknown opcode 0x%02x", op)
	}
}

func (s *Server) createQueue(body []byte) (byte, []byte) {
	r := protocol.NewReader(body)
	name, err := r.ReadBytes()
	if err != nil {
		return protocol.OpError, []byte("invalid queue name")
	}

	capacity, err := r.ReadU32()
	if err != nil {
		return protocol.OpError, []byte("invalid queue capacity")
	}

	n := int(capacity)
	if n <= 0 {
		n = defaultCapacity
	}

	if _, err := s.mgr.NewQueue(string(name), n); err != nil {
		if errors.Is(err, queue.ErrQueueExists) {
			return protocol.OpError, []byte(err.Error())
		}
		return protocol.OpError, []byte("failed to create queue")
	}

	return protocol.OpCreateQueue, nil
}

func (s *Server) produce(body []byte) (byte, []byte) {
	r := protocol.NewReader(body)
	name, err := r.ReadBytes()
	if err != nil {
		return protocol.OpError, []byte("invalid queue name")
	}

	payload, err := r.ReadBytes()
	if err != nil {
		return protocol.OpError, []byte("invalid message payload")
	}

	q, err := s.mgr.GetQueue(string(name))
	if err != nil {
		return protocol.OpError, []byte(err.Error())
	}

	id, err := q.Push(payload)
	if err != nil {
		return protocol.OpError, []byte("push failed")
	}

	var resp protocol.Buffer
	resp.WriteBytes([]byte(id))
	return protocol.OpProduce, resp.Bytes()
}

func (s *Server) consume(body []byte) (byte, []byte) {
	r := protocol.NewReader(body)
	name, err := r.ReadBytes()
	if err != nil {
		return protocol.OpError, []byte("invalid queue name")
	}

	q, err := s.mgr.GetQueue(string(name))
	if err != nil {
		return protocol.OpError, []byte(err.Error())
	}

	msg, err := q.Pop()
	if err != nil {
		return protocol.OpError, []byte(err.Error())
	}

	var resp protocol.Buffer
	resp.WriteBytes([]byte(msg.ID))
	resp.WriteBytes(msg.Payload)
	return protocol.OpConsume, resp.Bytes()
}
