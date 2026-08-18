package tcp

import (
	"bufio"
	"errors"
	"fmt"
	"net"

	"github.com/dmunish/mikyu/internal/queue"
)

const maxCapacity = 1000000

type Server struct {
	mgr *queue.Manager
}

func Mikyu() *Server {
	return &Server{mgr: queue.NewManager()}
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
		go s.handleConnection(conn)
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()
	r := bufio.NewReader(conn)

	for {
		op, body, err := readFrame(r)
		if err != nil {
			return
		}

		respOp, respBody := s.processRequest(op, body)
		if err := writeFrame(conn, respOp, respBody); err != nil {
			return
		}
	}
}

func (s *Server) processRequest(op byte, body []byte) (byte, []byte) {
	switch op {
	case OpCreateQueue:
		return s.handleCreate(body)
	case OpProduce:
		return s.handleProduce(body)
	case OpConsume:
		return s.handleConsume(body)
	default:
		return OpError, fmt.Appendf(nil, "unknown opcode 0x%02x", op)
	}
}

func (s *Server) handleCreate(body []byte) (byte, []byte) {
	r := NewReader(body)
	queueName, err := r.ReadBytes()
	if err != nil {
		return OpError, []byte("invalid queue name")
	}

	capacity, err := r.ReadU32()
	if err != nil {
		return OpError, []byte("invalid queue capacity")
	}

	cap := int(capacity)
	if cap <= 0 {
		cap = maxCapacity
	}

	if _, err := s.mgr.NewQueue(string(queueName), cap); err != nil {
		if errors.Is(err, queue.ErrQueueExists) {
			return OpError, []byte(err.Error())
		}
		return OpError, []byte("failed to create queue")
	}

	return OpCreateQueue, nil
}

func (s *Server) handleProduce(body []byte) (byte, []byte) {
	r := NewReader(body)
	queueName, err := r.ReadBytes()
	if err != nil {
		return OpError, []byte("invalid queue name")
	}

	payload, err := r.ReadBytes()
	if err != nil {
		return OpError, []byte("invalid message payload")
	}

	q, err := s.mgr.GetQueue(string(queueName))
	if err != nil {
		return OpError, []byte(err.Error())
	}

	id, err := q.Push(payload)
	if err != nil {
		return OpError, []byte("push failed")
	}

	var resp Buffer
	resp.WriteBytes([]byte(id))
	return OpProduce, resp.Bytes()
}

func (s *Server) handleConsume(body []byte) (byte, []byte) {
	r := NewReader(body)
	queueName, err := r.ReadBytes()
	if err != nil {
		return OpError, []byte("invalid queue name")
	}

	q, err := s.mgr.GetQueue(string(queueName))
	if err != nil {
		return OpError, []byte(err.Error())
	}

	msg, err := q.Pop()
	if err != nil {
		return OpError, []byte(err.Error())
	}

	var resp Buffer
	resp.WriteBytes([]byte(msg.ID))
	resp.WriteBytes(msg.Payload)
	return OpConsume, resp.Bytes()
}
