package server

import (
	"bufio"
	"fmt"
	"net"

	"github.com/dmunish/mikyu/internal/protocol"
	"github.com/dmunish/mikyu/internal/queue"
)

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
	case protocol.OpRead:
		return s.read(body)
	case protocol.OpTail:
		return s.tail(body)
	default:
		return protocol.OpError, protocol.EncodeError(
			protocol.ErrCodeUnknown, fmt.Sprintf("unknown opcode 0x%02x", op))
	}
}
