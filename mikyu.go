package mikyu

import (
	"bufio"
	"errors"
	"fmt"
	"net"

	"github.com/dmunish/mikyu/internal/protocol"
)

var (
	ErrQueueExists    = errors.New("queue with this name already exists")
	ErrQueueNotFound  = errors.New("queue not found")
	ErrOffsetNotFound = errors.New("offset not found")
)

type Message struct {
	Offset  uint64
	ID      string
	Payload []byte
}

type Client struct {
	conn net.Conn
	r    *bufio.Reader
}

func Dial(addr string) (*Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &Client{conn: conn, r: bufio.NewReader(conn)}, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) CreateQueue(name string) error {
	var body protocol.Buffer
	body.WriteBytes([]byte(name))

	_, err := c.exchange(protocol.OpCreateQueue, body.Bytes())
	return err
}

func (c *Client) Produce(queue string, payload []byte) (uint64, error) {
	var body protocol.Buffer
	body.WriteBytes([]byte(queue))
	body.WriteBytes(payload)

	resp, err := c.exchange(protocol.OpProduce, body.Bytes())
	if err != nil {
		return 0, err
	}

	offset, err := protocol.NewReader(resp).ReadU64()
	if err != nil {
		return 0, fmt.Errorf("invalid response payload: %w", err)
	}
	return offset, nil
}

func (c *Client) Read(queue string, offset uint64) (Message, error) {
	var body protocol.Buffer
	body.WriteBytes([]byte(queue))
	body.WriteU64(offset)

	resp, err := c.exchange(protocol.OpRead, body.Bytes())
	if err != nil {
		return Message{}, err
	}

	r := protocol.NewReader(resp)
	id, err := r.ReadBytes()
	if err != nil {
		return Message{}, fmt.Errorf("invalid response ID: %w", err)
	}
	payload, err := r.ReadBytes()
	if err != nil {
		return Message{}, fmt.Errorf("invalid response payload: %w", err)
	}
	respOffset, err := r.ReadU64()
	if err != nil {
		return Message{}, fmt.Errorf("invalid response offset: %w", err)
	}

	return Message{Offset: respOffset, ID: string(id), Payload: payload}, nil
}

func (c *Client) Tail(queue string) (uint64, error) {
	var body protocol.Buffer
	body.WriteBytes([]byte(queue))

	resp, err := c.exchange(protocol.OpTail, body.Bytes())
	if err != nil {
		return 0, err
	}

	offset, err := protocol.NewReader(resp).ReadU64()
	if err != nil {
		return 0, fmt.Errorf("invalid response offset: %w", err)
	}
	return offset, nil
}

func (c *Client) exchange(op byte, body []byte) ([]byte, error) {
	if err := protocol.WriteFrame(c.conn, op, body); err != nil {
		return nil, fmt.Errorf("write failed: %w", err)
	}

	respOp, respBody, err := protocol.ReadFrame(c.r)
	if err != nil {
		return nil, fmt.Errorf("read failed: %w", err)
	}

	if respOp == protocol.OpError {
		return nil, decodeError(respBody)
	}
	if respOp != op {
		return nil, fmt.Errorf("unexpected response opcode: 0x%02x (expected 0x%02x)", respOp, op)
	}

	return respBody, nil
}

func decodeError(body []byte) error {
	code, msg := protocol.DecodeError(body)
	switch code {
	case protocol.ErrCodeQueueExists:
		return ErrQueueExists
	case protocol.ErrCodeQueueNotFound:
		return ErrQueueNotFound
	case protocol.ErrCodeOffsetNotFound:
		return ErrOffsetNotFound
	default:
		return errors.New(msg)
	}
}
