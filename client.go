package mikyu

import (
	"bufio"
	"errors"
	"fmt"
	"net"

	"github.com/dmunish/mikyu/internal/protocol"
)

type Client struct {
	conn net.Conn
	r    *bufio.Reader
}

// Connects to a mikyud server at addr.
func Connect(addr string) (*Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &Client{conn: conn, r: bufio.NewReader(conn)}, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) CreateQueue(name string, capacity int) error {
	var body protocol.Buffer
	body.WriteBytes([]byte(name))
	body.WriteU32(uint32(capacity))

	_, err := c.exchange(protocol.OpCreateQueue, body.Bytes())
	return err
}

func (c *Client) Produce(queue string, payload []byte) (string, error) {
	var body protocol.Buffer
	body.WriteBytes([]byte(queue))
	body.WriteBytes(payload)

	resp, err := c.exchange(protocol.OpProduce, body.Bytes())
	if err != nil {
		return "", err
	}

	r := protocol.NewReader(resp)
	id, err := r.ReadBytes()
	if err != nil {
		return "", fmt.Errorf("invalid response payload: %w", err)
	}
	return string(id), nil
}

func (c *Client) Consume(queue string) (id string, payload []byte, err error) {
	var body protocol.Buffer
	body.WriteBytes([]byte(queue))

	resp, err := c.exchange(protocol.OpConsume, body.Bytes())
	if err != nil {
		return "", nil, err
	}

	r := protocol.NewReader(resp)
	idBytes, err := r.ReadBytes()
	if err != nil {
		return "", nil, fmt.Errorf("invalid response ID: %w", err)
	}
	payload, err = r.ReadBytes()
	if err != nil {
		return "", nil, fmt.Errorf("invalid response payload: %w", err)
	}

	return string(idBytes), payload, nil
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
		return nil, errors.New(string(respBody))
	}
	if respOp != op {
		return nil, fmt.Errorf("unexpected response opcode: 0x%02x (expected 0x%02x)", respOp, op)
	}

	return respBody, nil
}
