package server_test

import (
	"bufio"
	"errors"
	"net"
	"sync"
	"testing"

	"github.com/dmunish/mikyu/internal/protocol"
	"github.com/dmunish/mikyu/internal/server"
	"github.com/dmunish/mikyu/queue"
)

// pipeListener yields exactly one pre-made connection from Accept, so the
// server can be driven over an in-memory net.Pipe without binding ports.
type pipeListener struct {
	conn   net.Conn
	closed chan struct{}
	once   sync.Once
}

func newPipeListener() (*pipeListener, net.Conn) {
	srvConn, cliConn := net.Pipe()
	return &pipeListener{conn: srvConn, closed: make(chan struct{})}, cliConn
}

func (l *pipeListener) Accept() (net.Conn, error) {
	delivered := false
	l.once.Do(func() { delivered = true })
	if delivered {
		return l.conn, nil
	}
	<-l.closed // no more connections; block until Close
	return nil, errors.New("listener closed")
}

func (l *pipeListener) Close() error {
	close(l.closed)
	return nil
}

func (l *pipeListener) Addr() net.Addr { return pipeAddr{} }

type pipeAddr struct{}

func (pipeAddr) Network() string { return "pipe" }
func (pipeAddr) String() string  { return "pipe" }

// startPipedServer starts a Server backed by a fresh Manager and returns the
// client end of the pipe plus a cleanup func.
func startPipedServer(t *testing.T) (net.Conn, *queue.Manager) {
	t.Helper()

	mgr, err := queue.Open(t.TempDir())
	if err != nil {
		t.Fatalf("queue.Open: %v", err)
	}
	t.Cleanup(func() { mgr.Close() })

	ln, cliConn := newPipeListener()
	go server.New(mgr).Serve(ln)
	t.Cleanup(func() { ln.Close() })

	return cliConn, mgr
}

// roundTrip sends one request frame and reads one response frame.
func roundTrip(t *testing.T, conn net.Conn, op byte, body []byte) (byte, []byte) {
	t.Helper()

	if err := protocol.WriteFrame(conn, op, body); err != nil {
		t.Fatalf("WriteFrame(op=0x%02x): %v", op, err)
	}
	respOp, respBody, err := protocol.ReadFrame(bufio.NewReader(conn))
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	return respOp, respBody
}

func TestDispatch_CreateQueueProduceAndReadTail(t *testing.T) {
	conn, _ := startPipedServer(t)

	// Create queue.
	var createBody protocol.Buffer
	createBody.WriteBytes([]byte("orders"))
	op, body := roundTrip(t, conn, protocol.OpCreateQueue, createBody.Bytes())
	if op == protocol.OpError {
		t.Fatalf("CreateQueue returned error frame: %q", string(body))
	}
	if op != protocol.OpCreateQueue {
		t.Errorf("CreateQueue resp op = 0x%02x, want 0x%02x", op, protocol.OpCreateQueue)
	}

	// Produce.
	var produceBody protocol.Buffer
	produceBody.WriteBytes([]byte("orders"))
	produceBody.WriteBytes([]byte("hello"))
	op, body = roundTrip(t, conn, protocol.OpProduce, produceBody.Bytes())
	if op == protocol.OpError {
		t.Fatalf("Produce returned error frame: %q", string(body))
	}
	offset, err := protocol.NewReader(body).ReadU64()
	if err != nil || offset != 0 {
		t.Errorf("Produce offset = %d, %v; want 0, <nil>", offset, err)
	}

	// Read back.
	var readBody protocol.Buffer
	readBody.WriteBytes([]byte("orders"))
	readBody.WriteU64(0)
	op, body = roundTrip(t, conn, protocol.OpRead, readBody.Bytes())
	if op == protocol.OpError {
		t.Fatalf("Read returned error frame: %q", string(body))
	}
	r := protocol.NewReader(body)
	msgOffset, _ := r.ReadU64()
	id, _ := r.ReadBytes()
	_, _ = r.ReadU64()
	payload, _ := r.ReadBytes()
	if len(id) == 0 || string(payload) != "hello" || msgOffset != 0 {
		t.Errorf("Read = id:%q payload:%q offset:%d, want non-empty id, %q, 0", id, payload, msgOffset, "hello")
	}

	// Tail.
	var tailBody protocol.Buffer
	tailBody.WriteBytes([]byte("orders"))
	op, body = roundTrip(t, conn, protocol.OpTail, tailBody.Bytes())
	if op == protocol.OpError {
		t.Fatalf("Tail returned error frame: %q", string(body))
	}
	tail, err := protocol.NewReader(body).ReadU64()
	if err != nil || tail != 1 {
		t.Errorf("LastOffset = %d, %v; want 1, <nil>", tail, err)
	}
}

func TestDispatch_ErrorCodes(t *testing.T) {
	conn, _ := startPipedServer(t)

	tests := []struct {
		name     string
		op       byte
		build    func() []byte
		wantCode byte
	}{
		{
			name: "produce to missing queue",
			op:   protocol.OpProduce,
			build: func() []byte {
				var b protocol.Buffer
				b.WriteBytes([]byte("missing"))
				b.WriteBytes([]byte("x"))
				return b.Bytes()
			},
			wantCode: protocol.ErrCodeQueueNotFound,
		},
		{
			name: "read past tail",
			op:   protocol.OpRead,
			build: func() []byte {
				var b protocol.Buffer
				b.WriteBytes([]byte("missing"))
				b.WriteU64(99)
				return b.Bytes()
			},
			wantCode: protocol.ErrCodeQueueNotFound,
		},
		{
			name: "malformed body",
			op:   protocol.OpProduce,
			build: func() []byte {
				return []byte{0xFF} // length prefix claims 255 bytes, none follow
			},
			wantCode: protocol.ErrCodeUnknown,
		},
		{
			name:     "unknown opcode",
			op:       0x7F,
			build:    func() []byte { return nil },
			wantCode: protocol.ErrCodeUnknown,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			respOp, respBody := roundTrip(t, conn, tt.op, tt.build())
			if respOp != protocol.OpError {
				t.Fatalf("resp op = 0x%02x, want OpError", respOp)
			}
			code, _ := protocol.DecodeError(respBody)
			if code != tt.wantCode {
				t.Errorf("error code = %d, want %d", code, tt.wantCode)
			}
		})
	}
}

func TestDispatch_DuplicateCreateQueue(t *testing.T) {
	conn, _ := startPipedServer(t)

	var body protocol.Buffer
	body.WriteBytes([]byte("dup"))

	op, respBody := roundTrip(t, conn, protocol.OpCreateQueue, body.Bytes())
	if op == protocol.OpError {
		t.Fatalf("first CreateQueue failed: %q", string(respBody))
	}

	op, respBody = roundTrip(t, conn, protocol.OpCreateQueue, body.Bytes())
	if op != protocol.OpError {
		t.Fatal("second CreateQueue should return an error frame")
	}
	code, msg := protocol.DecodeError(respBody)
	if code != protocol.ErrCodeQueueExists {
		t.Errorf("error code = %d, want ErrCodeQueueExists", code)
	}
	if msg == "" {
		t.Error("error message is empty")
	}
}
