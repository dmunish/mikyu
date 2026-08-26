package protocol_test

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/dmunish/mikyu/internal/protocol"
)

func TestBufferReaderRoundTrip(t *testing.T) {
	t.Parallel()

	var buf protocol.Buffer
	buf.WriteU32(42)
	buf.WriteU64(1<<40 + 7)
	buf.WriteBytes([]byte("mikyu"))
	buf.WriteBytes(nil) // empty slice must round-trip as zero length

	r := protocol.NewReader(buf.Bytes())

	if got, err := r.ReadU32(); err != nil || got != 42 {
		t.Errorf("ReadU32 = %d, %v; want 42, <nil>", got, err)
	}
	if got, err := r.ReadU64(); err != nil || got != 1<<40+7 {
		t.Errorf("ReadU64 = %d, %v; want %d, <nil>", got, err, uint64(1<<40+7))
	}
	got, err := r.ReadBytes()
	if err != nil || !bytes.Equal(got, []byte("mikyu")) {
		t.Errorf("ReadBytes = %q, %v; want %q, <nil>", got, err, "mikyu")
	}
	if got, err := r.ReadBytes(); err != nil || len(got) != 0 {
		t.Errorf("empty ReadBytes = %q, %v; want empty, <nil>", got, err)
	}
}

func TestReaderTruncated(t *testing.T) {
	t.Parallel()

	var full protocol.Buffer
	full.WriteU32(1)
	full.WriteU64(2)
	full.WriteBytes([]byte("abc"))

	tests := []struct {
		name string
		buf  []byte
		read func(*protocol.Reader) error
	}{
		{
			name: "u32 past end",
			buf:  full.Bytes()[:3],
			read: func(r *protocol.Reader) error { _, err := r.ReadU32(); return err },
		},
		{
			name: "u64 straddles end",
			buf:  full.Bytes()[4 : 4+5],
			read: func(r *protocol.Reader) error { _, err := r.ReadU64(); return err },
		},
		{
			name: "bytes length exceeds buffer",
			buf: func() []byte {
				// Hand-build: length prefix claims 10 bytes, only "short" follows.
				var b []byte
				var n [4]byte
				binary.BigEndian.PutUint32(n[:], 10)
				b = append(b, n[:]...)
				return append(b, []byte("short")...)
			}(),
			read: func(r *protocol.Reader) error { _, err := r.ReadBytes(); return err },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := tt.read(protocol.NewReader(tt.buf))
			if !errors.Is(err, protocol.ErrTruncatedBody) {
				t.Errorf("err = %v, want %v", err, protocol.ErrTruncatedBody)
			}
		})
	}
}

func TestFrameRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		op      byte
		payload []byte
	}{
		{name: "produce frame", op: protocol.OpProduce, payload: []byte{0x01, 0x02, 0x03}},
		{name: "ack with empty body", op: protocol.OpCreateQueue, payload: nil},
		{name: "error frame", op: protocol.OpError, payload: protocol.EncodeError(protocol.ErrCodeQueueNotFound, "queue not found")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			raw := &bytes.Buffer{}
			if err := protocol.WriteFrame(raw, tt.op, tt.payload); err != nil {
				t.Fatalf("WriteFrame: %v", err)
			}

			op, body, err := protocol.ReadFrame(bufio.NewReader(raw))
			if err != nil {
				t.Fatalf("ReadFrame: %v", err)
			}
			if op != tt.op {
				t.Errorf("op = 0x%02x, want 0x%02x", op, tt.op)
			}
			if !bytes.Equal(body, tt.payload) {
				t.Errorf("body = %v, want %v", body, tt.payload)
			}
		})
	}
}

func TestWriteFrameLengthPrefix(t *testing.T) {
	t.Parallel()

	payload := []byte("xyz")
	raw := &bytes.Buffer{}
	if err := protocol.WriteFrame(raw, protocol.OpTail, payload); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}
	// Wire format: 4-byte length covering opcode + payload.
	if raw.Len() != 4+1+len(payload) {
		t.Fatalf("frame size = %d bytes, want %d", raw.Len(), 4+1+len(payload))
	}
}

func TestErrorEncodeDecode(t *testing.T) {
	t.Parallel()

	body := protocol.EncodeError(protocol.ErrCodeQueueExists, "dup")
	code, msg := protocol.DecodeError(body)
	if code != protocol.ErrCodeQueueExists || msg != "dup" {
		t.Errorf("DecodeError = (%d, %q), want (%d, %q)", code, msg, protocol.ErrCodeQueueExists, "dup")
	}

	code, msg = protocol.DecodeError(nil)
	if code != protocol.ErrCodeUnknown || msg != "" {
		t.Errorf("DecodeError(nil) = (%d, %q), want (%d, %q)", code, msg, protocol.ErrCodeUnknown, "")
	}
}
