package protocol

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseTarget(t *testing.T) {
	tests := []struct {
		target  string
		network string
		addr    string
		err     bool
	}{
		{"tcp://127.0.0.1:7420", "tcp", "127.0.0.1:7420", false},
		{"unix:///tmp/mikyu.sock", "unix", "/tmp/mikyu.sock", false},
		{"pipe://./pipe/mikyu", "pipe", "./pipe/mikyu", false},
		{"127.0.0.1:7420", "tcp", "127.0.0.1:7420", false},
		{"invalid://", "", "", true},
	}
	for _, tt := range tests {
		t.Run(tt.target, func(t *testing.T) {
			network, addr, err := ParseTarget(tt.target)
			if tt.err {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.network, network)
				assert.Equal(t, tt.addr, addr)
			}
		})
	}
}

func TestEncodeDecodeError(t *testing.T) {
	body := EncodeError(ErrCodeQueueNotFound, "queue not found")
	code, msg := DecodeError(body)
	assert.Equal(t, ErrCodeQueueNotFound, code)
	assert.Equal(t, "queue not found", msg)
}

func TestDecodeErrorEmpty(t *testing.T) {
	code, msg := DecodeError(nil)
	assert.Equal(t, ErrCodeUnknown, code)
	assert.Equal(t, "unknown error", msg)
}

func TestOpcodeNibbleGrouping(t *testing.T) {
	cases := []struct {
		subsystem byte
		ops       []byte
	}{
		{SubsystemAdmin, []byte{OpCreateQueue, OpDeleteQueue, OpListQueues, OpUpdateQueue, OpDescribeQueue, OpTruncate, OpInfo}},
		{SubsystemLog, []byte{OpAppend, OpAppendBatch, OpRead, OpReadBatch, OpLastOffset, OpCommit, OpCommitted, OpFetch}},
		{SubsystemTask, []byte{OpEnqueue, OpDequeue, OpAck, OpNack, OpExtend, OpDeadLetters, OpDeadLetterAck}},
	}
	seen := make(map[byte]bool)
	for _, c := range cases {
		for _, op := range c.ops {
			assert.Equalf(t, c.subsystem, op>>4, "opcode 0x%02X outside subsystem 0x%X", op, c.subsystem)
			assert.Falsef(t, seen[op], "duplicate opcode 0x%02X", op)
			seen[op] = true
		}
	}
	// Protocol-level ops must not collide with the frozen table either.
	for _, op := range []byte{OpShutdown, OpError, OpReservedHandshake} {
		assert.Falsef(t, seen[op], "protocol-level opcode 0x%02X collides with the queue-op table", op)
	}
}
