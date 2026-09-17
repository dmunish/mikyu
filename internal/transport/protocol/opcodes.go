package protocol

import (
	"fmt"
	"net/url"
	"strings"
)

// Opcodes are grouped by subsystem nibble: dispatch is op >> 4. The table is
// frozen; opcodes are never reused. Admin frames that carry a spec also carry
// their own u16 specVersion (the JSON payload's schema number, not a protocol
// version).
const (
	// SubsystemAdmin selects the 0x00-0x0F group.
	SubsystemAdmin byte = 0x0
	// SubsystemLog selects the 0x10-0x1F group.
	SubsystemLog byte = 0x1
	// SubsystemTask selects the 0x20-0x2F group.
	SubsystemTask byte = 0x2
	// SubsystemDeadLetter selects the 0x30-0x3F group.
	SubsystemDeadLetter byte = 0x3
	// SubsystemProtocol selects the 0x70-0x7F group.
	SubsystemProtocol byte = 0x7
	// SubsystemError selects the 0xF0-0xFF group.
	SubsystemError byte = 0xF
)

const (
	// Admin: queue lifecycle and system facts.

	OpCreate   byte = 0x00 // u8 kind + bytes(name) + u16 specVersion + bytes(json)
	OpDelete   byte = 0x01 // bytes(name)
	OpList     byte = 0x02 // -> u32 count, then per entry bytes(name) u8 kind
	OpUpdate   byte = 0x03 // bytes(name) + u16 specVersion + bytes(json patch)
	OpDescribe byte = 0x04 // bytes(name) -> u16 specVersion + bytes(json)
	OpTruncate byte = 0x05 // bytes(name) + u64 minOffset
	OpMetrics  byte = 0x07 // -> u16 specVersion + bytes(json snapshot)
	OpSchema   byte = 0x08 // -> u16 specVersion + bytes(json table)

	// Log: append/read/tail and consumer-group offsets.

	OpAppend      byte = 0x10
	OpAppendBatch byte = 0x11
	OpRead        byte = 0x12
	OpReadBatch   byte = 0x13
	OpLastOffset  byte = 0x14
	OpMinOffset   byte = 0x15
	OpCommit      byte = 0x16
	OpCommitted   byte = 0x17
	OpFetch       byte = 0x18

	// Task: enqueue/dequeue and lease operations.

	OpEnqueue byte = 0x20
	OpDequeue byte = 0x21
	OpAck     byte = 0x22
	OpNack    byte = 0x23
	OpExtend  byte = 0x24
	OpArchive byte = 0x25

	// Dead-letter: durable per-queue dead letters.

	OpDeadLetters   byte = 0x30
	OpDeadLetterAck byte = 0x31

	// Protocol-level ops.

	OpShutdown byte = 0x0C
	OpError    byte = 0xFF // u8 code + bytes(message); see errors.go
	OpAuth     byte = 0x7E // first frame on a connection; u8 method + bytes(credential); see auth.go
)

// ParseTarget converts target strings like "tcp://127.0.0.1:7420", "unix:///tmp/mikyu.sock", or "pipe://./pipe/mikyu" into network and address components for net.Dial/Listen.
func ParseTarget(target string) (network string, addr string, err error) {
	if after, ok := strings.CutPrefix(target, "unix://"); ok {
		return "unix", after, nil
	}
	if after, ok := strings.CutPrefix(target, "pipe://"); ok {
		return "pipe", after, nil
	}
	if after, ok := strings.CutPrefix(target, "tcp://"); ok {
		return "tcp", after, nil
	}
	// No scheme: treat as tcp host:port
	if !strings.Contains(target, "://") {
		return "tcp", target, nil
	}
	u, err := url.Parse(target)
	if err != nil {
		return "", "", fmt.Errorf("invalid target URI %q: %w", target, err)
	}
	switch u.Scheme {
	case "tcp", "unix", "pipe":
		return u.Scheme, u.Host, nil
	default:
		return "", "", fmt.Errorf("unsupported scheme %q", u.Scheme)
	}
}