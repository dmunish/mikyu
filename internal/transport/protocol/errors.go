package protocol

import "errors"

// Error codes are re-blocked by subsystem (code >> 4):
//
//	0x01-0x0F admin, 0x11-0x1F storage, 0x21-0x2F task, 0x31-0x3F dead-letter,
//	0x41-0x4F server.
//
// Error frames (OpError) carry one of these codes plus a human-readable
// message; the code is the stable contract, the message is not.
const (
	// Admin block: queue lifecycle and spec-carrying frames.
	ErrCodeUnknown        byte = 0x01
	ErrCodeQueueNotFound  byte = 0x02
	ErrCodeQueueExists    byte = 0x03
	ErrCodeInvalidConfig  byte = 0x04
	ErrCodeImmutableField byte = 0x05
	ErrCodeSpecTooNew     byte = 0x06
	ErrCodeSpecTooLarge   byte = 0x07
	ErrCodeUnauthorized   byte = 0x09

	// Storage block: log-segment reads and queue availability.
	ErrCodeOffsetNotFound   byte = 0x11
	ErrCodeOffsetTruncated  byte = 0x12
	ErrCodeMessageTooLarge  byte = 0x13
	ErrCodeQueueClosed      byte = 0x14
	ErrCodeNoData           byte = 0x15
	ErrCodeDiskFull         byte = 0x16
	ErrCodeCorruptRecord    byte = 0x17
	ErrCodeRecordTooLarge   byte = 0x18
	ErrCodeRecordCompacted  byte = 0x19
	ErrCodeEncryptionKey    byte = 0x1A

	// Task block: enqueue/dequeue and lease operations.
	ErrCodeTaskNotAvailable  byte = 0x21
	ErrCodeTaskNotFound      byte = 0x22
	ErrCodeTaskNotInflight   byte = 0x23
	ErrCodeTaskStale         byte = 0x24
	ErrCodeTaskInvalid       byte = 0x25
	ErrCodeCheckpointCorrupt byte = 0x26
	ErrCodeQueueFull         byte = 0x27

	// Dead-letter block.
	ErrCodeDeadLetterNotFound byte = 0x31
	ErrCodeDeadLetterStale    byte = 0x32
	ErrCodeArchiveFailed      byte = 0x33

	// Server block: connection admission and queue-kind enforcement.
	ErrCodeTooManyConnections byte = 0x41
	ErrCodeWrongKind          byte = 0x42
)

// ErrTooManyConnections reports that a server is at its concurrent-connection limit.
var ErrTooManyConnections = errors.New("too many connections")

// ErrUnauthorized reports a rejected or missing credential.
var ErrUnauthorized = errors.New("unauthorized")

// EncodeError encodes an error byte code and string message into a wire payload.
func EncodeError(code byte, msg string) []byte {
	buf := make([]byte, 1+len(msg))
	buf[0] = code
	copy(buf[1:], msg)
	return buf
}

// DecodeError decodes an error byte code and string message from a response payload.
func DecodeError(body []byte) (code byte, msg string) {
	if len(body) == 0 {
		return ErrCodeUnknown, "unknown error"
	}
	return body[0], string(body[1:])
}
