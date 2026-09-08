package protocol

import (
	"fmt"
	"net/url"
	"strings"
)

const (
	OpCreateQueue       byte = 0x01
	OpProduce           byte = 0x02
	OpRead              byte = 0x03
	OpTail              byte = 0x04
	OpCommitOffset      byte = 0x05
	OpFetchOffset       byte = 0x06
	OpProduceBatch      byte = 0x07
	OpReadBatch         byte = 0x08
	OpListQueues        byte = 0x09
	OpDeleteQueue       byte = 0x0A
	OpStats             byte = 0x0B
	OpTaskEnqueue       byte = 0x0D
	OpTaskDequeue       byte = 0x0E
	OpTaskAck           byte = 0x0F
	OpTaskFail          byte = 0x10
	OpTruncate          byte = 0x11
	OpTaskExtend        byte = 0x12
	OpShutdown          byte = 0x0C
	OpTaskDeadLetters   byte = 0x13
	OpTaskDeadLetterAck byte = 0x14
	OpError             byte = 0xFF
)

const (
	ErrCodeUnknown            byte = 0x01
	ErrCodeQueueNotFound      byte = 0x02
	ErrCodeQueueExists        byte = 0x03
	ErrCodeOffsetNotFound     byte = 0x04
	ErrCodeOffsetTruncated    byte = 0x05
	ErrCodeTaskNotAvailable   byte = 0x06
	ErrCodeTaskNotFound       byte = 0x07
	ErrCodeTaskNotInflight    byte = 0x08
	ErrCodeTaskStale          byte = 0x09
	ErrCodeTaskClosed         byte = 0x0A
	ErrCodeInvalidTask        byte = 0x0B
	ErrCodeDeadLetterNotFound byte = 0x0C
	ErrCodeDeadLetterStale    byte = 0x0D
)

// EncodeError encodes an error byte code and string message into a wire byte slice.
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

// ParseTarget converts target strings like "tcp://127.0.0.1:7420", "unix:///tmp/mikyu.sock",
// or "pipe://./pipe/mikyu" into network and address components for net.Dial/Listen.
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
