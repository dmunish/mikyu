package command

import (
	"context"
	"io"
	"os"
	"strings"

	"charm.land/lipgloss/v2"
	"github.com/dmunish/mikyu"
	"github.com/dmunish/mikyu/internal/daemon"
	"golang.org/x/term"
)

// GetTerminalWidth returns the width of the active terminal window, checking stdout, stderr, and stdin descriptors.
func GetTerminalWidth() int {
	for _, fd := range []int{int(os.Stdout.Fd()), int(os.Stderr.Fd()), int(os.Stdin.Fd())} {
		if term.IsTerminal(fd) {
			w, _, err := term.GetSize(fd)
			if err == nil && w > 0 {
				return w
			}
		}
	}
	return -1
}

// FormatSingleLinePayload sanitizes raw payload bytes (escaping control characters like newlines and tabs, and stripping non-printable characters) and truncates the string to fit availableWidth using visual cell count.
func FormatSingleLinePayload(payload []byte, availableWidth int) string {
	var sb strings.Builder
	sb.Grow(len(payload))

	str := string(payload)
	for _, r := range str {
		switch r {
		case '\r':
			sb.WriteString("\\r")
		case '\n':
			sb.WriteString("\\n")
		case '\t':
			sb.WriteString("\\t")
		default:
			if r < 32 || r == 127 {
				continue
			}
			sb.WriteRune(r)
		}
	}
	clean := sb.String()

	if availableWidth <= 0 {
		return clean
	}

	cleanWidth := lipgloss.Width(clean)
	if cleanWidth <= availableWidth {
		return clean
	}

	if availableWidth <= 3 {
		dots := "..."
		return dots[:max(0, availableWidth)]
	}

	targetWidth := availableWidth - 3
	var truncated strings.Builder
	currentWidth := 0

	for _, r := range clean {
		rw := lipgloss.Width(string(r))
		if currentWidth+rw > targetWidth {
			break
		}
		truncated.WriteRune(r)
		currentWidth += rw
	}
	truncated.WriteString("...")
	return truncated.String()
}

// ResolvePayload converts string input, @file paths, or stdin (-) into payload bytes.
func ResolvePayload(input string) ([]byte, error) {
	if input == "-" {
		return io.ReadAll(os.Stdin)
	}
	if after, ok := strings.CutPrefix(input, "@"); ok {
		path := after
		return os.ReadFile(path)
	}
	return []byte(input), nil
}

// List returns the names of all active queues.
func List(ctx context.Context, client mikyu.Client) ([]string, error) {
	return client.ListQueues(ctx)
}

// Create initializes a new queue from a sparse set of options.
func Create(ctx context.Context, client mikyu.Client, name string, opts ...mikyu.QueueOption) error {
	return client.CreateQueue(ctx, name, opts...)
}

// Update applies a sparse patch of mutable fields to a queue.
func Update(ctx context.Context, client mikyu.Client, name string, opts ...mikyu.QueueOption) error {
	return client.UpdateQueue(ctx, name, opts...)
}

// Describe returns a queue's stored spec, effective spec, defaults, and stats.
func Describe(ctx context.Context, client mikyu.Client, name string) (mikyu.QueueDescription, error) {
	return client.DescribeQueue(ctx, name)
}

// TightensRetention reports whether patch narrows a retention bound below what current (an effective spec) already allows.
func TightensRetention(current, patch mikyu.Spec) bool {
	if next := patch.Retention.Age; next != nil {
		if cur := current.Retention.Age; cur == nil || next.Duration() < cur.Duration() {
			return true
		}
	}
	if next := patch.Retention.Bytes; next != nil {
		if cur := current.Retention.Bytes; cur == nil || *next < *cur {
			return true
		}
	}
	return false
}

// Delete removes a queue and its stored data. If confirmFn is provided, deletion occurs only if confirmFn returns true.
func Delete(ctx context.Context, client mikyu.Client, name string, confirmFn func() (bool, error)) (executed bool, err error) {
	if confirmFn != nil {
		ok, err := confirmFn()
		if err != nil || !ok {
			return false, err
		}
	}
	if err := client.DeleteQueue(ctx, name); err != nil {
		return true, err
	}
	return true, nil
}

// Append sends payload input to the queue and returns the appended message offset and payload size in bytes.
func Append(ctx context.Context, client mikyu.Client, name string, input string) (offset uint64, payloadLen int, err error) {
	payload, err := ResolvePayload(input)
	if err != nil {
		return 0, 0, err
	}
	q, err := client.LogQueue(name)
	if err != nil {
		return 0, 0, err
	}
	off, err := q.Append(ctx, payload)
	if err != nil {
		return 0, 0, err
	}
	return off, len(payload), nil
}

// Read retrieves the message at a specific logical offset.
func Read(ctx context.Context, client mikyu.Client, name string, offset uint64) (mikyu.Message, error) {
	q, err := client.LogQueue(name)
	if err != nil {
		return mikyu.Message{}, err
	}
	return q.Read(ctx, offset)
}

// Tail streams records continuously starting from startOffset and calls onMessage for each message.
func Tail(ctx context.Context, client mikyu.Client, name string, startOffset uint64, onMessage func(msg mikyu.Message) error) error {
	q, err := client.LogQueue(name)
	if err != nil {
		return err
	}
	for msg, err := range q.Tail(ctx, startOffset) {
		if err != nil {
			return err
		}
		if onMessage != nil {
			if err := onMessage(msg); err != nil {
				return err
			}
		}
	}
	return nil
}

// Info retrieves the server's identity and aggregate storage state.
func Info(ctx context.Context, client mikyu.Client) (mikyu.Info, error) {
	return client.Info(ctx)
}

// Truncate truncates physical log segments for a queue before minOffset. If confirmFn is provided, truncation occurs only if confirmFn returns true.
func Truncate(ctx context.Context, client mikyu.Client, name string, minOffset uint64, confirmFn func() (bool, error)) (reclaimed int64, executed bool, err error) {
	if confirmFn != nil {
		ok, err := confirmFn()
		if err != nil || !ok {
			return 0, false, err
		}
	}
	reclaimed, err = client.Truncate(ctx, name, minOffset)
	if err != nil {
		return 0, true, err
	}
	return reclaimed, true, nil
}

// Shutdown sends an OpShutdown frame to the daemon process. If confirmFn is provided, shutdown occurs only if confirmFn returns true.
func Shutdown(target string, confirmFn func() (bool, error)) (executed bool, err error) {
	if confirmFn != nil {
		ok, err := confirmFn()
		if err != nil || !ok {
			return false, err
		}
	}
	if err := daemon.SendShutdownSignal(target); err != nil {
		return true, err
	}
	return true, nil
}
