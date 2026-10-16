package command

import (
	"context"
	"io"
	"os"
	"strings"

	"charm.land/lipgloss/v2"
	"github.com/dmunish/mikyu"
	"golang.org/x/term"
)

// GetTerminalWidth returns the width of the active terminal window, checking stdout, stderr, and stdin.
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

// FormatSingleLinePayload sanitizes payload bytes and truncates to availableWidth using visual cell count.
func FormatSingleLinePayload(payload []byte, availableWidth int) string {
	var sb strings.Builder
	sb.Grow(len(payload))

	for _, r := range string(payload) {
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

	if availableWidth <= 0 || lipgloss.Width(clean) <= availableWidth {
		return clean
	}
	if availableWidth <= 3 {
		return "..."
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
		return os.ReadFile(after)
	}
	return []byte(input), nil
}

// Append sends payload input to a log queue and returns the appended offset and payload size in bytes.
func Append(ctx context.Context, client mikyu.Client, name, input string, key []byte) (uint64, int, error) {
	payload, err := ResolvePayload(input)
	if err != nil {
		return 0, 0, err
	}
	q, err := client.Log(ctx, name)
	if err != nil {
		return 0, 0, err
	}
	var opts []mikyu.AppendOption
	if len(key) > 0 {
		opts = append(opts, mikyu.WithRecordKey(key))
	}
	off, err := q.Append(ctx, payload, opts...)
	if err != nil {
		return 0, 0, err
	}
	return off, len(payload), nil
}

// Read retrieves the record at one logical offset.
func Read(ctx context.Context, client mikyu.Client, name string, offset uint64) (mikyu.Record, error) {
	q, err := client.Log(ctx, name)
	if err != nil {
		return mikyu.Record{}, err
	}
	return q.Read(ctx, offset)
}

// Tail streams records continuously starting from startOffset, calling onMessage for each record.
func Tail(ctx context.Context, client mikyu.Client, name string, startOffset uint64, onMessage func(msg mikyu.Record) error) error {
	q, err := client.Log(ctx, name)
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

// Truncate truncates physical log segments below minOffset, gated by confirmFn when provided.
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
