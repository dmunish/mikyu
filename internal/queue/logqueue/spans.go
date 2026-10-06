package logqueue

import "github.com/dmunish/mikyu/internal/engine"

// SpanSource resolves record payload locations for zero-copy wire transport.
type SpanSource interface {
	PayloadSpans(start uint64, count int) ([]engine.PayloadSpan, error)
}

var _ SpanSource = (*queue)(nil)

// PayloadSpans resolves the longest file-backed prefix of the requested range.
func (q *queue) PayloadSpans(start uint64, count int) ([]engine.PayloadSpan, error) {
	ss, ok := q.storage.(engine.SpanSource)
	if !ok {
		return nil, nil
	}
	return ss.PayloadSpans(start, count)
}
