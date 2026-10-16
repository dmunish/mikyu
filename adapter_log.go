package mikyu

import (
	"context"
	"iter"
	"time"

	"github.com/dmunish/mikyu/internal/engine"
	"github.com/dmunish/mikyu/internal/queue/logqueue"
)

func toRecord(r engine.Record) Record {
	return Record{
		Offset:    r.Offset,
		ID:        r.ID,
		Timestamp: r.Timestamp,
		Group:     r.Group,
		Payload:   r.Payload,
	}
}

func toRecords(rs []engine.Record) []Record {
	out := make([]Record, len(rs))
	for i, r := range rs {
		out[i] = toRecord(r)
	}
	return out
}

type logQueue struct {
	inner logqueue.Log
}

func (q *logQueue) Name() string { return q.inner.Name() }

func (q *logQueue) Append(ctx context.Context, payload []byte, opts ...AppendOption) (uint64, error) {
	return q.inner.Append(ctx, payload, opts...)
}

func (q *logQueue) AppendBatch(ctx context.Context, payloads [][]byte, opts ...AppendOption) ([]uint64, error) {
	return q.inner.AppendBatch(ctx, payloads, opts...)
}

func (q *logQueue) Read(ctx context.Context, offset uint64) (Record, error) {
	r, err := q.inner.Read(ctx, offset)
	if err != nil {
		return Record{}, err
	}
	return toRecord(r), nil
}

func (q *logQueue) ReadBatch(ctx context.Context, start uint64, count int) ([]Record, error) {
	rs, err := q.inner.ReadBatch(ctx, start, count)
	if err != nil {
		return nil, err
	}
	return toRecords(rs), nil
}

func (q *logQueue) Fetch(ctx context.Context, offset uint64, maxCount int, maxWait time.Duration) ([]Record, error) {
	rs, err := q.inner.Fetch(ctx, offset, maxCount, maxWait)
	if err != nil {
		return nil, err
	}
	return toRecords(rs), nil
}

func (q *logQueue) Stream(ctx context.Context, start uint64) iter.Seq2[Record, error] {
	return func(yield func(Record, error) bool) {
		for r, err := range q.inner.Stream(ctx, start) {
			if err != nil {
				yield(Record{}, err)
				return
			}
			if !yield(toRecord(r), nil) {
				return
			}
		}
	}
}

func (q *logQueue) Tail(ctx context.Context, start uint64) iter.Seq2[Record, error] {
	return func(yield func(Record, error) bool) {
		for r, err := range q.inner.Tail(ctx, start) {
			if err != nil {
				yield(Record{}, err)
				return
			}
			if !yield(toRecord(r), nil) {
				return
			}
		}
	}
}

func (q *logQueue) LastOffset(ctx context.Context) (uint64, error) {
	return q.inner.LastOffset(ctx)
}

func (q *logQueue) MinOffset(ctx context.Context) (uint64, error) {
	return q.inner.MinOffset(ctx)
}

func (q *logQueue) Group(name string) Group {
	return q.inner.Group(name)
}

var _ LogQueue = (*logQueue)(nil)
