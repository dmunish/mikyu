package client

import (
	"context"
	"errors"
	"iter"
	"time"

	"github.com/dmunish/mikyu/internal/engine"
	"github.com/dmunish/mikyu/internal/queue/logqueue"
	"github.com/dmunish/mikyu/internal/transport/protocol"
)

const (
	streamBatch = 1024
	longPoll    = 30 * time.Second
	pollMargin  = 5 * time.Second
)

func (c *remoteClient) LogQueue(name string) (logqueue.ClientLogQueue, error) {
	return &remoteLogQueue{client: c, name: name}, nil
}

type remoteLogQueue struct {
	client *remoteClient
	name   string
}

func (q *remoteLogQueue) Name() string { return q.name }

func (q *remoteLogQueue) Append(ctx context.Context, payload []byte) (uint64, error) {
	body := protocol.Buffer{}
	body.WriteBytes([]byte(q.name))
	body.WriteBytes(payload)
	resp, err := q.client.exchange(ctx, protocol.OpAppend, body.Bytes())
	if err != nil {
		return 0, err
	}
	r := protocol.NewReader(resp)
	return r.ReadU64()
}

func (q *remoteLogQueue) AppendBatch(ctx context.Context, payloads [][]byte) ([]uint64, error) {
	body := protocol.Buffer{}
	body.WriteBytes([]byte(q.name))
	body.WriteU32(uint32(len(payloads)))
	for _, p := range payloads {
		body.WriteBytes(p)
	}
	resp, err := q.client.exchange(ctx, protocol.OpAppendBatch, body.Bytes())
	if err != nil {
		return nil, err
	}
	r := protocol.NewReader(resp)
	count, err := r.ReadU32()
	if err != nil {
		return nil, err
	}
	offsets := make([]uint64, count)
	for i := range count {
		off, err := r.ReadU64()
		if err != nil {
			return nil, err
		}
		offsets[i] = off
	}
	return offsets, nil
}

func (q *remoteLogQueue) Read(ctx context.Context, offset uint64) (engine.Message, error) {
	body := protocol.Buffer{}
	body.WriteBytes([]byte(q.name))
	body.WriteU64(offset)
	resp, err := q.client.exchange(ctx, protocol.OpRead, body.Bytes())
	if err != nil {
		return engine.Message{}, err
	}
	r := protocol.NewReader(resp)
	off, err := r.ReadU64()
	if err != nil {
		return engine.Message{}, err
	}
	idBytes, err := r.ReadBytes()
	if err != nil {
		return engine.Message{}, err
	}
	tsNano, err := r.ReadU64()
	if err != nil {
		return engine.Message{}, err
	}
	payload, err := r.ReadBytes()
	if err != nil {
		return engine.Message{}, err
	}
	return engine.Message{
		Offset:    off,
		ID:        string(idBytes),
		Timestamp: time.Unix(0, int64(tsNano)),
		Payload:   payload,
	}, nil
}

func (q *remoteLogQueue) ReadBatch(ctx context.Context, startOffset uint64, count int) ([]engine.Message, error) {
	body := protocol.Buffer{}
	body.WriteBytes([]byte(q.name))
	body.WriteU64(startOffset)
	body.WriteU32(uint32(count))
	resp, err := q.client.exchange(ctx, protocol.OpReadBatch, body.Bytes())
	if err != nil {
		return nil, err
	}
	r := protocol.NewReader(resp)
	n, err := r.ReadU32()
	if err != nil {
		return nil, err
	}
	msgs := make([]engine.Message, 0, n)
	for range n {
		off, _ := r.ReadU64()
		idBytes, _ := r.ReadBytes()
		tsNano, _ := r.ReadU64()
		payload, _ := r.ReadBytes()
		msgs = append(msgs, engine.Message{
			Offset:    off,
			ID:        string(idBytes),
			Timestamp: time.Unix(0, int64(tsNano)),
			Payload:   payload,
		})
	}
	return msgs, nil
}

func (q *remoteLogQueue) LastOffset(ctx context.Context) (uint64, error) {
	body := protocol.Buffer{}
	body.WriteBytes([]byte(q.name))
	resp, err := q.client.exchange(ctx, protocol.OpLastOffset, body.Bytes())
	if err != nil {
		return 0, err
	}
	r := protocol.NewReader(resp)
	return r.ReadU64()
}

func (q *remoteLogQueue) MinOffset(ctx context.Context) (uint64, error) {
	snap, err := q.client.Metrics(ctx)
	if err != nil {
		return 0, err
	}
	qv, ok := snap.Queue(q.name)
	if !ok {
		return 0, engine.ErrQueueNotFound
	}
	return uint64(qv.Gauges[engine.GaugeMinOffset]), nil
}

func (q *remoteLogQueue) Truncate(ctx context.Context, minOffset uint64) (int64, error) {
	return q.client.Truncate(ctx, q.name, minOffset)
}

func (q *remoteLogQueue) fetch(ctx context.Context, start uint64, maxCount int, maxWait time.Duration) ([]engine.Message, error) {
	wait := maxWait
	if deadline, ok := ctx.Deadline(); ok {
		if remaining := time.Until(deadline); remaining < wait {
			wait = remaining
		}
	}
	var b protocol.Buffer
	protocol.EncodeFetch(&b, q.name, start, maxCount, wait)
	callCtx, cancel := context.WithTimeout(ctx, wait+pollMargin)
	defer cancel()
	resp, err := q.client.exchange(callCtx, protocol.OpFetch, b.Bytes())
	if err != nil {
		if parentErr := ctx.Err(); parentErr != nil {
			return nil, parentErr
		}
		return nil, err
	}
	recs, err := protocol.ReadLogRecordList(protocol.NewReader(resp))
	if err != nil {
		return nil, err
	}
	msgs := make([]engine.Message, len(recs))
	for i, rec := range recs {
		msgs[i] = engine.Message{
			Offset:    rec.Offset,
			ID:        rec.ID,
			Timestamp: rec.Timestamp,
			Payload:   rec.Payload,
		}
	}
	return msgs, nil
}

func (q *remoteLogQueue) Stream(ctx context.Context, startOffset uint64) iter.Seq2[engine.Message, error] {
	return func(yield func(engine.Message, error) bool) {
		curr := startOffset
		for {
			msgs, err := q.fetch(ctx, curr, streamBatch, 0)
			if err != nil {
				if errors.Is(err, engine.ErrNoData) {
					return
				}
				yield(engine.Message{}, err)
				return
			}
			for _, msg := range msgs {
				if !yield(msg, nil) {
					return
				}
				curr = msg.Offset + 1
			}
		}
	}
}

func (q *remoteLogQueue) Tail(ctx context.Context, startOffset uint64) iter.Seq2[engine.Message, error] {
	return func(yield func(engine.Message, error) bool) {
		curr := startOffset
		for {
			if err := ctx.Err(); err != nil {
				yield(engine.Message{}, err)
				return
			}
			msgs, err := q.fetch(ctx, curr, streamBatch, longPoll)
			if err != nil {
				if errors.Is(err, engine.ErrNoData) {
					continue
				}
				yield(engine.Message{}, err)
				return
			}
			for _, msg := range msgs {
				if !yield(msg, nil) {
					return
				}
				curr = msg.Offset + 1
			}
		}
	}
}

func (q *remoteLogQueue) Group(name string) logqueue.Group {
	g := &remoteGroup{queue: q}
	g.Base.Name = name
	g.Base.CommitFn = func(ctx context.Context, offset uint64) error {
		body := protocol.Buffer{}
		body.WriteBytes([]byte(q.name))
		body.WriteBytes([]byte(name))
		body.WriteU64(offset)
		_, err := q.client.exchange(ctx, protocol.OpCommit, body.Bytes())
		return err
	}
	return g
}
