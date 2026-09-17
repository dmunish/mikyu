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

func (c *remoteClient) LogQueue(name string) (logqueue.Log, error) {
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

func (q *remoteLogQueue) Read(ctx context.Context, offset uint64) (engine.Record, error) {
	body := protocol.Buffer{}
	body.WriteBytes([]byte(q.name))
	body.WriteU64(offset)
	resp, err := q.client.exchange(ctx, protocol.OpRead, body.Bytes())
	if err != nil {
		return engine.Record{}, err
	}
	r := protocol.NewReader(resp)
	off, err := r.ReadU64()
	if err != nil {
		return engine.Record{}, err
	}
	idBytes, err := r.ReadBytes()
	if err != nil {
		return engine.Record{}, err
	}
	tsNano, err := r.ReadU64()
	if err != nil {
		return engine.Record{}, err
	}
	payload, err := r.ReadBytes()
	if err != nil {
		return engine.Record{}, err
	}
	return engine.Record{
		Offset:    off,
		ID:        string(idBytes),
		Timestamp: time.Unix(0, int64(tsNano)),
		Payload:   payload,
	}, nil
}

func (q *remoteLogQueue) ReadBatch(ctx context.Context, startOffset uint64, count int) ([]engine.Record, error) {
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
	msgs := make([]engine.Record, 0, n)
	for range n {
		off, _ := r.ReadU64()
		idBytes, _ := r.ReadBytes()
		tsNano, _ := r.ReadU64()
		payload, _ := r.ReadBytes()
		msgs = append(msgs, engine.Record{
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
	var body protocol.Buffer
	body.WriteBytes([]byte(q.name))
	resp, err := q.client.exchange(ctx, protocol.OpMinOffset, body.Bytes())
	if err != nil {
		return 0, err
	}
	return protocol.NewReader(resp).ReadU64()
}

func (q *remoteLogQueue) Truncate(ctx context.Context, minOffset uint64) (int64, error) {
	return q.client.Truncate(ctx, q.name, minOffset)
}

func (q *remoteLogQueue) fetch(ctx context.Context, start uint64, maxCount int, maxWait time.Duration) ([]engine.Record, error) {
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
	recs, err := protocol.ReadRecordList(protocol.NewReader(resp))
	if err != nil {
		return nil, err
	}
	msgs := make([]engine.Record, len(recs))
	for i, rec := range recs {
		msgs[i] = engine.Record{
			Offset:    rec.Offset,
			ID:        rec.ID,
			Timestamp: rec.Timestamp,
			Payload:   rec.Payload,
		}
	}
	return msgs, nil
}

func (q *remoteLogQueue) Stream(ctx context.Context, startOffset uint64) iter.Seq2[engine.Record, error] {
	return func(yield func(engine.Record, error) bool) {
		curr := startOffset
		for {
			msgs, err := q.fetch(ctx, curr, streamBatch, 0)
			if err != nil {
				if errors.Is(err, engine.ErrNoData) {
					return
				}
				yield(engine.Record{}, err)
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

func (q *remoteLogQueue) Tail(ctx context.Context, startOffset uint64) iter.Seq2[engine.Record, error] {
	return func(yield func(engine.Record, error) bool) {
		curr := startOffset
		for {
			if err := ctx.Err(); err != nil {
				yield(engine.Record{}, err)
				return
			}
			msgs, err := q.fetch(ctx, curr, streamBatch, longPoll)
			if err != nil {
				if errors.Is(err, engine.ErrNoData) {
					continue
				}
				yield(engine.Record{}, err)
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

func (q *remoteLogQueue) Fetch(ctx context.Context, offset uint64, maxCount int, maxWait time.Duration) ([]engine.Record, error) {
	return q.fetch(ctx, offset, maxCount, maxWait)
}

func (q *remoteLogQueue) CommitOffset(ctx context.Context, group string, offset uint64) error {
	body := protocol.Buffer{}
	body.WriteBytes([]byte(q.name))
	body.WriteBytes([]byte(group))
	body.WriteU64(offset)
	_, err := q.client.exchange(ctx, protocol.OpCommit, body.Bytes())
	return err
}

func (q *remoteLogQueue) Committed(ctx context.Context, group string) (uint64, error) {
	body := protocol.Buffer{}
	body.WriteBytes([]byte(q.name))
	body.WriteBytes([]byte(group))
	resp, err := q.client.exchange(ctx, protocol.OpCommitted, body.Bytes())
	if err != nil {
		return 0, err
	}
	r := protocol.NewReader(resp)
	return r.ReadU64()
}

func (q *remoteLogQueue) TruncateFloor(ctx context.Context) (uint64, bool, error) {
	return 0, false, nil // remote clients observe through the node's metrics; retention floor lives server-side
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
