package server

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/dmunish/mikyu/internal/engine"
	"github.com/dmunish/mikyu/internal/spec"
	"github.com/dmunish/mikyu/internal/transport/protocol"
)

// decodeSpecFrame parses a spec-bearing admin request:
// bytes(name) u16 specVersion bytes(json). The version is checked before the
// payload is touched, so a frame claiming a schema this build cannot decode
// is rejected without interpretation.
func decodeSpecFrame(r *protocol.Reader) (name string, data []byte, respOp byte, respErr []byte) {
	nameBytes, err := r.ReadBytes()
	if err != nil {
		return "", nil, protocol.OpError, protocol.EncodeError(protocol.ErrCodeInvalidConfig, err.Error())
	}
	version, err := r.ReadU16()
	if err != nil {
		return "", nil, protocol.OpError, protocol.EncodeError(protocol.ErrCodeInvalidConfig, err.Error())
	}
	if err := spec.CheckSpecVersion(version); err != nil {
		return "", nil, protocol.OpError, protocol.EncodeError(protocol.ErrCodeSpecTooNew, err.Error())
	}
	data, err = r.ReadBytes()
	if err != nil {
		return "", nil, protocol.OpError, protocol.EncodeError(protocol.ErrCodeInvalidConfig, err.Error())
	}
	return string(nameBytes), data, 0, nil
}

func (s *Server) handleCreateQueue(ctx context.Context, body []byte) (byte, []byte) {
	r := protocol.NewReader(body)
	name, data, op, errBody := decodeSpecFrame(r)
	if errBody != nil {
		return op, errBody
	}
	sp, err := spec.DecodeSpec(data)
	if err != nil {
		return taskErr(err)
	}
	if err := s.node.CreateQueue(ctx, name, sp); err != nil {
		if errors.Is(err, engine.ErrQueueExists) {
			return opErr(protocol.ErrCodeQueueExists, err.Error())
		}
		return taskErr(err)
	}
	return protocol.OpCreateQueue, nil
}

func (s *Server) handleUpdateQueue(ctx context.Context, body []byte) (byte, []byte) {
	r := protocol.NewReader(body)
	name, data, op, errBody := decodeSpecFrame(r)
	if errBody != nil {
		return op, errBody
	}
	patch, err := spec.DecodeSpec(data)
	if err != nil {
		return taskErr(err)
	}
	if err := s.node.UpdateQueue(ctx, name, patch); err != nil {
		return taskErr(err)
	}
	return protocol.OpUpdateQueue, nil
}

func (s *Server) handleDescribeQueue(ctx context.Context, body []byte) (byte, []byte) {
	r := protocol.NewReader(body)
	name, err := r.ReadBytes()
	if err != nil {
		return opErr(protocol.ErrCodeInvalidConfig, err.Error())
	}
	d, err := s.node.DescribeQueue(ctx, string(name))
	if err != nil {
		return taskErr(err)
	}
	data, err := json.Marshal(d)
	if err != nil {
		return opErr(protocol.ErrCodeUnknown, err.Error())
	}
	var buf protocol.Buffer
	buf.WriteU16(spec.CurrentSpecVersion)
	buf.WriteBytes(data)
	return protocol.OpDescribeQueue, buf.Bytes()
}

func (s *Server) handleListQueues() (byte, []byte) {
	names := s.node.Manager().ListQueues()
	buf := protocol.Buffer{}
	buf.WriteU32(uint32(len(names)))
	for _, n := range names {
		buf.WriteBytes([]byte(n))
	}
	return protocol.OpListQueues, buf.Bytes()
}

func (s *Server) handleDeleteQueue(body []byte) (byte, []byte) {
	r := protocol.NewReader(body)
	name, _ := r.ReadBytes()
	if err := s.node.DeleteQueue(context.Background(), string(name)); err != nil {
		return opErr(protocol.ErrCodeQueueNotFound, err.Error())
	}
	return protocol.OpDeleteQueue, nil
}

func (s *Server) handleTruncate(ctx context.Context, body []byte) (byte, []byte) {
	r := protocol.NewReader(body)
	name, err := r.ReadBytes()
	if err != nil {
		return opErr(protocol.ErrCodeInvalidConfig, err.Error())
	}
	minOffset, err := r.ReadU64()
	if err != nil {
		return opErr(protocol.ErrCodeInvalidConfig, err.Error())
	}

	// Truncate materializes both drivers so durable task state from a
	// previous process lifetime is included in the retention floor.
	reclaimed, err := s.node.Truncate(ctx, string(name), minOffset)
	if err != nil {
		if errors.Is(err, engine.ErrQueueNotFound) {
			return opErr(protocol.ErrCodeQueueNotFound, err.Error())
		}
		return taskErr(err)
	}

	buf := protocol.Buffer{}
	buf.WriteU64(uint64(reclaimed))
	return protocol.OpTruncate, buf.Bytes()
}

// handleMetrics serves OpMetrics: a u16 specVersion followed by the
// JSON-encoded live snapshot, the same envelope as OpDescribeQueue.
func (s *Server) handleMetrics(ctx context.Context) (byte, []byte) {
	snap, err := s.node.Metrics(ctx)
	if err != nil {
		return taskErr(err)
	}
	data, err := json.Marshal(snap)
	if err != nil {
		return opErr(protocol.ErrCodeUnknown, err.Error())
	}
	var buf protocol.Buffer
	buf.WriteU16(spec.CurrentSpecVersion)
	buf.WriteBytes(data)
	return protocol.OpMetrics, buf.Bytes()
}

// handleSchema serves OpSchema: a u16 specVersion followed by the JSON-encoded
// parameter table, the same envelope as OpDescribeQueue and OpMetrics.
func (s *Server) handleSchema() (byte, []byte) {
	data, err := json.Marshal(spec.Table)
	if err != nil {
		return opErr(protocol.ErrCodeUnknown, err.Error())
	}
	var buf protocol.Buffer
	buf.WriteU16(spec.CurrentSpecVersion)
	buf.WriteBytes(data)
	return protocol.OpSchema, buf.Bytes()
}
