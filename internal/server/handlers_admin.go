package server

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/dmunish/mikyu/internal/engine"
	"github.com/dmunish/mikyu/internal/node"
	"github.com/dmunish/mikyu/internal/spec"
	"github.com/dmunish/mikyu/internal/transport/protocol"
)

// decodeSpecFrame parses a spec-bearing admin request after any leading
// discriminator: bytes(name) u16 specVersion bytes(json). The version is
// checked before the payload is touched, so a frame claiming a schema this
// build cannot decode is rejected without interpretation.
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

// handleCreate serves OpCreate: u8 kind, bytes(name), u16 specVersion, bytes(json).
func (s *Server) handleCreate(ctx context.Context, body []byte) (byte, []byte) {
	r := protocol.NewReader(body)
	kindByte, err := r.ReadU8()
	if err != nil {
		return opErr(protocol.ErrCodeInvalidConfig, err.Error())
	}
	name, data, op, errBody := decodeSpecFrame(r)
	if errBody != nil {
		return op, errBody
	}
	sp, err := spec.DecodeSpec(data)
	if err != nil {
		return taskErr(err)
	}

	switch node.Kind(kindByte) {
	case node.KindLog:
		_, err = s.node.CreateLogQueue(ctx, name, sp)
	case node.KindTask:
		_, err = s.node.CreateTaskQueue(ctx, name, sp)
	default:
		return opErr(protocol.ErrCodeInvalidConfig, "unknown queue kind")
	}
	if err != nil {
		if errors.Is(err, engine.ErrQueueExists) {
			return opErr(protocol.ErrCodeQueueExists, err.Error())
		}
		return taskErr(err)
	}
	return protocol.OpCreate, nil
}

func (s *Server) handleUpdate(ctx context.Context, body []byte) (byte, []byte) {
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
	return protocol.OpUpdate, nil
}

func (s *Server) handleDescribe(ctx context.Context, body []byte) (byte, []byte) {
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
	return protocol.OpDescribe, buf.Bytes()
}

// handleList serves OpList: u32 count, then per entry bytes(name) u8 kind.
func (s *Server) handleList(ctx context.Context) (byte, []byte) {
	infos, err := s.node.ListQueues(ctx)
	if err != nil {
		return taskErr(err)
	}
	buf := protocol.Buffer{}
	buf.WriteU32(uint32(len(infos)))
	for _, info := range infos {
		buf.WriteBytes([]byte(info.Name))
		buf.WriteU8(byte(info.Kind))
	}
	return protocol.OpList, buf.Bytes()
}

func (s *Server) handleDelete(body []byte) (byte, []byte) {
	r := protocol.NewReader(body)
	name, _ := r.ReadBytes()
	if err := s.node.DeleteQueue(context.Background(), string(name)); err != nil {
		return taskErr(err)
	}
	return protocol.OpDelete, nil
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
	reclaimed, err := s.node.Truncate(ctx, string(name), minOffset)
	if err != nil {
		return taskErr(err)
	}

	buf := protocol.Buffer{}
	buf.WriteU64(uint64(reclaimed))
	return protocol.OpTruncate, buf.Bytes()
}

// handleMetrics serves OpMetrics: a u16 specVersion followed by the
// JSON-encoded live snapshot, the same envelope as OpDescribe.
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
// parameter table, the same envelope as OpDescribe and OpMetrics.
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