package server

import (
	"context"

	"github.com/dmunish/mikyu/internal/transport/protocol"
)

// handleMinOffset serves OpMinOffset: bytes(name) -> u64 minOffset.
func (s *Server) handleMinOffset(ctx context.Context, body []byte) (byte, []byte) {
	r := protocol.NewReader(body)
	name, err := r.ReadBytes()
	if err != nil {
		return opErr(protocol.ErrCodeInvalidConfig, err.Error())
	}
	q, err := s.node.LogQueue(string(name))
	if err != nil {
		return taskErr(err)
	}
	minOffset, err := q.MinOffset(ctx)
	if err != nil {
		return taskErr(err)
	}
	var buf protocol.Buffer
	buf.WriteU64(minOffset)
	return protocol.OpMinOffset, buf.Bytes()
}