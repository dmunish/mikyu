package client

import (
	"context"

	"github.com/dmunish/mikyu/internal/queue/group"
	"github.com/dmunish/mikyu/internal/transport/protocol"
)

type remoteGroup struct {
	group.Base
	queue *remoteLogQueue
}

func (g *remoteGroup) Name() string {
	return g.Base.Name
}

func (g *remoteGroup) Committed(ctx context.Context) (uint64, error) {
	body := protocol.Buffer{}
	body.WriteBytes([]byte(g.queue.name))
	body.WriteBytes([]byte(g.Base.Name))
	resp, err := g.queue.client.exchange(ctx, protocol.OpCommitted, body.Bytes())
	if err != nil {
		return 0, err
	}
	r := protocol.NewReader(resp)
	return r.ReadU64()
}
