package client

import (
	"github.com/dmunish/mikyu/internal/node"
	"github.com/dmunish/mikyu/internal/spec"
)

// QueueStats is a per-queue storage snapshot as seen through the client.
type QueueStats struct {
	MessageCount uint64 `json:"message_count"`
	SegmentCount int    `json:"segment_count"`
	SizeBytes    int64  `json:"size_bytes"`
	LastOffset   uint64 `json:"last_offset"`
	MinOffset    uint64 `json:"min_offset"`
}

// QueueDescription is the full truth about one queue: its stored spec, its effective spec (stored over defaults over built-in), the session defaults it inherits from, and its current storage stats.
type QueueDescription struct {
	Spec       spec.Spec  `json:"spec"`
	Effective  spec.Spec  `json:"effective"`
	Defaults   spec.Spec  `json:"defaults"`
	QueueStats QueueStats `json:"stats"`
}

// Info is a snapshot of the server's identity and aggregate storage state.
type Info struct {
	Version      string                `json:"version"`
	UptimeNs     int64                 `json:"uptime_ns"`
	DataDir      string                `json:"data_dir"`
	ActiveQueues int                   `json:"active_queues"`
	TotalBytes   int64                 `json:"total_bytes"`
	Queues       map[string]QueueStats `json:"queues"`
}

func toClientQueueStats(s node.QueueStats) QueueStats {
	return QueueStats{
		MessageCount: s.MessageCount,
		SegmentCount: s.SegmentCount,
		SizeBytes:    s.SizeBytes,
		LastOffset:   s.LastOffset,
		MinOffset:    s.MinOffset,
	}
}

func toClientDescription(d node.QueueDescription) QueueDescription {
	return QueueDescription{
		Spec:       d.Spec,
		Effective:  d.Effective,
		Defaults:   d.Defaults,
		QueueStats: toClientQueueStats(d.QueueStats),
	}
}

func toClientInfo(i node.Info) Info {
	queues := make(map[string]QueueStats, len(i.Queues))
	for name, qs := range i.Queues {
		queues[name] = toClientQueueStats(qs)
	}
	return Info{
		Version:      i.Version,
		UptimeNs:     i.UptimeNs,
		DataDir:      i.DataDir,
		ActiveQueues: i.ActiveQueues,
		TotalBytes:   i.TotalBytes,
		Queues:       queues,
	}
}
