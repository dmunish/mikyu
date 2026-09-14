package server

import (
	"fmt"
	"time"

	"github.com/dmunish/mikyu/internal/metrics"
	"github.com/dmunish/mikyu/internal/transport/protocol"
)

// serverScopeName is the registry scope that holds server-wide instruments.
const serverScopeName = "server"

// opDurationBounds are latency buckets, in seconds, for wire operations.
var opDurationBounds = []float64{
	1e-6, 5e-6, 1e-5, 5e-5, 1e-4, 5e-4, 1e-3, 5e-3, 1e-2, 5e-2, 1e-1, 5e-1, 1,
}

// opLabels maps opcodes to the series suffix used in Prometheus output. The
// metrics model has no labels, so each op gets its own counter and histogram.
var opLabels = map[byte]string{
	protocol.OpCreateQueue:   "create_queue",
	protocol.OpDeleteQueue:   "delete_queue",
	protocol.OpListQueues:    "list_queues",
	protocol.OpUpdateQueue:   "update_queue",
	protocol.OpDescribeQueue: "describe_queue",
	protocol.OpTruncate:      "truncate",
	protocol.OpInfo:          "info",
	protocol.OpAppend:        "append",
	protocol.OpAppendBatch:   "append_batch",
	protocol.OpRead:          "read",
	protocol.OpReadBatch:     "read_batch",
	protocol.OpLastOffset:    "last_offset",
	protocol.OpCommit:        "commit",
	protocol.OpCommitted:     "committed",
	protocol.OpFetch:         "fetch",
	protocol.OpEnqueue:       "enqueue",
	protocol.OpDequeue:       "dequeue",
	protocol.OpAck:           "ack",
	protocol.OpNack:          "nack",
	protocol.OpExtend:        "extend",
	protocol.OpDeadLetters:   "dead_letters",
	protocol.OpDeadLetterAck: "dead_letter_ack",
}

func opLabel(op byte) string {
	if name, ok := opLabels[op]; ok {
		return name
	}
	return fmt.Sprintf("0x%02x", op)
}

// serverMetrics caches the server's per-op instruments.
type serverMetrics struct {
	connections *metrics.Gauge
	ops         map[byte]*metrics.Counter
	durations   map[byte]*metrics.Histogram
}

func newServerMetrics(scope *metrics.Scope) *serverMetrics {
	sm := &serverMetrics{
		connections: scope.Gauge("connections"),
		ops:         make(map[byte]*metrics.Counter, len(opLabels)),
		durations:   make(map[byte]*metrics.Histogram, len(opLabels)),
	}
	for op := range opLabels {
		label := opLabel(op)
		sm.ops[op] = scope.Counter("op_" + label)
		sm.durations[op] = scope.Histogram("op_"+label+"_duration", opDurationBounds)
	}
	return sm
}

// recordOp counts one dispatched operation and its latency.
func (s *Server) recordOp(op byte, start time.Time) {
	if c, ok := s.metrics.ops[op]; ok {
		c.Inc()
	}
	if h, ok := s.metrics.durations[op]; ok {
		h.Observe(time.Since(start).Seconds())
	}
}
