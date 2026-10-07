package engine

import (
	"context"
	"os"
	"sync"
	"time"

	"github.com/dmunish/mikyu/internal/iow"
	"github.com/dmunish/mikyu/internal/metrics"
)

type pendingEntry struct {
	pos int64
	len int64
}

type asyncEntry struct {
	seg    *segment
	rec    []byte
	pos    int64
	recLen int64
	offset uint64
	buf    *pooledBuf
}

// wal writes segment bytes through an iow.Writer. Two paths share the struct:
// the sync path stages byte-budgeted groups behind one flusher goroutine with
// micro-linger group commits, and the async path submits every producer batch
// straight to the writer's bounded worker pool; on hosts with io_uring the ring
// path submits to the shared ring instead, with one reaper per data directory
// completing every queue's operations.
//
// Single writer per batch: a batch is written by exactly one writer goroutine —
// the pool's worker, or the ring reaper — and completions arrive in submission
// order, so the byte watermark below only ever advances over a contiguous
// durable prefix. Producers only stage records via push/pushBatch (sync path)
// or pushAsyncBatch (async path). Sequencing is byte-based: pushedBytes and
// publishedBytes count staged and durable bytes, and wait(seq) blocks until
// publishedBytes reaches the byte offset the push returned. Async
// backpressure is bounded by the WAL's slot channel.
type wal struct {
	cfg    Config
	files  *sink      // log+idx adapter for the pool path
	writer iow.Writer // nil on the ring path
	sink   ringSink   // nil on the pool path
	ring   *uringRing // nil on the syscall path
	m      *walMetrics

	mu             sync.Mutex
	cond           *sync.Cond
	err            error
	asyncNext      uint64
	pushedBytes    int64
	publishedBytes int64
	pendingBytes   int64

	// pool-path state: the sync path's staged group, buffers reclaimed from
	// completed batches, and the producer-to-writer submission context.
	stage         staged
	free          []staged
	flushInFlight bool
	flushCtx      context.Context
	flushCancel   context.CancelFunc
	wake          chan struct{}
	// configChanged carries UpdateConfig to the flusher's ticker.
	configChanged chan struct{}
	// slots bounds async batches submitted to the writer but not yet complete.
	slots chan struct{}

	// ring-path state: a submission-sequence watermark turns out-of-order ring
	// completions back into the byte-based durable guarantee.
	inflightWG sync.WaitGroup
	submitSeq  uint64
	watermark  uint64
	completed  map[uint64]int64

	shutdown chan struct{}
	done     chan struct{}
	stopOnce sync.Once
}

// newWAL builds the WAL for one segment; ring may be nil on hosts without io_uring.
func newWAL(seg *segment, cfg Config, startOffset uint64, ring *uringRing, scopes ...*metrics.Scope) *wal {
	if ring != nil && os.Getenv("MIKYU_FORCE_SYSCALL_WAL") == "" {
		return newRingWAL(seg, cfg, startOffset, ring, scopes...)
	}
	return newSyscallWAL(seg, cfg, startOffset, scopes...)
}

func (w *wal) start() {
	if w.ring != nil {
		w.startRing()
		return
	}
	w.startSyscall()
}

func (w *wal) push(rec []byte, pos int64) (seq int64, wake bool, err error) {
	if w.ring != nil {
		return w.pushRing(rec, pos)
	}
	return w.pushSyscall(rec, pos)
}

func (w *wal) pushBatch(recs [][]byte, positions []int64, idxStart int64) (seq int64, wake bool, err error) {
	if w.ring != nil {
		return w.pushBatchRing(recs, positions, idxStart)
	}
	return w.pushBatchSyscall(recs, positions)
}

func (w *wal) pushAsync(ctx context.Context, e asyncEntry) error {
	return w.pushAsyncBatch(ctx, []asyncEntry{e})
}

func (w *wal) pushAsyncBatch(ctx context.Context, entries []asyncEntry) error {
	if len(entries) == 0 {
		return nil
	}
	if w.ring != nil {
		return w.pushAsyncBatchRing(ctx, entries, int64(entries[0].offset-entries[0].seg.baseOff)*rawIndexEntrySize)
	}
	return w.pushAsyncBatchSyscall(ctx, entries)
}

// UpdateConfig swaps the WAL's byte budget and timing parameters.
func (w *wal) UpdateConfig(maxFlushBytes int64, maxFlushWait, flushLinger time.Duration) {
	if w.ring != nil {
		w.updateConfigRing(maxFlushBytes, maxFlushWait, flushLinger)
		return
	}
	w.updateConfigSyscall(maxFlushBytes, maxFlushWait, flushLinger)
}

func (w *wal) stop() error {
	if w.ring != nil {
		return w.stopRing()
	}
	return w.stopSyscall()
}

// ownsIndex reports whether the WAL's sink writes the segment's index entries alongside the log bytes.
func (w *wal) ownsIndex() bool {
	return w.ring != nil
}

// bytes returns the WAL's committed, flushed, and staged byte counts.
func (w *wal) bytes() (pushed, published, pending int64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.pushedBytes, w.publishedBytes, w.pendingBytes
}

// channelFill reports the async backlog occupancy as a fraction in [0,1].
func (w *wal) channelFill() float64 {
	if capacity := cap(w.slots); capacity > 0 {
		return min(float64(len(w.slots))/float64(capacity), 1)
	}
	return 0
}

// byteFill reports staged WAL bytes as a fraction of the flush budget.
func (w *wal) byteFill() float64 {
	w.mu.Lock()
	pending, maxBytes := w.pendingBytes, w.cfg.MaxFlushBytes
	w.mu.Unlock()
	if maxBytes <= 0 {
		return 0
	}
	return min(float64(pending)/float64(maxBytes), 1)
}

// wait blocks until every byte staged before seq is durable.
func (w *wal) wait(seq int64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	for w.publishedBytes < seq && w.err == nil {
		w.cond.Wait()
	}
	return w.err
}

func (w *wal) currentError() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.err
}

func (w *wal) setError(err error) {
	w.mu.Lock()
	w.setErrorLocked(err)
	w.mu.Unlock()
}

// setErrorLocked records the first fatal error and wakes every waiter.
func (w *wal) setErrorLocked(err error) {
	if err == nil || w.err != nil {
		return
	}
	w.err = err
	w.cond.Broadcast()
}
