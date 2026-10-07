package engine

import (
	"os"
	"sync"
)

// sink is the WAL's durable byte target for one segment: it owns the log and
// index file handles the segment writes through. Write returns once the batch
// is durable (sync sinks) or queued (async sinks), and invokes done exactly
// once per batch — sync sinks inline, async sinks when the batch's bytes are
// visible. Byte slices in a batch must remain valid until done runs (async)
// or until Write returns (sync).
type sink interface {
	Write(batch sinkBatch, done sinkDone) error
	Sync() error
	Close() error
}

// sinkBatch is one producer batch: the log records to append, plus any index entries that must become durable with them.
type sinkBatch struct {
	Records []sinkWrite
	Index   sinkIndex
}

// sinkWrite is one record's bytes and their offset in the log file.
type sinkWrite struct {
	P   []byte
	Off int64
}

// sinkIndex describes the index slots to write for a batch; empty Positions means the caller keeps index writes to itself.
type sinkIndex struct {
	Start     int64
	Positions []int64
	Lengths   []int64
}

func (s sinkIndex) empty() bool { return len(s.Positions) == 0 }

// sinkDone runs once per async batch, on any goroutine; a non-nil error means the batch failed.
type sinkDone func(err error)

// newSink returns the sink a WAL with the given durability needs writes through; a nil ring falls back silently to the portable sinks.
func newSink(log, idx *os.File, cfg Config, ring *uringRing) sink {
	if cfg.Async() {
		return newAsyncSink(log, idx, max(1, int(cfg.MaxFlushBytes)/4096))
	}
	return newDurableSink(log, idx, ring)
}

// writerHook, when non-nil, runs before every sink file write so tests can assert the single-writer invariant.
var writerHook func()

// writeAtHook, when non-nil, receives every sink log write's offset and length so tests can assert batching.
var writeAtHook func(off int64, n int)

// writeRecords writes each record at its own offset, firing the test hooks.
func writeRecords(f *os.File, records []sinkWrite) error {
	for _, w := range records {
		if writerHook != nil {
			writerHook()
		}
		n, err := f.WriteAt(w.P, w.Off)
		if writeAtHook != nil {
			writeAtHook(w.Off, n)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// --- async sink ---------------------------------------------------------------

// asyncSink batches writes onto a channel and lets a single goroutine drain
// them; this is the shape the WAL has always used for async durability, and
// ring hosts replace it with direct submission.
type asyncSink struct {
	log *os.File
	idx *os.File
	ch  chan asyncBatch

	stop      chan struct{}
	done      chan struct{}
	startOnce sync.Once
	stopOnce  sync.Once
}

type asyncBatch struct {
	batch sinkBatch
	done  sinkDone
}

func newAsyncSink(log, idx *os.File, depth int) *asyncSink {
	return &asyncSink{
		log:  log,
		idx:  idx,
		ch:   make(chan asyncBatch, max(1, depth)),
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}
}

func (s *asyncSink) Write(batch sinkBatch, done sinkDone) error {
	s.start()
	select {
	case <-s.stop:
		return ErrQueueClosed
	default:
	}
	select {
	case s.ch <- asyncBatch{batch: batch, done: done}:
		return nil
	case <-s.stop:
		return ErrQueueClosed
	}
}

func (s *asyncSink) Sync() error { return syncData(s.log) }

func (s *asyncSink) Close() error {
	s.stopOnce.Do(func() {
		s.start()
		close(s.stop)
		<-s.done
	})
	return nil
}

func (s *asyncSink) start() {
	s.startOnce.Do(func() { go s.loop() })
}

func (s *asyncSink) loop() {
	defer close(s.done)
	for {
		select {
		case b := <-s.ch:
			s.writeBatch(b)
		case <-s.stop:
			for {
				select {
				case b := <-s.ch:
					s.writeBatch(b)
				default:
					return
				}
			}
		}
	}
}

func (s *asyncSink) writeBatch(b asyncBatch) {
	err := s.write(b.batch)
	if b.done != nil {
		b.done(err)
	}
}

func (s *asyncSink) write(batch sinkBatch) error {
	if err := writeRecords(s.log, batch.Records); err != nil {
		return err
	}
	if !batch.Index.empty() {
		return writeIndexBatch(s.idx, batch.Index)
	}
	return nil
}
