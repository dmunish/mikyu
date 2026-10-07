//go:build linux

package engine

import "os"

// newDurableSink returns an io_uring-backed sink when a ring is available and
// the portable sync sink otherwise; a nil ring falls back silently.
func newDurableSink(log, idx *os.File, ring *uringRing) sink {
	if ring == nil {
		return &syncSink{log: log, idx: idx}
	}
	return newRingSink(log, idx, true, ring)
}

// newAsyncSinkOf returns the async sink for the WAL: direct ring submission
// when a ring is available, the channel-drained async sink otherwise.
func newAsyncSinkOf(log, idx *os.File, depth int, ring *uringRing) sink {
	if ring == nil {
		return newAsyncSink(log, idx, depth)
	}
	return newRingSink(log, idx, false, ring)
}

// newRingSink returns a sink that submits the WAL's writes to the shared ring.
func newRingSink(log, idx *os.File, durable bool, ring *uringRing) sink {
	return &ringSink{log: log, idx: idx, handle: ring.Register(), durable: durable}
}

// ringSink submits one tag per Write: the batch's record write, its index
// write, and — for durable sinks — a draining fsync over both files. A nil
// done makes Write synchronous (it returns once the batch is durable); a
// non-nil done makes it asynchronous (Write returns after submission and done
// runs once on completion, on the reaper goroutine).
type ringSink struct {
	log     *os.File
	idx     *os.File
	handle  *uringSinkHandle
	durable bool
}

func (s *ringSink) Write(batch sinkBatch, done sinkDone) error {
	if len(batch.Records) == 0 && batch.Index.empty() {
		if done != nil {
			done(nil)
		}
		return nil
	}
	tag := s.handle.NextTag()
	if writerHook != nil {
		writerHook()
	}
	ops := make([]uringOp, 0, len(batch.Records)+2)
	link := !s.durable // an async batch chains its index write after its record write
	for _, w := range batch.Records {
		ops = append(ops, uringOp{Kind: uringOpWrite, File: s.log, Buf: w.P, Off: w.Off, Link: link, Tag: tag})
	}
	var indexBuf *pooledBuf
	if !batch.Index.empty() {
		indexBuf = getBuf(len(batch.Index.Positions) * rawIndexEntrySize)
		encodeIndexInto(indexBuf.b, batch.Index)
		ops = append(ops, uringOp{Kind: uringOpWrite, File: s.idx, Buf: indexBuf.b, Off: batch.Index.Start, Tag: tag})
	}
	if s.durable {
		ops = append(ops, uringOp{Kind: uringOpFsync, File: s.log, DataSync: true, Drain: true, Tag: tag})
	}
	releaseIndex := func() {
		if indexBuf != nil {
			indexBuf.release(maxIndexKeepBytes)
		}
	}
	if done != nil {
		err := s.handle.Submit(ops, func(err error) {
			releaseIndex()
			done(err)
		})
		if err != nil {
			releaseIndex()
		}
		return err
	}
	err := s.handle.Submit(ops, nil)
	if err == nil {
		err = s.handle.Wait(tag)
	}
	releaseIndex()
	return err
}

func (s *ringSink) Sync() error  { return s.handle.fsync(s.log) }
func (s *ringSink) Close() error { return s.handle.Close() }
