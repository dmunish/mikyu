//go:build linux

package engine

import "os"

// newDurableSink returns an io_uring-backed sink when a ring is available and
// the portable sync sink otherwise; a nil ring falls back silently.
func newDurableSink(log, idx *os.File, ring *uringRing) sink {
	if ring == nil {
		return &syncSink{log: log, idx: idx}
	}
	return &uringSink{log: log, idx: idx, handle: ring.Register()}
}

// uringSink submits one tag per Write: the batch's record writes, its index
// write, then a draining fsync over both files. Write returns once every
// completion for the tag has arrived.
type uringSink struct {
	log    *os.File
	idx    *os.File
	handle *uringSinkHandle
}

func (s *uringSink) Write(batch sinkBatch, done sinkDone) error {
	if len(batch.Records) == 0 && batch.Index.empty() {
		return nil
	}
	tag := s.handle.NextTag()
	ops := make([]uringOp, 0, len(batch.Records)+2)
	for _, w := range batch.Records {
		ops = append(ops, uringOp{Kind: uringOpWrite, File: s.log, Buf: w.P, Off: w.Off, Tag: tag})
	}
	var indexBuf *pooledBuf
	if !batch.Index.empty() {
		indexBuf = getBuf(len(batch.Index.Positions) * rawIndexEntrySize)
		encodeIndexInto(indexBuf.b, batch.Index)
		ops = append(ops, uringOp{Kind: uringOpWrite, File: s.idx, Buf: indexBuf.b, Off: batch.Index.Start, Tag: tag})
	}
	ops = append(ops, uringOp{Kind: uringOpFsync, File: s.log, DataSync: true, Drain: true, Tag: tag})
	err := s.handle.Submit(ops)
	if err == nil {
		err = s.handle.Wait(tag)
	}
	if indexBuf != nil {
		indexBuf.release(maxIndexKeepBytes)
	}
	if done != nil {
		done(err)
	}
	return err
}

func (s *uringSink) Sync() error  { return s.handle.fsync(s.log) }
func (s *uringSink) Close() error { return s.handle.Close() }
