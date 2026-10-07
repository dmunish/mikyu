package engine

import "os"

// syncSink is the portable sync-durability sink: pwrite then fdatasync. It is
// the fallback on every platform, including Linux hosts where io_uring is
// unavailable.
type syncSink struct {
	log *os.File
	idx *os.File
}

func (s *syncSink) Write(batch sinkBatch, done sinkDone) error {
	err := s.write(batch)
	if done != nil {
		done(err)
	}
	return err
}

func (s *syncSink) write(batch sinkBatch) error {
	if err := writeRecords(s.log, batch.Records); err != nil {
		return err
	}
	if !batch.Index.empty() {
		if err := writeIndexBatch(s.idx, batch.Index); err != nil {
			return err
		}
	}
	return syncData(s.log)
}

func (s *syncSink) Sync() error  { return syncData(s.log) }
func (s *syncSink) Close() error { return nil }
