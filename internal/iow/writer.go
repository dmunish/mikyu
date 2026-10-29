// Package iow provides bounded, ordered, concurrent writes to a set of files.
package iow

import (
	"context"
	"errors"
	"os"
	"runtime"
)

// Write is one buffer at one offset of one file.
type Write struct {
	F   *os.File
	Buf []byte
	Off int64
}

// Durability is the boundary at which a batch's done callback fires.
type Durability int

const (
	// Buffered fires done once the bytes are in the OS page cache.
	Buffered Durability = iota
	// DataSync fires done once the bytes are on stable storage.
	DataSync
)

// Options configures a Writer.
type Options struct {
	// Workers is the number of batches that may be writing concurrently; zero selects GOMAXPROCS.
	Workers int
	// Outstanding bounds submitted-but-not-completed batches; zero selects four per worker.
	Outstanding int
	// Durability selects the completion boundary.
	Durability Durability
	// SyncFiles limits the files a DataSync batch fsyncs; empty means every file the batch wrote. It never narrows Sync, which flushes every file the writer owns.
	SyncFiles []*os.File
}

// outstandingDepth resolves Options.Outstanding: an explicit bound, else four
// batches per worker, with Workers itself defaulting to GOMAXPROCS.
func outstandingDepth(opts Options) int {
	if opts.Outstanding > 0 {
		return opts.Outstanding
	}
	workers := opts.Workers
	if workers <= 0 {
		workers = runtime.GOMAXPROCS(0)
	}
	return workers * 4
}

// Writer is the durable-write contract, safe for concurrent use. Every submission is given a sequence number and its done fires exactly once, in submission order, so callers can advance a monotonic durable watermark. A Write that returns an error (closed, failed, or cancelled) still has its done called with that error.
type Writer interface {
	// Write submits one batch, blocking while Outstanding batches are in flight or until ctx ends; done may be nil.
	Write(ctx context.Context, batch []Write, done func(error)) error
	// Sync waits for outstanding writes and fsyncs every file the writer owns.
	Sync() error
	// Close waits for outstanding writes, fsyncs, and closes the files; it is idempotent.
	Close() error
}

// ErrClosed reports a Write after Close or after a fatal batch error.
var ErrClosed = errors.New("iow: writer is closed")

// ErrRingBroken reports a retired io_uring ring: a submission failed and no batch may ever be published again.
var ErrRingBroken = errors.New("iow: io_uring ring is broken")

// New returns the best Writer the platform offers: the ring, else the port under Buffered, else the pool.
func New(files []*os.File, opts Options) (Writer, error) {
	if os.Getenv("MIKYU_FORCE_SYSCALL_WAL") == "" {
		if w, ok := newRingWriter(files, opts); ok {
			return w, nil
		}
		if opts.Durability == Buffered {
			if w, ok := newIOCPWriter(files, opts); ok {
				return w, nil
			}
		}
	}
	return newPool(files, opts), nil
}
