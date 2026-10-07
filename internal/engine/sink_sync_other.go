//go:build !linux

package engine

import "os"

func newDurableSink(log, idx *os.File, _ *uringRing) sink {
	return &syncSink{log: log, idx: idx}
}

func newAsyncSinkOf(log, idx *os.File, depth int, _ *uringRing) sink {
	return newAsyncSink(log, idx, depth)
}
