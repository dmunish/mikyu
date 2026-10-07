//go:build !linux

package engine

import "os"

func newDurableSink(log, idx *os.File, _ *uringRing) sink {
	return &syncSink{log: log, idx: idx}
}
