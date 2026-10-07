package engine

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/dmunish/mikyu/internal/iow"
)

// heldBatch is one submitted batch a blockingWriter has not completed yet.
type heldBatch struct {
	writes []iow.Write
	done   func(error)
}

// blockingWriter completes submitted batches only when the test releases them.
type blockingWriter struct {
	mu      sync.Mutex
	held    []heldBatch
	entered chan struct{}
	err     error
}

// useBlockingWriter installs a blocking writer for the next WAL built and returns it.
func useBlockingWriter(t *testing.T) *blockingWriter {
	t.Helper()
	w := &blockingWriter{entered: make(chan struct{}, 1024)}
	prev := newWriter
	newWriter = func([]*os.File, iow.Options) (iow.Writer, error) { return w, nil }
	t.Cleanup(func() { newWriter = prev })
	return w
}

func (b *blockingWriter) Write(_ context.Context, batch []iow.Write, done func(error)) error {
	b.mu.Lock()
	if b.err != nil {
		err := b.err
		b.mu.Unlock()
		if done != nil {
			done(err)
		}
		return err
	}
	b.held = append(b.held, heldBatch{writes: batch, done: done})
	b.mu.Unlock()
	select {
	case b.entered <- struct{}{}:
	default:
	}
	return nil
}

func (b *blockingWriter) Sync() error {
	b.release()
	return nil
}

func (b *blockingWriter) Close() error {
	b.release()
	return nil
}

// release completes every held batch with the writer's error, if any.
func (b *blockingWriter) release() {
	b.mu.Lock()
	held, err := b.held, b.err
	b.held = nil
	b.mu.Unlock()
	for _, h := range held {
		if h.done != nil {
			h.done(err)
		}
	}
}

// awaitEntered blocks until the writer has accepted one batch.
func (b *blockingWriter) awaitEntered(t *testing.T) {
	t.Helper()
	select {
	case <-b.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("writer never received a batch")
	}
}

// syncWriter completes every batch synchronously inside Write, keeping the
// whole WAL lifecycle inside a synctest bubble.
type syncWriter struct{}

func (syncWriter) Write(_ context.Context, _ []iow.Write, done func(error)) error {
	if done != nil {
		done(nil)
	}
	return nil
}

func (syncWriter) Sync() error  { return nil }
func (syncWriter) Close() error { return nil }

// useSyncWriter installs a synchronous writer for the next WAL built.
func useSyncWriter(t *testing.T) {
	t.Helper()
	prev := newWriter
	newWriter = func([]*os.File, iow.Options) (iow.Writer, error) { return syncWriter{}, nil }
	t.Cleanup(func() { newWriter = prev })
}

// fillSlots submits batches until every async slot is taken.
func fillSlots(t *testing.T, w *wal, seg *segment) {
	t.Helper()
	entry := asyncEntry{seg: seg, rec: []byte("x"), pos: 0, recLen: 1, offset: 0}
	for range cap(w.slots) {
		require.NoError(t, w.pushAsync(t.Context(), entry))
	}
}
