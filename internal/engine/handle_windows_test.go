//go:build windows

package engine

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestOverlappedHandleRoundTrip writes through an overlapped handle, fsyncs,
// closes, and requires a plain reopen to read the bytes back.
func TestOverlappedHandleRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "overlapped.bin")
	data := bytes.Repeat([]byte("mikyu"), 200)

	f, err := openOverlapped(path, os.O_CREATE|os.O_RDWR, 0o644)
	require.NoError(t, err)
	n, err := f.WriteAt(data, 0)
	require.NoError(t, err)
	require.Equal(t, len(data), n)
	require.NoError(t, f.Sync())
	require.NoError(t, f.Close())

	f2, err := os.OpenFile(path, os.O_RDONLY, 0)
	require.NoError(t, err)
	defer f2.Close()
	got := make([]byte, len(data))
	_, err = f2.ReadAt(got, 0)
	require.NoError(t, err)
	require.Equal(t, data, got)
}
