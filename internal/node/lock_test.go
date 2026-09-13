package node

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestLockedDirRejectsSecondProcess spawns a child that holds the lock and
// asserts the parent's acquireDirLock returns ErrLocked.
func TestLockedDirRejectsSecondProcess(t *testing.T) {
	if os.Getenv("MIKYU_LOCK_HELPER") == "1" {
		lock, err := acquireDirLock(os.Getenv("MIKYU_LOCK_DIR"))
		if err != nil {
			t.Fatal(err)
		}
		defer lock.release()
		_ = os.WriteFile(os.Getenv("MIKYU_LOCK_MARKER"), []byte("ready"), 0o600)
		time.Sleep(10 * time.Second)
		return
	}

	dir := t.TempDir()
	marker := filepath.Join(dir, "ready")
	cmd := exec.Command(os.Args[0], "-test.run", "^TestLockedDirRejectsSecondProcess$")
	cmd.Env = append(os.Environ(),
		"MIKYU_LOCK_HELPER=1",
		"MIKYU_LOCK_DIR="+dir,
		"MIKYU_LOCK_MARKER="+marker,
	)
	require.NoError(t, cmd.Start())
	defer func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(marker); err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	_, err := acquireDirLock(dir)
	require.ErrorIs(t, err, ErrLocked)
}
