package daemon

import (
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsDaemonAlive(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			conn.Close()
		}
	}()

	target := "tcp://" + ln.Addr().String()
	assert.True(t, IsDaemonAlive(target))
	assert.False(t, IsDaemonAlive("tcp://127.0.0.1:1"))

	ln.Close()
	<-done
}

func TestEnsureDaemonRunningRejectsEmptyTarget(t *testing.T) {
	err := EnsureDaemonRunning(SpawnOptions{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "target is required")
}

func TestEnsureDaemonRunningLeavesLiveDaemonAlone(t *testing.T) {
	target := "tcp://" + freeTCPAddr(t)
	stop := startDaemon(t, target, "--target", target, "--data-dir", filepath.Join(t.TempDir(), "data"))
	defer stop()

	require.NoError(t, EnsureDaemonRunning(SpawnOptions{Target: target}))
	assert.True(t, IsDaemonAlive(target))
}

// TestEnsureDaemonRunningPassesTarget spawns a real detached daemon through
// the test binary acting as a helper process.
func TestEnsureDaemonRunningPassesTarget(t *testing.T) {
	t.Setenv(daemonHelperEnv, "1")

	// An ephemeral port can be reused between the probe and the spawn, so retry
	// on a fresh port until the daemon we spawned creates its data directory.
	var target, dataDir string
	require.Eventually(t, func() bool {
		target = "tcp://" + freeTCPAddr(t)
		dataDir = filepath.Join(t.TempDir(), "data")
		if err := EnsureDaemonRunning(SpawnOptions{Target: target, DataDir: dataDir}); err != nil {
			return false
		}
		_, err := os.Stat(dataDir)
		return err == nil
	}, 10*time.Second, 100*time.Millisecond, "spawned daemon never honored the session's data directory")

	t.Cleanup(func() {
		_ = SendShutdownSignal(target)
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) && IsDaemonAlive(target) {
			time.Sleep(25 * time.Millisecond)
		}
	})
	require.True(t, IsDaemonAlive(target), "spawned daemon never came online")
}
