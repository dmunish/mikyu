package daemon

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dmunish/mikyu/internal/spec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// spawnDaemon spawns a real detached daemon through the test binary acting as a helper process.
func spawnDaemon(t *testing.T, opts SpawnOptions) *Process {
	t.Helper()
	t.Setenv(daemonHelperEnv, "1")

	proc, err := Spawn(opts)
	require.NoError(t, err)
	t.Cleanup(func() { _ = proc.Kill() })
	return proc
}

func TestSpawnPicksAnEphemeralPort(t *testing.T) {
	proc := spawnDaemon(t, SpawnOptions{DataDir: filepath.Join(t.TempDir(), "data")})

	assert.NotZero(t, proc.PID)
	assert.Contains(t, proc.Target, "tcp://127.0.0.1:")
	assert.True(t, WaitForAlive(proc.Target, 10*time.Second), "spawned daemon never came online")
	assert.True(t, proc.Alive())
}

func TestSpawnHonorsSessionFlags(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	target := "tcp://" + freeTCPAddr(t)
	proc := spawnDaemon(t, SpawnOptions{Target: target, DataDir: dataDir})

	assert.Equal(t, target, proc.Target, "an explicit target is used as given")
	require.Eventually(t, func() bool {
		_, err := os.Stat(filepath.Join(dataDir, "mikyu.lock"))
		return err == nil
	}, 10*time.Second, 50*time.Millisecond, "the spawned daemon never took the data directory")
}

func TestSpawnThenKill(t *testing.T) {
	proc := spawnDaemon(t, SpawnOptions{DataDir: filepath.Join(t.TempDir(), "data")})
	require.True(t, WaitForAlive(proc.Target, 10*time.Second))

	require.NoError(t, proc.Kill())
	assert.False(t, WaitForAlive(proc.Target, 2*time.Second), "a killed daemon must stop serving")
	assert.False(t, proc.Alive())
	assert.NoError(t, proc.Kill(), "Kill is idempotent")
}

func TestWaitReportsExit(t *testing.T) {
	proc := spawnDaemon(t, SpawnOptions{DataDir: filepath.Join(t.TempDir(), "data")})
	require.True(t, WaitForAlive(proc.Target, 10*time.Second))

	require.NoError(t, SendShutdownSignal(proc.Target, spec.BuiltinSession()))
	assert.NoError(t, proc.Wait())
	assert.False(t, proc.Alive())
}

func TestWaitForAliveTimesOut(t *testing.T) {
	start := time.Now()
	assert.False(t, WaitForAlive("tcp://127.0.0.1:1", 200*time.Millisecond))
	assert.Less(t, time.Since(start), 5*time.Second)
}

func TestDaemonArgs(t *testing.T) {
	args := daemonArgs(SpawnOptions{
		DataDir:       "/tmp/data",
		TLSEnabled:    true,
		TLSCert:       "cert.pem",
		TLSKey:        "key.pem",
		AuthMethod:    "token",
		AuthTokenFile: "token.txt",
		TLSClientCA:   "ca.pem",
		ExtraArgs:     []string{"--metrics-listen", "127.0.0.1:9090"},
	})
	assert.Equal(t, []string{
		"--data-dir", "/tmp/data",
		"--tls-cert", "cert.pem",
		"--tls-key", "key.pem",
		"--auth-method", "token",
		"--auth-token-file", "token.txt",
		"--tls-client-ca", "ca.pem",
		"--tls-enabled",
		"--metrics-listen", "127.0.0.1:9090",
	}, args)

	assert.Empty(t, daemonArgs(SpawnOptions{}), "a zero SpawnOptions contributes no flags")
}
