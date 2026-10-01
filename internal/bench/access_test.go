package bench

import (
	"testing"
	"time"

	"github.com/dmunish/mikyu/internal/daemon"
	"github.com/dmunish/mikyu/internal/spec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRemoteAccessRequiresTarget(t *testing.T) {
	opts := baseOptions(t, ModeProduce)
	opts.Spec = asyncSpec()
	opts.Workload.Duration = time.Second
	opts.Access = AccessRemote

	_, err := Run(t.Context(), opts)
	require.ErrorIs(t, err, ErrUsage)
}

func TestDaemonAccess(t *testing.T) {
	opts := baseOptions(t, ModeProduce)
	opts.Access = AccessDaemon
	opts.Workload.Duration = 300 * time.Millisecond

	res, err := Run(t.Context(), opts)
	require.NoError(t, err)
	require.True(t, res.Complete)
	assert.Equal(t, "daemon", res.Access)
	assert.NotEmpty(t, res.AccessTarget)
	assert.Greater(t, res.Metrics.Messages, int64(0))
	assert.False(t, daemon.IsDaemonAlive(res.AccessTarget), "a non-persisted daemon must be stopped")
}

func TestDaemonPersistKeepsDaemon(t *testing.T) {
	opts := baseOptions(t, ModeProduce)
	opts.Access = AccessDaemon
	opts.Persist = true
	opts.Workload.Duration = 200 * time.Millisecond

	res, err := Run(t.Context(), opts)
	require.NoError(t, err)
	require.True(t, res.Complete)

	assert.True(t, daemon.IsDaemonAlive(res.AccessTarget), "a persisted daemon must stay up")
	t.Cleanup(func() { _ = daemon.SendShutdownSignal(res.AccessTarget, spec.BuiltinSession()) })

	require.NoError(t, daemon.SendShutdownSignal(res.AccessTarget, spec.BuiltinSession()))
	assert.Eventually(t, func() bool { return !daemon.IsDaemonAlive(res.AccessTarget) }, 10*time.Second, 50*time.Millisecond,
		"the daemon must stop after the shutdown signal")
}
