package bench

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRecoveryCompletes(t *testing.T) {
	opts := baseOptions(t, ModeRecovery)
	opts.Spec = asyncSpec()
	opts.Workload.CrashAfterBytes = 256 << 10

	res, err := Run(t.Context(), opts)
	require.NoError(t, err)
	require.True(t, res.Complete)
	require.NotNil(t, res.Metrics)
	require.NotNil(t, res.Metrics.Recovery)
	assert.Equal(t, "crash", res.Metrics.BoundFired)

	recovery := res.Metrics.Recovery
	assert.Positive(t, recovery.TimeToOpenNS, "reopening must be timed")
	assert.Positive(t, recovery.TimeToFirstReadNS, "the first read must be timed")
	assert.Positive(t, recovery.TimeToFullRecoveryNS, "full recovery must be timed")

	assert.NotEmpty(t, res.Phases)
	assert.Equal(t, "populate", res.Phases[0].Name)
	assert.Equal(t, "abort", res.Phases[len(res.Phases)-1].Name)
}

func TestRecoveryRefusesMultipleQueues(t *testing.T) {
	opts := baseOptions(t, ModeRecovery)
	opts.Workload.Queues = 2
	opts.Workload.CrashAfterBytes = 256 << 10

	_, err := Run(t.Context(), opts)
	require.ErrorIs(t, err, errRecoveryQueues)
}

func TestRecoveryRefusesNonEmbeddedAccess(t *testing.T) {
	opts := baseOptions(t, ModeRecovery)
	opts.Workload.CrashAfterBytes = 256 << 10
	opts.Access = AccessRemote
	opts.Target = "tcp://127.0.0.1:1"

	_, err := Run(t.Context(), opts)
	require.ErrorIs(t, err, ErrUsage)
}

func TestRunChildRequiresQueue(t *testing.T) {
	require.ErrorIs(t, RunChild(nil), ErrUsage)
}
