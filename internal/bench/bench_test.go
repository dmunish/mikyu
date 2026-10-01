package bench

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/dmunish/mikyu/internal/bench/result"
	"github.com/dmunish/mikyu/internal/spec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func baseOptions(t *testing.T, mode Mode) Options {
	t.Helper()
	return Options{
		Mode:   mode,
		Access: AccessEmbedded,
		Dir:    t.TempDir(),
		Out:    t.TempDir(),
		Workload: Workload{
			Kind: KindLog, Queues: 1, MsgSize: 256, Workers: 4, Batch: 1, Seed: 1,
		},
	}
}

func asyncSpec() spec.Spec {
	return spec.Spec{Storage: spec.StorageSpec{Durability: new(spec.Durability(spec.DurabilityAsync))}}
}

func TestProduceClosedLoop(t *testing.T) {
	opts := baseOptions(t, ModeProduce)
	opts.Spec = asyncSpec()
	opts.Workload.Bytes = 1 << 20

	res, err := Run(t.Context(), opts)
	require.NoError(t, err)
	require.True(t, res.Complete)
	require.NotNil(t, res.Metrics)
	assert.Equal(t, "bytes", res.Metrics.BoundFired)
	assert.Greater(t, res.Metrics.Messages, int64(0))
	assert.Equal(t, "produce", res.Mode)
	assert.Equal(t, "embedded", res.Access)
	assert.NotEmpty(t, res.Phases)
}

func TestProduceBatch(t *testing.T) {
	opts := baseOptions(t, ModeProduce)
	opts.Spec = asyncSpec()
	opts.Workload.Batch = 4
	opts.Workload.Bytes = 256 << 10

	res, err := Run(t.Context(), opts)
	require.NoError(t, err)
	require.True(t, res.Complete)
	moved := res.Metrics.Messages * int64(opts.Workload.MsgSize)
	assert.GreaterOrEqual(t, moved, opts.Workload.Bytes, "counted bytes must cover the budget")
}

func TestProduceOpenLoop(t *testing.T) {
	opts := baseOptions(t, ModeProduce)
	opts.Spec = asyncSpec()
	opts.Workload.Duration = time.Second
	opts.Workload.Rate = 10_000

	res, err := Run(t.Context(), opts)
	require.NoError(t, err)
	require.True(t, res.Complete)
	assert.Equal(t, "duration", res.Metrics.BoundFired)
	assert.InDelta(t, 10_000.0, res.Metrics.ThroughputMsgs, 1_000.0, "offered rate must be tracked")
}

func TestConsumeWithPreload(t *testing.T) {
	opts := baseOptions(t, ModeConsume)
	opts.Spec = asyncSpec()
	opts.Workload.Preload = 1 << 20
	opts.Workload.Bytes = 1 << 20

	res, err := Run(t.Context(), opts)
	require.NoError(t, err)
	require.True(t, res.Complete)
	assert.Equal(t, int64((1<<20)/256), res.Metrics.Messages)
}

func TestProduceTaskQueue(t *testing.T) {
	opts := baseOptions(t, ModeProduce)
	opts.Spec = asyncSpec()
	opts.Workload.Kind = KindTask
	opts.Workload.Bytes = 256 << 10

	res, err := Run(t.Context(), opts)
	require.NoError(t, err)
	require.True(t, res.Complete)
	assert.Equal(t, "task", res.Workload.Kind)
	assert.Greater(t, res.Metrics.Messages, int64(0))
}

func TestMixSymmetric(t *testing.T) {
	opts := baseOptions(t, ModeMix)
	opts.Spec = asyncSpec()
	opts.Workload.Duration = 500 * time.Millisecond
	opts.Workload.Workers = 2

	res, err := Run(t.Context(), opts)
	require.NoError(t, err)
	require.True(t, res.Complete)
	assert.Greater(t, res.Metrics.Messages, int64(0))
}

func TestAbortOnCancel(t *testing.T) {
	opts := baseOptions(t, ModeProduce)
	opts.Spec = asyncSpec()
	opts.Workload.Duration = 30 * time.Second

	ctx, cancel := context.WithCancel(t.Context())
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	res, err := Run(ctx, opts)
	require.ErrorIs(t, err, context.Canceled)
	assert.False(t, res.Complete)
	assert.Equal(t, "context canceled", res.Aborted)
}

func TestEnvelopeShape(t *testing.T) {
	opts := baseOptions(t, ModeProduce)
	opts.Spec = asyncSpec()
	opts.Workload.Bytes = 256 << 10

	res, err := Run(t.Context(), opts)
	require.NoError(t, err)
	require.True(t, res.Complete)

	loaded, err := result.ReadResult(opts.Out)
	require.NoError(t, err)
	assert.Equal(t, result.CurrentVersion, loaded.Version)
	assert.Equal(t, "produce", loaded.Mode)
	assert.True(t, loaded.Complete)
	assert.Equal(t, opts.Workload.Seed, loaded.Workload.Seed)
	assert.NotEmpty(t, loaded.Environment.GoVersion)
	assert.NotEmpty(t, loaded.Phases)
	assert.True(t, result.IsRun(opts.Out))
}

func TestEnvelopeStreaming(t *testing.T) {
	dir := t.TempDir()
	env, err := newEnvelope(dir, result.Result{Version: result.CurrentVersion, RunID: "bench-partial", Mode: "produce"})
	require.NoError(t, err)
	require.NoError(t, env.AppendRow(result.SeriesRow{T: 1, Phase: "measure", Client: result.ClientCols{Throughput: 1000}}))

	loaded, err := result.ReadResult(dir)
	require.NoError(t, err, "a partially written envelope must stay readable")
	assert.False(t, loaded.Complete)

	rows, err := result.ReadSeries(dir)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, 1000.0, rows[0].Client.Throughput)

	require.NoError(t, env.series.Close())
}

func TestAccessValidation(t *testing.T) {
	opts := baseOptions(t, ModeProduce)
	opts.Spec = asyncSpec()
	opts.Workload.Duration = time.Second
	opts.Dir, opts.Target = "", "tcp://127.0.0.1:1"
	_, err := Run(t.Context(), opts)
	require.ErrorIs(t, err, ErrUsage)

	opts = baseOptions(t, ModeProduce)
	opts.Spec = asyncSpec()
	opts.Workload.Duration = time.Second
	opts.Access = AccessRemote
	opts.Target = ""
	_, err = Run(t.Context(), opts)
	require.ErrorIs(t, err, ErrUsage)
}

func TestProgressEventsSpanTheRun(t *testing.T) {
	opts := baseOptions(t, ModeProduce)
	opts.Spec = asyncSpec()
	opts.Workload.Duration = 2 * time.Second
	opts.Workload.Warmup = time.Second

	var mu sync.Mutex
	started := map[Phase]int{}
	ticks := 0
	opts.Progress = func(e ProgressEvent) {
		mu.Lock()
		defer mu.Unlock()
		switch e.Kind {
		case ProgressStart:
			started[e.Phase]++
		case ProgressTick:
			ticks++
		}
	}

	_, err := Run(t.Context(), opts)
	require.NoError(t, err)

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 1, started[PhaseWarmup])
	assert.Equal(t, 1, started[PhaseMeasure])
	assert.Equal(t, 1, started[PhaseDrain])
	assert.Positive(t, ticks, "a duration-bound run reports progress every second")
}

func TestStartProgressStopsCleanly(t *testing.T) {
	opts := baseOptions(t, ModeProduce)
	r := &runner{opts: opts}
	r.bound.begin(PhaseMeasure, time.Now())

	stop := r.startProgress(t.Context())
	stop()
	stop()
}
