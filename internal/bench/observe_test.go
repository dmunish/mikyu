package bench

import (
	"io"
	"testing"
	"time"

	"github.com/dmunish/mikyu"
	"github.com/dmunish/mikyu/internal/bench/result"
	"github.com/stretchr/testify/require"
)

func TestObserverStop(t *testing.T) {
	client, err := mikyu.Open(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	env, err := newEnvelope(t.TempDir(), result.Result{Version: result.CurrentVersion})
	require.NoError(t, err)

	obs := newObserver(client, env, NewMeasure(), func() Phase { return PhaseMeasure }, nil, KindLog, io.Discard)
	obs.Start(t.Context())
	time.Sleep(1100 * time.Millisecond)
	obs.Stop()
	require.NoError(t, env.series.Close())
}
