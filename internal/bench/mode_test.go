package bench

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseModeAcceptsEveryMode(t *testing.T) {
	for _, mode := range Modes {
		got, err := ParseMode(string(mode))
		require.NoError(t, err)
		assert.Equal(t, mode, got)
	}

	got, err := ParseMode("  mix\n")
	require.NoError(t, err)
	assert.Equal(t, ModeMix, got, "surrounding space is not a mode")
}

func TestParseModeRejectsUnknownText(t *testing.T) {
	for _, text := range []string{"", "carrier-pigeon", "produce,consume", "PRODUCE"} {
		_, err := ParseMode(text)
		require.ErrorIs(t, err, ErrUsage, "mode %q must be rejected", text)
	}
}

func TestParseModeNamesEveryModeInItsError(t *testing.T) {
	_, err := ParseMode("nope")
	require.Error(t, err)
	for _, mode := range Modes {
		assert.Contains(t, err.Error(), string(mode))
	}
}

func TestScopeNamesListsTheModesARowAppliesTo(t *testing.T) {
	rate, ok := Lookup("rate")
	require.True(t, ok)
	assert.Equal(t, []string{"produce", "mix"}, rate.ScopeNames())

	seed, ok := Lookup("seed")
	require.True(t, ok)
	assert.Empty(t, seed.ScopeNames(), "an unscoped row applies everywhere")
}

func TestNormalizeOptionsDefaultsTheMode(t *testing.T) {
	assert.Equal(t, DefaultMode, normalizeOptions(Options{}).Mode)
	assert.Equal(t, ModeConsume, normalizeOptions(Options{Mode: ModeConsume}).Mode)
}

func TestDefaultModeIsProduce(t *testing.T) {
	assert.Equal(t, ModeProduce, DefaultMode)
}

func TestEnsureBoundMeasuresForTheDefaultDuration(t *testing.T) {
	w := DefaultWorkload()
	w.EnsureBound(ModeProduce)
	assert.Equal(t, DefaultDuration, w.Duration)
	assert.Equal(t, int64(DefaultByteCap), w.Bytes)
	require.NoError(t, w.Validate())
}

// A reading mode with no backlog measures nothing, so a bare run must backfill one.
func TestEnsureBoundBackfillsTheReadingModes(t *testing.T) {
	for _, mode := range []Mode{ModeConsume, ModeMix} {
		w := DefaultWorkload()
		w.EnsureBound(mode)
		assert.Equal(t, int64(DefaultPreload), w.Preload, mode)
		assert.Equal(t, int64(DefaultPreload), w.Bytes, "%s drains exactly its own backlog", mode)
		require.NoError(t, w.Validate(), mode)
	}
}

func TestEnsureBoundLeavesAWriteModeUnbackfilled(t *testing.T) {
	for _, mode := range []Mode{ModeProduce, ModeRecovery} {
		w := DefaultWorkload()
		w.EnsureBound(mode)
		assert.Zero(t, w.Preload, "%s has nothing to read back", mode)
	}
}

func TestEnsureBoundKeepsAnExplicitPreload(t *testing.T) {
	w := DefaultWorkload()
	w.Preload = 4096
	w.EnsureBound(ModeConsume)
	assert.Equal(t, int64(4096), w.Preload)
}

func TestEnsureBoundLeavesANamedBoundAlone(t *testing.T) {
	tests := []struct {
		name  string
		patch func(*Workload)
		want  time.Duration
	}{
		{"duration", func(w *Workload) { w.Duration = 5 * time.Second }, 5 * time.Second},
		{"bytes", func(w *Workload) { w.Bytes = 1 << 20 }, 0},
		{"crash_after_bytes", func(w *Workload) { w.CrashAfterBytes = 1 << 20 }, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w := DefaultWorkload()
			tc.patch(&w)
			w.EnsureBound(ModeMix)
			assert.Equal(t, tc.want, w.Duration, "a named bound is the only bound")
			require.NoError(t, w.Validate())
		})
	}
}

// TestEnsureBoundLeavesRecoveryAlone proves a crash run never gets a measure window it cannot use.
func TestEnsureBoundLeavesRecoveryAlone(t *testing.T) {
	w := DefaultWorkload()
	w.EnsureBound(ModeRecovery)
	assert.Zero(t, w.Duration)
	require.ErrorIs(t, w.Validate(), ErrUsage, "a recovery run still names its own crash point")
}
