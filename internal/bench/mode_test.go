package bench

import (
	"testing"

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
