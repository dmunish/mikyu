package bench

import (
	"testing"

	"github.com/dmunish/mikyu/internal/spec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultSpecIsTheTunedRunConfiguration(t *testing.T) {
	s := DefaultSpec()
	require.NotNil(t, s.Storage.Durability)
	assert.Equal(t, spec.DurabilityAsync, *s.Storage.Durability)
	require.NotNil(t, s.Storage.MaxSegment)
	assert.Equal(t, int64(256<<20), *s.Storage.MaxSegment)
	require.NotNil(t, s.Storage.MaxCache)
	assert.Equal(t, int64(128<<20), *s.Storage.MaxCache)
	require.NotNil(t, s.Flush.MaxBytes)
	assert.Equal(t, int64(4<<20), *s.Flush.MaxBytes)
}

func TestDefaultSpecPassesValidation(t *testing.T) {
	require.NoError(t, spec.ValidateCreate(spec.Resolve(DefaultSpec())))
}

// TestNormalizeOptionsLeavesExplicitSpecAlone proves the defaults fill gaps without overriding a caller's choice.
func TestNormalizeOptionsLeavesExplicitSpecAlone(t *testing.T) {
	got := normalizeOptions(Options{Spec: spec.Spec{Storage: spec.StorageSpec{
		Durability: new(spec.Durability(spec.DurabilitySync)),
	}}})

	require.NotNil(t, got.Spec.Storage.Durability)
	assert.Equal(t, spec.DurabilitySync, *got.Spec.Storage.Durability, "an explicit durability wins")
	require.NotNil(t, got.Spec.Storage.MaxSegment, "unset fields still get the run default")
	assert.Equal(t, int64(256<<20), *got.Spec.Storage.MaxSegment)
}

func TestNormalizeOptionsAppliesDefaultsToAnEmptySpec(t *testing.T) {
	got := normalizeOptions(Options{})

	require.NotNil(t, got.Spec.Storage.Durability)
	assert.Equal(t, spec.DurabilityAsync, *got.Spec.Storage.Durability)
	assert.Equal(t, AccessEmbedded, got.Access)
	assert.Equal(t, "bench", got.Out)
	assert.Equal(t, defaultMetrics, got.Metrics)
}
