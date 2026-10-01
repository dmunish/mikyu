package bench

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCacheStateWarmByDefault(t *testing.T) {
	opts := baseOptions(t, ModeProduce)
	opts.Spec = asyncSpec()
	opts.Workload.Bytes = 128 << 10

	res, err := Run(t.Context(), opts)
	require.NoError(t, err)
	assert.Equal(t, cacheWarm, res.Environment.CacheState)
	assert.False(t, res.Environment.ColdVerified)
}

func TestCacheStateColdRun(t *testing.T) {
	opts := baseOptions(t, ModeProduce)
	opts.Spec = asyncSpec()
	opts.Workload.Bytes = 128 << 10
	opts.Cold = true

	res, err := Run(t.Context(), opts)
	require.NoError(t, err)

	switch runtime.GOOS {
	case "linux":
		assert.Contains(t, []string{cacheCold, cacheUnknown}, res.Environment.CacheState)
		assert.Equal(t, res.Environment.CacheState == cacheCold, res.Environment.ColdVerified)
	default:
		assert.Equal(t, cacheUnknown, res.Environment.CacheState)
		assert.False(t, res.Environment.ColdVerified)
	}
}

func TestDropCacheUnknownOnThisPlatform(t *testing.T) {
	if runtime.GOOS == "linux" {
		t.Skip("linux dropCache depends on the host page cache")
	}
	verified, err := dropCache(t.TempDir())
	require.NoError(t, err)
	assert.False(t, verified)
}
