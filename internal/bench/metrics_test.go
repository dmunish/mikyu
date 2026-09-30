package bench

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetricsDefault(t *testing.T) {
	assert.Equal(t, []string{"throughput"}, normalizeOptions(Options{}).Metrics)
}

func TestMetricsPreserved(t *testing.T) {
	assert.Equal(t, []string{"p50", "p99"}, normalizeOptions(Options{Metrics: []string{"p50", "p99"}}).Metrics)
}

func TestMetricsValidation(t *testing.T) {
	require.NoError(t, validateMetrics([]string{"throughput", "p50", "p99", "queue_depth"}))
	require.ErrorIs(t, validateMetrics([]string{"bogus"}), ErrUsage)
}

func TestRunRejectsUnknownMetric(t *testing.T) {
	opts := baseOptions(t, ModeProduce)
	opts.Spec = asyncSpec()
	opts.Workload.Bytes = 128 << 10
	opts.Metrics = []string{"nope"}

	_, err := Run(t.Context(), opts)
	require.ErrorIs(t, err, ErrUsage)
}
