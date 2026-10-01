package spec

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseBytesAcceptsCountsAndUnits(t *testing.T) {
	tests := []struct {
		text string
		want int64
	}{
		{"0", 0},
		{"1048576", 1 << 20},
		{"1", 1},
		{"1B", 1},
		{"1b", 1},
		{"64B", 64},
		{"64MB", 64_000_000},
		{"64 MB", 64_000_000},
		{"64_MB", 64_000_000},
		{"1kb", 1_000},
		{"1KB", 1_000},
		{"1k", 1_000},
		{"1M", 1_000_000},
		{"2GB", 2_000_000_000},
		{"1TB", 1_000_000_000_000},
		{"64MiB", 64 << 20},
		{"64 MiB", 64 << 20},
		{"64mib", 64 << 20},
		{"512KiB", 512 << 10},
		{"1GiB", 1 << 30},
		{"1TiB", 1 << 40},
		{"-5MB", -5_000_000},
	}
	for _, tt := range tests {
		t.Run(tt.text, func(t *testing.T) {
			got, err := parseBytes(tt.text)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestParseBytesRejectsBadText(t *testing.T) {
	for _, text := range []string{"", "MB", "abc", "1.5MB", "12ZB", "1 2 MB", "MB1"} {
		t.Run(text, func(t *testing.T) {
			_, err := parseBytes(text)
			require.ErrorIs(t, err, ErrInvalidConfig)
		})
	}
}

func TestParseValueRoutesByteUnits(t *testing.T) {
	v, err := ParseValue(KindBytes, "256MiB")
	require.NoError(t, err)
	assert.Equal(t, int64(256<<20), v.Bytes())
	assert.Equal(t, "268435456", v.String())
}

func TestByteParamParsesUnits(t *testing.T) {
	p, ok := Lookup("max_segment")
	require.True(t, ok)

	v, err := p.Parse("256MB")
	require.NoError(t, err)
	assert.Equal(t, int64(256_000_000), v.Bytes())

	v, err = p.Parse("256 MiB")
	require.NoError(t, err)
	assert.Equal(t, int64(256<<20), v.Bytes())

	_, err = p.Parse("256MBB")
	require.ErrorIs(t, err, ErrInvalidConfig)
}

// TestByteUnitsDistinguishSIFromIEC pins the one distinction a reader can get wrong.
func TestByteUnitsDistinguishSIFromIEC(t *testing.T) {
	si, err := parseBytes("1MB")
	require.NoError(t, err)
	iec, err := parseBytes("1MiB")
	require.NoError(t, err)
	assert.Equal(t, int64(1_000_000), si)
	assert.Equal(t, int64(1<<20), iec)
	assert.NotEqual(t, si, iec)
}
