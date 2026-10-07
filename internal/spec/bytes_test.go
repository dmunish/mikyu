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
		{"64MB", 64 << 20},
		{"64 MB", 64 << 20},
		{"64_MB", 64 << 20},
		{"1kb", 1 << 10},
		{"1KB", 1 << 10},
		{"1k", 1 << 10},
		{"1M", 1 << 20},
		{"2GB", 2 << 30},
		{"1TB", 1 << 40},
		{"64MiB", 64 << 20},
		{"64 MiB", 64 << 20},
		{"64mib", 64 << 20},
		{"512KiB", 512 << 10},
		{"1GiB", 1 << 30},
		{"1TiB", 1 << 40},
		{"-5MB", -(5 << 20)},
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
	assert.Equal(t, int64(256<<20), v.Bytes())

	v, err = p.Parse("256 MiB")
	require.NoError(t, err)
	assert.Equal(t, int64(256<<20), v.Bytes())

	_, err = p.Parse("256MBB")
	require.ErrorIs(t, err, ErrInvalidConfig)
}

// TestByteUnitsTreatEverySpellingAsBinary pins the one rule a reader can get wrong.
func TestByteUnitsTreatEverySpellingAsBinary(t *testing.T) {
	for _, mag := range []struct {
		spellings []string
		scale     int64
	}{
		{[]string{"1MB", "1MiB", "1M", "1m"}, 1 << 20},
		{[]string{"1GB", "1GiB", "1G", "1g"}, 1 << 30},
		{[]string{"1TB", "1TiB", "1T", "1t"}, 1 << 40},
		{[]string{"1KB", "1KiB", "1K", "1k"}, 1 << 10},
	} {
		for _, text := range mag.spellings {
			got, err := parseBytes(text)
			require.NoErrorf(t, err, text)
			assert.Equalf(t, mag.scale, got, "%s is a binary step", text)
		}
	}
}
