package human

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBytes(t *testing.T) {
	tests := []struct {
		name string
		in   int64
		raw  string
		want string
	}{
		{"zero", 0, "0", "0 B"},
		{"sub-kib", 512, "512", "512 B"},
		{"largest bytes", 1023, "1023", "1023 B"},
		{"first kib", 1024, "1024", "1.0 KiB"},
		{"one and a half kib", 1536, "1536", "1.5 KiB"},
		{"mib", 1 << 20, "1048576", "1.0 MiB"},
		{"plan example", 200 << 20, "209715200", "200.0 MiB"},
		{"fractional mib", 1_600 << 10, "1638400", "1.6 MiB"},
		{"gib", 1 << 30, "1073741824", "1.0 GiB"},
		{"tib", 1 << 40, "1099511627776", "1.0 TiB"},
		{"largest unit", 1 << 50, "1125899906842624", "1.0 PiB"},
		{"beyond pi", 1 << 60, "1152921504606846976", "1.0 EiB"},
		{"beyond unit table", 1<<63 - 1, "9223372036854775807", "8.0 EiB"},
		{"negative", -1536, "-1536", "-1.5 KiB"},
		{"min int64", -1 << 63, "-9223372036854775808", "-8.0 EiB"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, Formatter{}.Bytes(tt.in))
			assert.Equal(t, tt.raw, Formatter{Raw: true}.Bytes(tt.in))
		})
	}
}

func TestCount(t *testing.T) {
	tests := []struct {
		name string
		in   int64
		raw  string
		want string
	}{
		{"zero", 0, "0", "0"},
		{"small", 7, "7", "7"},
		{"largest ungrouped", 999, "999", "999"},
		{"first grouped", 1000, "1000", "1,000"},
		{"grouped boundary", 999_999, "999999", "999,999"},
		{"first abbreviated", 1_000_000, "1000000", "1.0M"},
		{"plan example", 1_200_000, "1200000", "1.2M"},
		{"grouped example", 999_800, "999800", "999,800"},
		{"abbreviated boundary", 1_234_567, "1234567", "1.2M"},
		{"billions", 2_500_000_000, "2500000000", "2.5G"},
		{"trillions", 3_400_000_000_000, "3400000000000", "3.4T"},
		{"quadrillions", 2_000_000_000_000_000, "2000000000000000", "2.0Q"},
		{"beyond unit table", 9_000_000_000_000_000_000, "9000000000000000000", "9,000,000,000,000,000,000"},
		{"beyond int64", 1<<63 - 1, "9223372036854775807", "9,223,372,036,854,775,807"},
		{"negative", -1500, "-1500", "-1,500"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, Formatter{}.Count(tt.in))
			assert.Equal(t, tt.raw, Formatter{Raw: true}.Count(tt.in))
		})
	}
}

func TestDuration(t *testing.T) {
	tests := []struct {
		name string
		in   time.Duration
		raw  string
		want string
	}{
		{"zero", 0, "0", "0s"},
		{"nanoseconds", 12 * time.Nanosecond, "0.000000012", "12ns"},
		{"microseconds", 12 * time.Microsecond, "0.000012", "12µs"},
		{"milliseconds", 350 * time.Millisecond, "0.35", "350ms"},
		{"fractional milliseconds", 1500 * time.Microsecond, "0.0015", "1.5ms"},
		{"sub-millisecond", 12 * time.Microsecond, "0.000012", "12µs"},
		{"half a millisecond", 500 * time.Microsecond, "0.0005", "500µs"},
		{"nanoseconds", 12 * time.Nanosecond, "0.000000012", "12ns"},
		{"whole seconds", 42 * time.Second, "42", "42s"},
		{"seconds with fraction", 1500 * time.Millisecond, "1.5", "1.5s"},
		{"minutes", 4*time.Minute + 12*time.Second, "252", "4m 12s"},
		{"hours", time.Hour + 23*time.Minute, "4980", "1h 23m"},
		{"never raw seconds", 5025500 * time.Millisecond, "5025.5", "1h 23m"},
		{"drops zero second unit", time.Hour, "3600", "1h"},
		{"drops zero minute unit", 2 * time.Hour, "7200", "2h"},
		{"wire ten ms", 10 * time.Millisecond, "0.01", "10ms"},
		{"wire thirty s", 30 * time.Second, "30", "30s"},
		{"ninety s promotes to minutes", 90 * time.Second, "90", "1m 30s"},
		{"days collapse to hours", 25 * time.Hour, "90000", "25h"},
		{"negative", -90 * time.Second, "-90", "-1m 30s"},
		{"min int64", time.Duration(-1 << 63), "-9223372036.854776", "-2562047h 47m"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, Formatter{}.Duration(tt.in))
			assert.Equal(t, tt.raw, Formatter{Raw: true}.Duration(tt.in))
		})
	}
}

func TestPercent(t *testing.T) {
	tests := []struct {
		in   float64
		raw  string
		want string
	}{
		{0, "0", "0.0 %"},
		{0.124, "12.4", "12.4 %"},
		{1, "100", "100.0 %"},
		{0.5, "50", "50.0 %"},
		{-0.25, "-25", "-25.0 %"},
		{0.0017, "0.17", "0.2 %"},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.want, Formatter{}.Percent(tt.in))
		assert.Equal(t, tt.raw, Formatter{Raw: true}.Percent(tt.in))
	}
}

func TestRate(t *testing.T) {
	tests := []struct {
		name string
		in   float64
		raw  string
		want string
	}{
		{"zero", 0, "0", "0 B/s"},
		{"bytes", 512, "512", "512 B/s"},
		{"plan example", 12_582_912, "12582912", "12.0 MiB/s"},
		{"fractional", 1_572_864, "1572864", "1.5 MiB/s"},
		{"negative", -1024, "-1024", "-1.0 KiB/s"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, Formatter{}.Rate(tt.in))
			assert.Equal(t, tt.raw, Formatter{Raw: true}.Rate(tt.in))
		})
	}
}

// TestFormatterZeroValueIsHuman: the zero Formatter must be the human form so
// callers never render raw numbers by accident.
func TestFormatterZeroValueIsHuman(t *testing.T) {
	var f Formatter
	assert.False(t, f.Raw)
	assert.Equal(t, "1.0 KiB", f.Bytes(1024))
	assert.Equal(t, "1,000", f.Count(1000))
	assert.Equal(t, "1.0 %", f.Percent(0.01))
}
