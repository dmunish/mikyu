// Package human formats quantities for people or for machines.
package human

import (
	"math"
	"strconv"
	"strings"
	"time"
)

// Formatter renders quantities for humans, or as bare numbers when Raw is set.
type Formatter struct {
	Raw bool
}

// byteUnits are the binary suffixes Bytes and Rate abbreviate with.
var byteUnits = [...]string{"B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"}

// countUnits are the decimal suffixes Count abbreviates with.
var countUnits = [...]string{"", "K", "M", "G", "T", "Q"}

// durationUnits are the suffixes Duration walks down, coarsest first.
var durationUnits = [...]struct {
	size time.Duration
	name string
}{
	{time.Hour, "h"},
	{time.Minute, "m"},
	{time.Second, "s"},
	{time.Millisecond, "ms"},
	{time.Microsecond, "µs"},
	{time.Nanosecond, "ns"},
}

// Bytes renders a byte count in binary units, keeping one decimal above 1 KiB.
func (f Formatter) Bytes(n int64) string {
	if f.Raw {
		return strconv.FormatInt(n, 10)
	}
	sign, size := splitSign(n)
	if size < 1024 {
		return sign + strconv.FormatUint(size, 10) + " B"
	}
	div, exp := uint64(1024), 1
	for size/div >= 1024 && exp < len(byteUnits)-1 {
		div *= 1024
		exp++
	}
	return sign + fixed1(float64(size)/float64(div)) + " " + byteUnits[exp]
}

// Count abbreviates a million and up, comma-groups a thousand and up, else stays bare.
func (f Formatter) Count(n int64) string {
	if f.Raw {
		return strconv.FormatInt(n, 10)
	}
	sign, size := splitSign(n)
	if size < 1_000_000 {
		digits := strconv.FormatUint(size, 10)
		if size < 1_000 {
			return sign + digits
		}
		return sign + groupDigits(digits)
	}
	div, exp := uint64(1_000_000), 2
	for exp < len(countUnits) {
		if size/div < 1_000 {
			break
		}
		if exp == len(countUnits)-1 {
			return sign + groupDigits(strconv.FormatUint(size, 10))
		}
		div *= 1_000
		exp++
	}
	return sign + fixed1(float64(size)/float64(div)) + countUnits[exp]
}

// Duration renders d in its most significant unit, adding a second part only above a minute.
func (f Formatter) Duration(d time.Duration) string {
	if f.Raw {
		return strconv.FormatFloat(d.Seconds(), 'f', -1, 64)
	}
	if d == 0 {
		return "0s"
	}
	sign, ns := "", uint64(d)
	if d < 0 {
		sign, ns = "-", -(ns+1)+1
	}
	unit, idx := durationUnits[len(durationUnits)-1], len(durationUnits)-1
	for i, candidate := range durationUnits {
		if ns >= uint64(candidate.size) {
			unit, idx = candidate, i
			break
		}
	}
	whole, rem := ns/uint64(unit.size), ns%uint64(unit.size)
	if unit.size < time.Minute {
		return sign + trim1(float64(whole)+float64(rem)/float64(unit.size)) + unit.name
	}
	text := strconv.FormatUint(whole, 10) + unit.name
	if idx+1 >= len(durationUnits) || rem < uint64(durationUnits[idx+1].size) {
		return sign + text
	}
	next := durationUnits[idx+1]
	return sign + text + " " + strconv.FormatUint(rem/uint64(next.size), 10) + next.name
}

// Percent renders a fraction as a percentage, dropping the unit when Raw.
func (f Formatter) Percent(frac float64) string {
	if f.Raw {
		return strconv.FormatFloat(frac*100, 'f', -1, 64)
	}
	return fixed1(frac*100) + " %"
}

// Rate renders a bytes-per-second rate, abbreviated in human mode.
func (f Formatter) Rate(bytesPerSec float64) string {
	if f.Raw {
		return strconv.FormatFloat(bytesPerSec, 'f', -1, 64)
	}
	if bytesPerSec < 0 {
		return "-" + f.Bytes(int64(-bytesPerSec)) + "/s"
	}
	return f.Bytes(int64(bytesPerSec)) + "/s"
}

// splitSign returns the sign of n and its magnitude without overflowing.
func splitSign(n int64) (sign string, size uint64) {
	if n < 0 {
		return "-", uint64(-(n + 1)) + 1
	}
	return "", uint64(n)
}

// fixed1 renders v with exactly one decimal, so values in a column line up on
// the decimal point. Very large magnitudes fall back to shortest round-trip.
func fixed1(v float64) string {
	if math.Abs(v) >= 1e15 {
		return strconv.FormatFloat(v, 'f', -1, 64)
	}
	return strconv.FormatFloat(v, 'f', 1, 64)
}

// trim1 renders v with one decimal only when the value has a fraction, so 10
// reads as 10 and 10.5 as 10.5.
func trim1(v float64) string {
	return strings.TrimSuffix(fixed1(v), ".0")
}

// groupDigits inserts a thousands separator into an unsigned decimal string.
func groupDigits(s string) string {
	if len(s) <= 3 {
		return s
	}
	var b strings.Builder
	b.Grow(len(s) + len(s)/3)
	if head := len(s) % 3; head > 0 {
		b.WriteString(s[:head])
	}
	for i := len(s) % 3; i < len(s); i += 3 {
		if b.Len() > 0 {
			b.WriteByte(',')
		}
		b.WriteString(s[i : i+3])
	}
	return b.String()
}
