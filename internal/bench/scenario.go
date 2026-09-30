package bench

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/goccy/go-yaml"
)

// rateSuffixes are the abbreviations a rate may carry; a bare number is messages per second.
var rateSuffixes = [...]struct {
	suffix string
	scale  uint64
}{
	{"k", 1_000},
	{"M", 1_000_000},
	{"G", 1_000_000_000},
}

// ParseRateSchedule reads "0:0,30s:100k,60s:10k" into its steps; suffix k means 1000, M means 1e6.
func ParseRateSchedule(text string) ([]RatePoint, error) {
	var points []RatePoint
	for _, pair := range strings.Split(text, ",") {
		at, rate, ok := strings.Cut(strings.TrimSpace(pair), ":")
		if !ok {
			return nil, fmt.Errorf("%w: rate_schedule entry %q is not duration:rate", ErrUsage, pair)
		}
		offset, err := time.ParseDuration(strings.TrimSpace(at))
		if err != nil {
			return nil, fmt.Errorf("%w: rate_schedule time %q: %w", ErrUsage, at, err)
		}
		value, err := parseRate(rate)
		if err != nil {
			return nil, err
		}
		points = append(points, RatePoint{At: offset, Rate: value})
	}
	return points, nil
}

// DecodeRateScheduleYAML reads the scenario block "- 30s: 100000" into the same steps as the string form.
func DecodeRateScheduleYAML(data []byte) ([]RatePoint, error) {
	var entries []map[string]uint64
	if err := yaml.Unmarshal(data, &entries); err != nil {
		return nil, fmt.Errorf("%w: rate_schedule: %w", ErrUsage, err)
	}
	points := make([]RatePoint, 0, len(entries))
	for _, entry := range entries {
		if len(entry) != 1 {
			return nil, fmt.Errorf("%w: rate_schedule entry must hold exactly one timestamp", ErrUsage)
		}
		for at, rate := range entry {
			offset, err := time.ParseDuration(at)
			if err != nil {
				return nil, fmt.Errorf("%w: rate_schedule time %q: %w", ErrUsage, at, err)
			}
			points = append(points, RatePoint{At: offset, Rate: rate})
		}
	}
	return points, nil
}

// parseRate reads "100000", "100k", or "1.5M" as messages per second.
func parseRate(text string) (uint64, error) {
	s := strings.TrimSpace(text)
	if s == "" {
		return 0, fmt.Errorf("%w: rate_schedule rate is empty", ErrUsage)
	}
	for _, u := range rateSuffixes {
		if !strings.HasSuffix(s, u.suffix) {
			continue
		}
		head, err := strconv.ParseFloat(strings.TrimSuffix(s, u.suffix), 64)
		if err != nil {
			return 0, fmt.Errorf("%w: rate_schedule rate %q: %w", ErrUsage, text, err)
		}
		return uint64(head * float64(u.scale)), nil
	}
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%w: rate_schedule rate %q: %w", ErrUsage, text, err)
	}
	return n, nil
}
