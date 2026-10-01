package bench

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/goccy/go-yaml"

	"github.com/dmunish/mikyu/internal/spec"
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

// DecodeWorkloadYAML parses a scenario's workload block into a Workload.
func DecodeWorkloadYAML(data []byte) (Workload, error) {
	var doc workloadDoc
	if err := yaml.UnmarshalWithOptions(data, &doc, yaml.DisallowUnknownField()); err != nil {
		return Workload{}, fmt.Errorf("%w: workload: %w", ErrUsage, err)
	}
	schedule, err := decodeSchedule(doc.RateSchedule)
	if err != nil {
		return Workload{}, err
	}
	return Workload{
		Kind:            doc.Kind,
		Queues:          doc.Queues,
		MsgSize:         doc.MsgSize,
		Bytes:           doc.Bytes,
		Duration:        doc.Duration.Duration(),
		Rate:            doc.Rate,
		RateSchedule:    schedule,
		Workers:         doc.Workers,
		Batch:           doc.Batch,
		Keys:            doc.Keys,
		Preload:         doc.Preload,
		Warmup:          doc.Warmup.Duration(),
		AckRate:         doc.AckRate,
		CrashAfterBytes: doc.CrashAfterBytes,
		Seed:            doc.Seed,
	}, nil
}

// workloadDoc is the YAML shape of a scenario's workload block.
type workloadDoc struct {
	Kind            string              `yaml:"kind"`
	Queues          int                 `yaml:"queues"`
	MsgSize         int                 `yaml:"msg_size"`
	Bytes           int64               `yaml:"bytes"`
	Duration        spec.Duration       `yaml:"duration"`
	Rate            uint64              `yaml:"rate"`
	RateSchedule    []map[string]uint64 `yaml:"rate_schedule"`
	Workers         int                 `yaml:"workers"`
	Batch           int                 `yaml:"batch"`
	Keys            int                 `yaml:"keys"`
	Preload         int64               `yaml:"preload"`
	Warmup          spec.Duration       `yaml:"warmup"`
	AckRate         float64             `yaml:"ack_rate"`
	CrashAfterBytes int64               `yaml:"crash_after_bytes"`
	Seed            uint32              `yaml:"seed"`
}

// decodeSchedule reads the workload block's "- 30s: 100000" rate steps.
func decodeSchedule(entries []map[string]uint64) ([]RatePoint, error) {
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
