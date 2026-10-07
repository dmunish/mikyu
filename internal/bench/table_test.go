package bench

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dmunish/mikyu/internal/spec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEveryParamHasDoc(t *testing.T) {
	for _, p := range Table {
		assert.NotEmptyf(t, p.Doc, "%s has no doc", p.Key)
	}
}

func TestParamNames(t *testing.T) {
	for _, p := range Table {
		assert.Equal(t, "MIKYU_BENCH_"+strings.ToUpper(p.Key), p.EnvName, p.Key)
		assert.Equal(t, strings.ReplaceAll(p.Key, "_", "-"), p.FlagName, p.Key)
	}
}

func TestTableKeyOrder(t *testing.T) {
	want := []string{
		"kind", "queues", "msg_size", "bytes", "duration", "rate", "rate_schedule",
		"workers", "batch", "keys", "preload", "ack_rate", "crash_after_bytes", "seed",
	}
	got := make([]string, len(Table))
	for i, p := range Table {
		got[i] = p.Key
	}
	assert.Equal(t, want, got)
}

// TestTableCoversWorkload proves no Workload field is orphaned: applying one row moves exactly that field.
func TestTableCoversWorkload(t *testing.T) {
	rt := reflect.TypeFor[Workload]()
	for i := range rt.NumField() {
		field := rt.Field(i)
		var owners []string
		for _, p := range Table {
			w := Workload{}
			v, err := p.Parse(sampleFor(t, p))
			require.NoError(t, err, p.Key)
			p.Apply(&w, v)
			if fieldChanged(rt, w, field) {
				owners = append(owners, p.Key)
			}
		}
		assert.Lenf(t, owners, 1, "field %s is set by %v, want exactly one row", field.Name, owners)
	}
}

func fieldChanged(rt reflect.Type, w Workload, field reflect.StructField) bool {
	got := reflect.ValueOf(w).FieldByIndex(field.Index)
	switch field.Type.Kind() {
	case reflect.Slice:
		return got.Len() > 0
	default:
		return !got.IsZero()
	}
}

func TestDefaultWorkload(t *testing.T) {
	w := DefaultWorkload()
	assert.Equal(t, Workload{
		Kind:     KindLog,
		Queues:   1,
		MsgSize:  1024,
		Workers:  8,
		Batch:    64,
		AckRate:  0.9,
		Duration: 0,
		Seed:     0,
	}, w)

	w.Duration = time.Second
	require.NoError(t, w.Validate())
}

func TestValidate(t *testing.T) {
	base := func() Workload {
		w := DefaultWorkload()
		w.Duration = time.Second
		return w
	}
	tests := []struct {
		name string
		mut  func(*Workload)
		want string
	}{
		{"valid duration", func(*Workload) {}, ""},
		{"valid bytes", func(w *Workload) { w.Duration, w.Bytes = 0, 4096 }, ""},
		{"valid recovery", func(w *Workload) { w.Duration, w.CrashAfterBytes = 0, 4096 }, ""},
		{"valid keyed log", func(w *Workload) { w.Keys = 16 }, ""},
		{"valid schedule", func(w *Workload) { w.RateSchedule = []RatePoint{{At: 0, Rate: 10}, {At: time.Second, Rate: 20}} }, ""},
		{"bad kind", func(w *Workload) { w.Kind = "stream" }, "kind"},
		{"no queues", func(w *Workload) { w.Queues = 0 }, "queues"},
		{"zero msg_size", func(w *Workload) { w.MsgSize = 0 }, "msg_size"},
		{"no workers", func(w *Workload) { w.Workers = 0 }, "workers"},
		{"zero batch", func(w *Workload) { w.Batch = 0 }, "batch"},
		{"negative keys", func(w *Workload) { w.Keys = -1 }, "keys"},
		{"keyed task", func(w *Workload) { w.Kind, w.Keys = KindTask, 4 }, "keys"},
		{"ack rate high", func(w *Workload) { w.AckRate = 1.5 }, "ack_rate"},
		{"ack rate nan", func(w *Workload) { w.AckRate = nan() }, "ack_rate"},
		{"no bound", func(w *Workload) { w.Duration = 0 }, "at least one"},
		{"crash with duration", func(w *Workload) { w.CrashAfterBytes = 4096 }, "crash_after_bytes"},
		{"rate and schedule", func(w *Workload) {
			w.Rate, w.RateSchedule = 10, []RatePoint{{At: 0, Rate: 5}}
		}, "either rate or rate_schedule"},
		{"schedule not anchored", func(w *Workload) {
			w.RateSchedule = []RatePoint{{At: time.Second, Rate: 10}}
		}, "anchored at 0s"},
		{"schedule not increasing", func(w *Workload) {
			w.RateSchedule = []RatePoint{{At: 0, Rate: 10}, {At: time.Second, Rate: 20}, {At: time.Second, Rate: 30}}
		}, "strictly increasing"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := base()
			tt.mut(&w)
			err := w.Validate()
			if tt.want == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, ErrUsage)
			assert.Contains(t, err.Error(), tt.want)
		})
	}
}

func TestAppliesTo(t *testing.T) {
	produce, ok := Lookup("rate")
	require.True(t, ok)
	assert.True(t, produce.AppliesTo(ModeProduce))
	assert.True(t, produce.AppliesTo(ModeMix))
	assert.False(t, produce.AppliesTo(ModeConsume))
	assert.False(t, produce.AppliesTo(ModeRecovery))

	kind, ok := Lookup("kind")
	require.True(t, ok)
	for _, m := range Modes {
		assert.Truef(t, kind.AppliesTo(m), "an unscoped row must apply to %s", m)
	}
}

func TestRateScheduleParse(t *testing.T) {
	want := []RatePoint{
		{At: 0, Rate: 0},
		{At: 30 * time.Second, Rate: 100_000},
		{At: time.Minute, Rate: 10_000},
	}

	fromString, err := ParseRateSchedule("0:0,30s:100k,60s:10k")
	require.NoError(t, err)
	assert.Equal(t, want, fromString)

	fromYAML, err := DecodeRateScheduleYAML([]byte("- 0s: 0\n- 30s: 100000\n- 1m0s: 10000\n"))
	require.NoError(t, err)
	assert.Equal(t, want, fromYAML)
}

func TestRateScheduleRejectsGarbage(t *testing.T) {
	for _, text := range []string{"0", "0:0,30", "soon:0", "0:fast", "0:0,,", "0:0,:0"} {
		_, err := ParseRateSchedule(text)
		require.ErrorIsf(t, err, ErrUsage, "text %q", text)
	}

	for _, doc := range []string{"- 0s: 0\n  - 1s: 1\n", "- {a: 0s, b: 1s}\n", "- soon: 1\n", "[[1]]"} {
		_, err := DecodeRateScheduleYAML([]byte(doc))
		require.Errorf(t, err, "doc %q", doc)
	}
}

func TestRateSuffixes(t *testing.T) {
	points, err := ParseRateSchedule("0:1.5k,1s:2M,2s:3G,3s:4")
	require.NoError(t, err)
	require.Len(t, points, 4)
	assert.Equal(t, uint64(1500), points[0].Rate)
	assert.Equal(t, uint64(2_000_000), points[1].Rate)
	assert.Equal(t, uint64(3_000_000_000), points[2].Rate)
	assert.Equal(t, uint64(4), points[3].Rate)
}

func TestParseRejectsBadText(t *testing.T) {
	queues, ok := Lookup("queues")
	require.True(t, ok)
	_, err := queues.Parse("many")
	require.ErrorIs(t, err, ErrUsage)
	assert.Contains(t, err.Error(), "queues")

	_, ok = Lookup("nope")
	assert.False(t, ok)
}

func TestWorkloadView(t *testing.T) {
	w := DefaultWorkload()
	w.Duration = 5 * time.Second
	w.RateSchedule = []RatePoint{{At: 0, Rate: 1000}, {At: 30 * time.Second, Rate: 0}}
	w.AckRate = 0

	view := w.View()
	assert.Equal(t, "log", view.Kind)
	assert.Equal(t, "5s", view.Duration)
	assert.Equal(t, 1024, view.MsgSize)
	assert.Equal(t, 64, view.Batch)
	assert.Equal(t, 8, view.Workers)
	assert.Equal(t, "30s", view.RateSchedule[1].At)
	assert.Zero(t, view.AckRate)

	bare := Workload{Kind: KindLog}
	assert.Equal(t, "", bare.View().Duration, "an unset duration is absent, not 0s")
}

func TestEveryRowAppliesItsDefault(t *testing.T) {
	for _, p := range Table {
		if p.Default == "" {
			continue
		}
		v, err := p.Parse(p.Default)
		require.NoErrorf(t, err, "%s default %q", p.Key, p.Default)
		assert.Equalf(t, p.Kind, v.Kind(), "%s default %q has the wrong kind", p.Key, p.Default)
		assert.Truef(t, p.AppliesTo(ModeProduce) || p.AppliesTo(ModeConsume) || p.AppliesTo(ModeMix) || p.AppliesTo(ModeRecovery),
			"%s applies to no mode", p.Key)
	}
}

// sampleFor is a text that exercises a row's kind, used by the coverage test.
func sampleFor(t *testing.T, p Param) string {
	t.Helper()
	switch p.Key {
	case "rate_schedule":
		return "0:0,1s:10"
	case "kind":
		return KindLog
	}
	switch p.Kind {
	case spec.KindInt:
		return "7"
	case spec.KindUint32:
		return "11"
	case spec.KindDuration:
		return "2s"
	case spec.KindBytes:
		return "4096"
	case spec.KindString:
		return "sample"
	case spec.KindFloat:
		return "0.25"
	default:
		t.Fatalf("no sample text for kind %s", p.Kind)
		return ""
	}
}

func nan() float64 {
	zero := 0.0
	return zero / zero
}
