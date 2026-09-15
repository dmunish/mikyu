package spec

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Every non-boolean spec parameter must appear here: a field with no
// out-of-range case is a field nothing checks.
func TestValidateRejectsOutOfRange(t *testing.T) {
	cases := []struct {
		key string
		v   Value
	}{
		{"batch_size", NewInt(0)},
		{"batch_size", NewInt(-3)},
		{"max_interval", NewDuration(0)},
		{"max_interval", NewDuration(-time.Millisecond)},
		{"min_interval", NewDuration(-time.Second)},
		{"segment_size", NewBytes(0)},
		{"segment_age", NewDuration(0)},
		{"max_message_bytes", NewBytes(-1)},
		{"retention_age", NewDuration(-time.Hour)},
		{"retention_bytes", NewBytes(-1)},
		{"default_visibility", NewDuration(0)},
		{"default_max_attempts", NewUint32(0)},
	}

	covered := map[string]bool{}
	for _, tc := range cases {
		covered[tc.key] = true
		_, ok := Lookup(tc.key)
		require.True(t, ok, tc.key+" is not a table key")

		t.Run(tc.key, func(t *testing.T) {
			err := ValidateCreate(patchWith(t, tc.key, tc.v))
			require.ErrorIs(t, err, ErrInvalidConfig)
			require.Contains(t, err.Error(), tc.key, "the error must name the canonical key")

			if !IsCreateOnly(tc.key) {
				require.ErrorIs(t, ValidateUpdate(patchWith(t, tc.key, tc.v)), ErrInvalidConfig)
			}
		})
	}

	for _, p := range Params(LayerSpec) {
		if p.Kind == KindBool {
			continue
		}
		require.True(t, covered[p.Key], p.Key+" has no out-of-range case")
	}
}

func TestValidateAcceptsEdgeValues(t *testing.T) {
	var s Spec
	s.Engine.MinInterval = new(Duration(0))
	s.Engine.Encrypted = new(false)
	s.Engine.Fsync = new(false)
	s.Engine.SegmentSize = new(int64(4096))
	s.Engine.MaxMessageBytes = new(int64(4096))
	s.Retention.Age = new(Duration(0))
	s.Retention.Bytes = new(int64(0))

	require.NoError(t, ValidateCreate(s))
}

func TestValidateMessageTooLargeForSegment(t *testing.T) {
	var s Spec
	s.Engine.SegmentSize = new(int64(1024))
	s.Engine.MaxMessageBytes = new(int64(4096))

	err := ValidateCreate(s)
	require.ErrorIs(t, err, ErrInvalidConfig)
	require.Contains(t, err.Error(), "max_message_bytes")
	require.Contains(t, err.Error(), "segment_size")
}

// A sparse proposal can conflict with the layer it inherits from, which is why
// callers revalidate the resolved spec.
func TestEffectiveSpecCatchesInheritedConflict(t *testing.T) {
	var proposal Spec
	proposal.Engine.MaxMessageBytes = new(int64(128 << 20))
	require.NoError(t, ValidateCreate(proposal), "segment_size unset here, so no pair to check")

	conflict := proposal.Effective(Spec{}, Builtin())
	require.ErrorIs(t, ValidateCreate(conflict), ErrInvalidConfig)
}

func TestValidateUpdateRejectsCreateOnly(t *testing.T) {
	var createOnly, mutable int

	for _, p := range Params(LayerSpec) {
		patch := patchWith(t, p.Key, sampleValue(p.Kind))

		switch p.Mutability {
		case MutCreateOnly:
			createOnly++
			err := ValidateUpdate(patch)
			require.ErrorIs(t, err, ErrImmutableField, p.Key)
			require.Contains(t, err.Error(), p.Key, "message must name the field")
			require.NoError(t, ValidateCreate(patch), p.Key+" is fine at create")
		case MutMutable:
			mutable++
			require.NoError(t, ValidateUpdate(patch), p.Key)
			require.NoError(t, ValidateCreate(patch), p.Key)
		default:
			t.Fatalf("%s: spec params must be mutable or create-only", p.Key)
		}
	}

	require.Equal(t, len(Params(LayerSpec))-len(Updatable()), createOnly)
	require.NotZero(t, createOnly, "the table must have create-only fields to reject")
	require.Equal(t, len(Updatable()), mutable)
	require.NotEmpty(t, Updatable())
}

func TestValidateSession(t *testing.T) {
	require.NoError(t, ValidateSession(BuiltinSession()), "built-in session is valid")

	// Interval may be zero only when metrics are disabled.
	disabled := BuiltinSession()
	disabled.MetricsEnabled = false
	disabled.MetricsInterval = 0
	require.NoError(t, ValidateSession(disabled))

	enabled := BuiltinSession()
	enabled.MetricsInterval = 0
	err := ValidateSession(enabled)
	require.ErrorIs(t, err, ErrInvalidConfig)
	require.Contains(t, err.Error(), "metrics_interval")

	lowHistory := BuiltinSession()
	lowHistory.MetricsHistory = 0
	err = ValidateSession(lowHistory)
	require.ErrorIs(t, err, ErrInvalidConfig)
	require.Contains(t, err.Error(), "metrics_history")

	badListen := BuiltinSession()
	badListen.MetricsListen = "no-port"
	err = ValidateSession(badListen)
	require.ErrorIs(t, err, ErrInvalidConfig)
	require.Contains(t, err.Error(), "metrics_listen")

	goodListen := BuiltinSession()
	goodListen.MetricsListen = "127.0.0.1:9090"
	require.NoError(t, ValidateSession(goodListen))

	noConns := BuiltinSession()
	noConns.MaxConns = 0
	err = ValidateSession(noConns)
	require.ErrorIs(t, err, ErrInvalidConfig)
	require.Contains(t, err.Error(), "max_conns")
}

func TestIsCreateOnly(t *testing.T) {
	require.True(t, IsCreateOnly("segment_size"))
	require.True(t, IsCreateOnly("max_message_bytes"))
	require.True(t, IsCreateOnly("encrypted"))
	require.True(t, IsCreateOnly("fsync"))
	require.False(t, IsCreateOnly("batch_size"))
	require.False(t, IsCreateOnly("retention_age"))
	require.False(t, IsCreateOnly("nope"))
	require.False(t, IsCreateOnly("visibility"))
}

func patchWith(t *testing.T, key string, v Value) Spec {
	t.Helper()

	p, ok := Lookup(key)
	require.True(t, ok, key)
	require.NotNil(t, p.Apply, key)

	var s Spec
	p.Apply(&s, v)
	return s
}
