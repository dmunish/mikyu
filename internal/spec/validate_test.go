package spec

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestResolveDerivesEveryOptional(t *testing.T) {
	got := Resolve(Spec{})

	require.Equal(t, int64(64<<20), *got.Storage.MaxSegment)
	require.Equal(t, time.Hour, got.Storage.MaxSegmentAge.Duration())
	require.Equal(t, int64(1<<20), *got.Storage.MaxPayload, "min(1 MiB, 64 MiB - 36)")
	require.Equal(t, int64(32<<20), *got.Storage.MaxCache, "min(32 MiB, 64 MiB * 4)")
	require.Equal(t, DurabilitySync, *got.Storage.Durability)
	require.Equal(t, EncryptionNone, *got.Storage.Encryption)
	require.Equal(t, CompactionNone, *got.Storage.Compaction)
	require.Equal(t, int64(1<<20), *got.Flush.MaxBytes)
	require.Equal(t, 10*time.Millisecond, got.Flush.MaxWait.Duration())
	require.Equal(t, Duration(0), *got.Retention.MaxAge)
	require.Equal(t, int64(0), *got.Retention.MaxBytes)
	require.Equal(t, 30*time.Second, got.Task.Lease.Duration())
	require.Equal(t, uint32(5), *got.Task.MaxAttempts)
	require.Equal(t, uint32(100_000), *got.Task.MaxInFlight)
	require.Equal(t, uint32(100_000), *got.Task.MaxDeadLetters, "max_in_flight")

	// Resolve must not mutate its argument.
	require.Nil(t, Spec{}.Storage.MaxSegment)
}

func TestResolveDerivesFromOverrides(t *testing.T) {
	var s Spec
	s.Storage.MaxSegment = new(int64(1 << 20))
	s.Task.MaxInFlight = new(uint32(10))

	got := Resolve(s)
	require.Equal(t, int64(1<<20-RecordOverhead), *got.Storage.MaxPayload)
	require.Equal(t, int64(4<<20), *got.Storage.MaxCache, "min(32 MiB, 1 MiB * 4)")
	require.Equal(t, uint32(10), *got.Task.MaxDeadLetters)
}

func TestResolveKeepsExplicitValues(t *testing.T) {
	var s Spec
	s.Storage.MaxPayload = new(int64(512))
	s.Storage.MaxCache = new(int64(8192))
	s.Flush.MaxWait = new(Duration(time.Second))

	got := Resolve(s)
	require.Equal(t, int64(512), *got.Storage.MaxPayload, "an explicit value is never overwritten")
	require.Equal(t, int64(8192), *got.Storage.MaxCache)
	require.Equal(t, time.Second, got.Flush.MaxWait.Duration())
}

func TestFlushLinger(t *testing.T) {
	var s Spec
	s.Flush.MaxWait = new(Duration(800 * time.Millisecond))
	require.Equal(t, 100*time.Millisecond, FlushLinger(s))

	require.Equal(t, 10*time.Millisecond/8, FlushLinger(Spec{}), "unset falls back to the built-in wait")
}

// Every non-boolean spec parameter must appear here: a field with no
// out-of-range case is a field nothing checks.
func TestValidateRejectsOutOfRange(t *testing.T) {
	cases := []struct {
		key string
		v   Value
	}{
		{"max_segment", NewBytes(0)},
		{"max_segment", NewBytes(-1)},
		{"max_segment_age", NewDuration(0)},
		{"max_segment_age", NewDuration(-time.Millisecond)},
		{"max_payload", NewBytes(0)},
		{"max_payload", NewBytes(-3)},
		{"max_cache", NewBytes(0)},
		{"max_flush_bytes", NewBytes(0)},
		{"max_flush_wait", NewDuration(0)},
		{"max_flush_wait", NewDuration(-time.Millisecond)},
		{"max_retention_age", NewDuration(-time.Hour)},
		{"max_retention_bytes", NewBytes(-1)},
		{"lease", NewDuration(0)},
		{"max_attempts", NewUint32(0)},
		{"max_in_flight", NewUint32(0)},
		{"max_dead_letters", NewUint32(0)},
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
		if p.Kind == KindEnum {
			continue
		}
		require.True(t, covered[p.Key], p.Key+" has no out-of-range case")
	}
}

func TestValidateRejectsInvalidEnum(t *testing.T) {
	bad := Durability("eventual")
	err := ValidateCreate(Spec{Storage: StorageSpec{Durability: &bad}})
	require.ErrorIs(t, err, ErrInvalidConfig)
	require.Contains(t, err.Error(), "durability")

	badEnc := Encryption("vault")
	err = ValidateCreate(Spec{Storage: StorageSpec{Encryption: &badEnc}})
	require.ErrorIs(t, err, ErrInvalidConfig)
	require.Contains(t, err.Error(), "encryption")

	badComp := Compaction("eventually")
	err = ValidateCreate(Spec{Storage: StorageSpec{Compaction: &badComp}})
	require.ErrorIs(t, err, ErrInvalidConfig)
	require.Contains(t, err.Error(), "compaction")
}

func TestValidateAcceptsEdgeValues(t *testing.T) {
	var s Spec
	s.Storage.MaxSegment = new(int64(4096))
	s.Storage.MaxSegmentAge = new(Duration(time.Millisecond))
	s.Storage.MaxPayload = new(int64(4096 - RecordOverhead))
	s.Storage.MaxCache = new(int64(4096))
	s.Storage.Durability = new(DurabilityAsync)
	s.Storage.Encryption = new(EncryptionOS)
	s.Storage.Compaction = new(CompactionKeys)
	s.Flush.MaxBytes = new(int64(1))
	s.Flush.MaxWait = new(Duration(100 * time.Microsecond))
	s.Retention.MaxAge = new(Duration(0))
	s.Retention.MaxBytes = new(int64(0))
	s.Task.Lease = new(Duration(time.Millisecond))
	s.Task.MaxAttempts = new(uint32(1))
	s.Task.MaxInFlight = new(uint32(1))
	s.Task.MaxDeadLetters = new(uint32(1))

	require.NoError(t, ValidateCreate(s))
}

func TestValidateRejectsExplicitPairConflict(t *testing.T) {
	t.Run("payload exceeds segment", func(t *testing.T) {
		var s Spec
		s.Storage.MaxSegment = new(int64(4096))
		s.Storage.MaxPayload = new(int64(4096))
		err := ValidateCreate(s)
		require.ErrorIs(t, err, ErrInvalidConfig)
		require.Contains(t, err.Error(), "max_payload")
		require.Contains(t, err.Error(), "max_segment")
	})

	t.Run("payload without segment is fine", func(t *testing.T) {
		var s Spec
		s.Storage.MaxPayload = new(int64(4096))
		require.NoError(t, ValidateCreate(s), "max_segment unset here, so no pair to check")
	})

	t.Run("cache below segment", func(t *testing.T) {
		var s Spec
		s.Storage.MaxSegment = new(int64(4096))
		s.Storage.MaxCache = new(int64(1024))
		err := ValidateCreate(s)
		require.ErrorIs(t, err, ErrInvalidConfig)
		require.Contains(t, err.Error(), "max_cache")
		require.Contains(t, err.Error(), "max_segment")
	})

	t.Run("cache at the built-in budget is fine even for a huge segment", func(t *testing.T) {
		var s Spec
		s.Storage.MaxSegment = new(int64(256 << 20))
		s.Storage.MaxCache = new(int64(32 << 20))
		require.NoError(t, ValidateCreate(s))
	})

	t.Run("cache at segment is fine", func(t *testing.T) {
		var s Spec
		s.Storage.MaxSegment = new(int64(4096))
		s.Storage.MaxCache = new(int64(4096))
		require.NoError(t, ValidateCreate(s))
	})

	t.Run("cache without segment is fine", func(t *testing.T) {
		var s Spec
		s.Storage.MaxCache = new(int64(1024))
		require.NoError(t, ValidateCreate(s))
	})

	// A sparse proposal can conflict with the layer it inherits from, which is
	// why callers revalidate the resolved spec.
	t.Run("effective catches an inherited conflict", func(t *testing.T) {
		var proposal Spec
		proposal.Storage.MaxPayload = new(int64(128 << 20))
		require.NoError(t, ValidateCreate(proposal))

		conflict := proposal.Effective(Spec{}, Builtin())
		err := ValidateCreate(conflict)
		require.ErrorIs(t, err, ErrInvalidConfig)
		require.Contains(t, err.Error(), "max_payload")
	})
}

func TestValidateFlushWaitFloor(t *testing.T) {
	var s Spec
	s.Flush.MaxWait = new(Duration(50 * time.Microsecond))
	err := ValidateCreate(s)
	require.ErrorIs(t, err, ErrInvalidConfig)
	require.Contains(t, err.Error(), "max_flush_wait")
}

func TestValidateUpdateRejectsCreateOnly(t *testing.T) {
	var createOnly, mutable int

	for _, p := range Params(LayerSpec) {
		patch := patchWith(t, p.Key, sampleValueFor(p))

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
	disabled.Metrics.Enabled = false
	disabled.Metrics.Interval = 0
	require.NoError(t, ValidateSession(disabled))

	enabled := BuiltinSession()
	enabled.Metrics.Interval = 0
	err := ValidateSession(enabled)
	require.ErrorIs(t, err, ErrInvalidConfig)
	require.Contains(t, err.Error(), "metrics_interval")

	lowHistory := BuiltinSession()
	lowHistory.Metrics.History = 0
	err = ValidateSession(lowHistory)
	require.ErrorIs(t, err, ErrInvalidConfig)
	require.Contains(t, err.Error(), "metrics_history")

	badListen := BuiltinSession()
	badListen.Metrics.Listen = "no-port"
	err = ValidateSession(badListen)
	require.ErrorIs(t, err, ErrInvalidConfig)
	require.Contains(t, err.Error(), "metrics_listen")

	goodListen := BuiltinSession()
	goodListen.Metrics.Listen = "127.0.0.1:9090"
	require.NoError(t, ValidateSession(goodListen))

	noConns := BuiltinSession()
	noConns.MaxConnections = 0
	err = ValidateSession(noConns)
	require.ErrorIs(t, err, ErrInvalidConfig)
	require.Contains(t, err.Error(), "max_connections")

	badAuth := BuiltinSession()
	badAuth.Auth.Method = "kerberos"
	err = ValidateSession(badAuth)
	require.ErrorIs(t, err, ErrInvalidConfig)
	require.Contains(t, err.Error(), "auth_method")

	tokenNoSource := BuiltinSession()
	tokenNoSource.Auth.Method = "token"
	err = ValidateSession(tokenNoSource)
	require.ErrorIs(t, err, ErrInvalidConfig)
	require.Contains(t, err.Error(), "auth_token_file")

	tokenTooShort := BuiltinSession()
	tokenTooShort.Auth.Method = "token"
	tokenTooShort.Auth.Token = "short"
	err = ValidateSession(tokenTooShort)
	require.ErrorIs(t, err, ErrInvalidConfig)
	require.Contains(t, err.Error(), "auth_token")

	mtlsWithoutTLS := BuiltinSession()
	mtlsWithoutTLS.Auth.Method = "mtls"
	err = ValidateSession(mtlsWithoutTLS)
	require.ErrorIs(t, err, ErrInvalidConfig)
	require.Contains(t, err.Error(), "tls_enabled")
}

func TestIsCreateOnly(t *testing.T) {
	require.True(t, IsCreateOnly("durability"))
	require.True(t, IsCreateOnly("encryption"))
	require.True(t, IsCreateOnly("compaction"))
	require.True(t, IsCreateOnly("max_segment"))
	require.True(t, IsCreateOnly("max_payload"))
	require.False(t, IsCreateOnly("max_segment_age"))
	require.False(t, IsCreateOnly("max_cache"))
	require.False(t, IsCreateOnly("lease"))
	require.False(t, IsCreateOnly("max_dead_letters"))
	require.False(t, IsCreateOnly("nope"))
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
