package mikyu_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/dmunish/mikyu"
	"github.com/dmunish/mikyu/internal/spec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSchemaIsTotalAndStable pins the published table: every row names itself
// on each surface, and the kinds and layers are the ones this build knows.
func TestSchemaIsTotalAndStable(t *testing.T) {
	for _, p := range mikyu.Schema() {
		require.NotEmpty(t, p.Key)
		require.NotEmpty(t, p.FlagName, p.Key)
		require.NotEmpty(t, p.OptionName, p.Key)
		if p.Layer != mikyu.LayerCall {
			require.NotEmpty(t, p.EnvName, p.Key)
		}
		require.NotEqual(t, spec.KindUnknown, p.Kind, p.Key)
		require.NotEqual(t, spec.LayerUnspecified, p.Layer, p.Key)
	}
	assert.Equal(t, len(spec.Table), len(mikyu.Schema()))
}

func TestLookupFindsEverySchemaKey(t *testing.T) {
	for _, p := range mikyu.Schema() {
		got, ok := mikyu.Lookup(p.Key)
		require.True(t, ok, "Lookup(%q) misses", p.Key)
		a, err := json.Marshal(p)
		require.NoError(t, err)
		b, err := json.Marshal(got)
		require.NoError(t, err)
		assert.Equal(t, a, b, p.Key)
	}
}

func TestSchemaCopyIsIsolated(t *testing.T) {
	first := mikyu.Schema()
	first[0].Key = "tampered"
	second := mikyu.Schema()
	require.NotEqual(t, "tampered", second[0].Key)
}

func TestEncodeDecodeJSONRoundTrip(t *testing.T) {
	s := fullSpec(t)
	data, err := mikyu.EncodeSpec(s)
	require.NoError(t, err)
	back, err := mikyu.DecodeSpec(data)
	require.NoError(t, err)
	assert.Equal(t, s, back)
}

func TestEncodeDecodeYAMLRoundTrip(t *testing.T) {
	s := fullSpec(t)
	data, err := mikyu.EncodeSpecYAML(s)
	require.NoError(t, err)
	back, err := mikyu.DecodeSpecYAML(data)
	require.NoError(t, err)
	assert.Equal(t, s, back)
}

// TestJSONIsValidYAML pins that YAML 1.2 stays a JSON superset for the spec
// schema: anything the wire sends is readable by the YAML config path.
func TestJSONIsValidYAML(t *testing.T) {
	data, err := mikyu.EncodeSpec(fullSpec(t))
	require.NoError(t, err)
	_, err = mikyu.DecodeSpecYAML(data)
	require.NoError(t, err)
}

func TestDecodeSpecRejectsUnknownKey(t *testing.T) {
	_, err := mikyu.DecodeSpec([]byte(`{"engine":{"bogus":1}}`))
	require.ErrorIs(t, err, mikyu.ErrInvalidConfig)
	assert.Contains(t, err.Error(), "bogus")
}

func TestValidateCreateRejectsPairConflict(t *testing.T) {
	s := mikyu.Builtin()
	s.Engine.SegmentSize = new(int64(1 << 10))
	s.Engine.MaxMessageBytes = new(int64(1 << 20))
	err := mikyu.ValidateCreate(s)
	require.ErrorIs(t, err, mikyu.ErrInvalidConfig)
	assert.Contains(t, err.Error(), "max_message_bytes")
	assert.Contains(t, err.Error(), "segment_size")
}

// TestValidateUpdateRejectsCreateOnly exercises every create-only row of the
// published table through the public ValidateUpdate.
func TestValidateUpdateRejectsCreateOnly(t *testing.T) {
	createOnly := 0
	for _, p := range mikyu.Schema() {
		if p.Layer != mikyu.LayerSpec || p.Mutability != spec.MutCreateOnly {
			continue
		}
		createOnly++
		err := mikyu.ValidateUpdate(specWith(t, p))
		require.ErrorIs(t, err, mikyu.ErrImmutableField, p.Key)
		assert.Contains(t, err.Error(), p.Key)
	}
	assert.Equal(t, 4, createOnly, "the table gained or lost a create-only spec parameter")
}

func TestBuiltinIsTotal(t *testing.T) {
	for _, p := range mikyu.Schema() {
		if p.Layer != mikyu.LayerSpec {
			continue
		}
		_, set := mikyu.Builtin().Field(p)
		require.True(t, set, "Builtin() leaves "+p.Key+" unset")
	}
}

func TestBuiltinSessionIsTotal(t *testing.T) {
	s := mikyu.BuiltinSession()
	require.NotEmpty(t, s.Target)
	require.Greater(t, s.MaxConns, 0)
	require.Greater(t, s.MetricsInterval.Duration(), time.Duration(0))
	require.Greater(t, s.MetricsHistory, 0)
}

// TestSchemaGateParity holds the facade to the table: every option name the
// schema publishes exists as a With* constructor on the public package.
func TestSchemaGateParity(t *testing.T) {
	have := map[string]bool{}
	for _, name := range exportedFuncs(t, ".") {
		if strings.HasPrefix(name, "With") {
			have[name] = true
		}
	}
	for _, p := range mikyu.Schema() {
		if p.OptionName == "" {
			continue
		}
		assert.True(t, have[p.OptionName], "option constructor %s is missing from the SDK", p.OptionName)
	}
}
