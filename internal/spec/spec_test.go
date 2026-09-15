package spec

import (
	"encoding/json"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/goccy/go-yaml"
	"github.com/stretchr/testify/require"
)

func TestDurationRoundTrip(t *testing.T) {
	t.Run("json string form", func(t *testing.T) {
		var e EngineSpec
		require.NoError(t, json.Unmarshal([]byte(`{"max_interval":"10ms","min_interval":"30us"}`), &e))
		require.Equal(t, 10*time.Millisecond, e.MaxInterval.Duration())
		require.Equal(t, 30*time.Microsecond, e.MinInterval.Duration())

		e.MinInterval = nil
		data, err := json.Marshal(e)
		require.NoError(t, err)
		require.Equal(t, `{"max_interval":"10ms"}`, string(data))
	})

	t.Run("json nanos form", func(t *testing.T) {
		var e EngineSpec
		require.NoError(t, json.Unmarshal([]byte(`{"max_interval":10000000}`), &e))
		require.Equal(t, 10*time.Millisecond, e.MaxInterval.Duration())
	})

	t.Run("yaml forms", func(t *testing.T) {
		for _, doc := range []string{
			"max_interval: 10ms\n",
			"max_interval: \"10ms\"\n",
			"max_interval: 10000000\n",
		} {
			var e EngineSpec
			require.NoError(t, yaml.Unmarshal([]byte(doc), &e), doc)
			require.NotNil(t, e.MaxInterval, doc)
			require.Equal(t, 10*time.Millisecond, e.MaxInterval.Duration(), doc)
		}
	})

	t.Run("rejects junk", func(t *testing.T) {
		var e EngineSpec
		require.Error(t, json.Unmarshal([]byte(`{"max_interval":"soon"}`), &e))

		err := yaml.UnmarshalWithOptions([]byte("max_interval: soon\n"), &e, yaml.DisallowUnknownField())
		require.Error(t, err)
		require.Contains(t, err.Error(), "soon")
	})
}

func TestMergePresence(t *testing.T) {
	builtin := Builtin()

	queue := Spec{}
	queue.Engine.BatchSize = new(7)
	queue.Task.DefaultMaxAttempts = new(uint32(2))

	merged := queue.Merge(builtin)
	require.Equal(t, 7, *merged.Engine.BatchSize, "a set field wins")
	require.Equal(t, *builtin.Engine.Fsync, *merged.Engine.Fsync, "an unset field inherits")
	require.Equal(t, uint32(2), *merged.Task.DefaultMaxAttempts)

	require.Nil(t, Spec{}.Engine.MaxInterval, "nil stays nil over nil")
	require.NotNil(t, Spec{}.Merge(builtin).Engine.MaxInterval)

	require.Nil(t, queue.Engine.MinInterval, "merge must not mutate the receiver")
	require.Equal(t, Builtin(), builtin, "merge must not mutate the argument")
}

func TestEffectivePrecedence(t *testing.T) {
	builtin := Builtin()

	session := Spec{}
	session.Engine.Fsync = new(false)
	session.Engine.SegmentAge = new(Duration(time.Hour))
	session.Task.DefaultVisibility = new(Duration(time.Minute))

	queue := Spec{}
	queue.Engine.Fsync = new(true)

	eff := queue.Effective(session, builtin)
	require.True(t, *eff.Engine.Fsync, "queue spec beats session")
	require.Equal(t, time.Minute, eff.Task.DefaultVisibility.Duration(), "session beats built-in")
	require.Equal(t, *builtin.Engine.MaxInterval, *eff.Engine.MaxInterval, "built-in fills the rest")

	for _, p := range Params(LayerSpec) {
		_, set := eff.Field(p)
		require.True(t, set, "effective spec must be total: "+p.Key)
	}
}

func TestSpecTagsIdentical(t *testing.T) {
	var walk func(reflect.Type, string)
	walk = func(typ reflect.Type, prefix string) {
		for field := range typ.Fields() {
			path := prefix + field.Name
			jsonTag, hasJSON := field.Tag.Lookup("json")
			yamlTag, hasYAML := field.Tag.Lookup("yaml")
			require.True(t, hasJSON, path+" has no json tag")
			require.True(t, hasYAML, path+" has no yaml tag")
			require.Equal(t, tagName(jsonTag), tagName(yamlTag), path+" key text differs")

			if field.Type.Kind() == reflect.Struct {
				walk(field.Type, path+".")
				continue
			}
			require.Equal(t, reflect.Pointer, field.Type.Kind(), path+" must be a pointer to stay sparse")
		}
	}
	walk(reflect.TypeFor[Spec](), "")

	// The parameter table is the wire schema for non-Go SDKs: every exported
	// field serializes, the function bindings stay out, and the keys are stable.
	pt := reflect.TypeFor[Param]()
	for field := range pt.Fields() {
		tag, ok := field.Tag.Lookup("json")
		require.True(t, ok, "Param."+field.Name+" has no json tag")
		switch field.Name {
		case "Apply", "ApplySession":
			require.Equal(t, "-", tag, "Param."+field.Name)
		default:
			require.NotEqual(t, "-", tag, "Param."+field.Name)
			require.NotEmpty(t, tagName(tag), "Param."+field.Name)
		}
	}
}

func TestTableMatchesSpecFields(t *testing.T) {
	var fromTable []string
	for _, p := range Params(LayerSpec) {
		fromTable = append(fromTable, p.Path)
	}
	slices.Sort(fromTable)

	var fromStruct []string
	for _, leaf := range specLeafFields() {
		fromStruct = append(fromStruct, leaf.path)
	}
	require.Equal(t, fromTable, fromStruct, "the table and Spec must describe the same fields")
}

// Session has no per-field getter, so its table binding is checked by shape:
// every yaml key must have exactly one session param.
func TestTableMatchesSessionFields(t *testing.T) {
	var fromStruct []string
	for field := range reflect.TypeFor[Session]().Fields() {
		fromStruct = append(fromStruct, "server."+tagName(field.Tag.Get("yaml")))
	}
	slices.Sort(fromStruct)

	var fromTable []string
	for _, p := range Params(LayerSession) {
		fromTable = append(fromTable, p.Path)
	}
	slices.Sort(fromTable)

	require.Equal(t, fromTable, fromStruct, "the table and Session must describe the same fields")
}

func TestTableSelfCheck(t *testing.T) {
	seen := map[string]Param{}
	for _, p := range Table {
		require.NotContains(t, seen, p.Key, "duplicate canonical key")
		seen[p.Key] = p

		require.NotEmpty(t, p.Key)
		require.NotEqual(t, KindUnknown, p.Kind, p.Key)
		require.NotEqual(t, LayerUnspecified, p.Layer, p.Key)
		require.NotEqual(t, MutUnspecified, p.Mutability, p.Key)

		require.Equal(t, flagName(p.Key), p.FlagName, p.Key)
		require.Equal(t, optionName(p.Key), p.OptionName, p.Key)
		if p.Layer != LayerCall {
			require.Equal(t, envName(p.Key), p.EnvName, p.Key)
		}

		normalized := strings.ReplaceAll(p.Path, ".", "_")
		require.True(t, normalized == p.Key || strings.HasSuffix(normalized, "_"+p.Key),
			p.Path+" does not name "+p.Key)

		switch p.Layer {
		case LayerCall:
			require.Empty(t, p.EnvName, "per-call values are arguments, not config")
			require.Nil(t, p.Apply)
			require.Nil(t, p.ApplySession)
		case LayerSpec:
			require.NotNil(t, p.Apply, p.Key)
			require.Nil(t, p.ApplySession, p.Key)
		case LayerSession:
			require.NotNil(t, p.ApplySession, p.Key)
			require.Nil(t, p.Apply, p.Key)
			require.NotEmpty(t, p.EnvName, p.Key)
		}

		got, ok := Lookup(p.Key)
		require.True(t, ok, p.Key)
		require.Equal(t, withoutFuncs(p), withoutFuncs(got))
	}

	layers := len(Params(LayerCall)) + len(Params(LayerSpec)) + len(Params(LayerSession))
	require.Equal(t, len(Table), layers)

	for _, leaf := range specLeafFields() {
		p, ok := lookupByPath(leaf.path)
		require.True(t, ok, leaf.path+" has no table entry")
		require.Contains(t, []string{leaf.key, leaf.group + "_" + leaf.key}, p.Key,
			leaf.path+" canonical key is neither the leaf nor its group prefix")
		require.Equal(t, kindOf(leaf.typ.Elem()), p.Kind, leaf.path+" kind disagrees with its field type")
	}
}

func TestSettersAndGettersAgree(t *testing.T) {
	for _, p := range Params(LayerSpec) {
		want := sampleValue(p.Kind)

		var s Spec
		p.Apply(&s, want)

		got, set := s.Field(p)
		require.True(t, set, p.Key)
		require.Equal(t, want, got, p.Key)
	}
}

func TestSessionSettersLandOnTheirOwnField(t *testing.T) {
	for _, p := range Params(LayerSession) {
		var s Session
		p.ApplySession(&s, sampleValue(p.Kind))

		var changed []string
		v := reflect.ValueOf(s)
		for i := range v.NumField() {
			if !v.Field(i).IsZero() {
				changed = append(changed, tagName(v.Type().Field(i).Tag.Get("yaml")))
			}
		}
		require.Equal(t, []string{p.Key}, changed, p.Key+" must write exactly its own field")
	}
}

func TestBuiltinIsTotalAndValid(t *testing.T) {
	builtin := Builtin()
	for _, p := range Params(LayerSpec) {
		_, set := builtin.Field(p)
		require.True(t, set, "built-in layer must set "+p.Key)
	}
	require.NoError(t, ValidateCreate(builtin))
}

func TestSparseEncoding(t *testing.T) {
	var s Spec
	s.Engine.Fsync = new(true)

	data, err := EncodeSpec(s)
	require.NoError(t, err)
	require.Equal(t, `{"engine":{"fsync":true}}`, string(data))

	back, err := DecodeSpec(data)
	require.NoError(t, err)
	require.Equal(t, s, back)

	empty, err := EncodeSpec(Spec{})
	require.NoError(t, err)
	require.Equal(t, "{}", string(empty))
}

func TestDecodeYAMLMatchesBuiltin(t *testing.T) {
	doc := `
engine:
  batch_size: 256
  max_interval: "10ms"
  min_interval: "30us"
  segment_size: 67108864
  max_message_bytes: 1048576
  segment_age: "1h"
  encrypted: false
  fsync: true
retention:
  age: "0s"
  bytes: 0
task:
  default_visibility: "30s"
  default_max_attempts: 5
`
	s, err := DecodeSpecYAML([]byte(doc))
	require.NoError(t, err)
	require.Equal(t, Builtin(), s)
	require.NoError(t, ValidateCreate(s))
}

func TestDecodeRejectsUnknownKey(t *testing.T) {
	_, err := DecodeSpec([]byte(`{"engine":{"fsync":true,"bogus":1}}`))
	require.ErrorIs(t, err, ErrInvalidConfig)
	require.Contains(t, err.Error(), "bogus")

	_, err = DecodeSpecYAML([]byte("engine:\n  fsync: true\n  bogus: 1\n"))
	require.ErrorIs(t, err, ErrInvalidConfig)
	require.Contains(t, err.Error(), "bogus")
}

func TestDecodeRejectsOversizedPayload(t *testing.T) {
	data := []byte(`{"padding":"` + strings.Repeat("x", MaxSpecPayload) + `"}`)
	_, err := DecodeSpec(data)
	require.ErrorIs(t, err, ErrSpecTooLarge)
}

func TestDecodeRejectsTrailingData(t *testing.T) {
	_, err := DecodeSpec([]byte(`{"engine":{"fsync":true}} {"engine":{"fsync":false}}`))
	require.ErrorIs(t, err, ErrInvalidConfig)
	require.Contains(t, err.Error(), "trailing")
}

func TestParseValueFromText(t *testing.T) {
	for _, p := range Table {
		text := sampleText(p.Kind)
		v, err := p.Parse(text)
		require.NoError(t, err, p.Key)
		require.Equal(t, p.Kind, v.Kind(), p.Key)

		if p.Kind == KindString {
			continue
		}
		_, err = p.Parse("not-a-" + p.Kind.String())
		require.ErrorIs(t, err, ErrInvalidConfig, p.Key)
	}
}

func TestParseValueAcceptsNanos(t *testing.T) {
	v, err := ParseValue(KindDuration, "10000000")
	require.NoError(t, err)
	require.Equal(t, 10*time.Millisecond, v.Duration())
}

func TestSpecYAMLRoundTrip(t *testing.T) {
	var s Spec
	s.Engine.MinInterval = new(Duration(30 * time.Microsecond))
	s.Task.DefaultVisibility = new(Duration(90 * time.Second))

	data, err := yaml.Marshal(s)
	require.NoError(t, err)

	back, err := DecodeSpecYAML(data)
	require.NoError(t, err)
	require.Equal(t, s, back)
}

func TestBuiltinSessionLayer(t *testing.T) {
	s := BuiltinSession()
	require.Equal(t, "tcp://127.0.0.1:7420", s.Target)
	require.Empty(t, s.DataDir)
	require.Equal(t, 10, s.MaxConns)
	require.False(t, s.TLSEnabled)
}

func TestMetricsSessionDefaults(t *testing.T) {
	s := BuiltinSession()
	require.True(t, s.MetricsEnabled)
	require.Equal(t, time.Second, s.MetricsInterval.Duration())
	require.Equal(t, 300, s.MetricsHistory)
	require.Empty(t, s.MetricsListen)
	require.False(t, s.MetricsPerQueuePrometheus)
	require.True(t, s.MetricsHistograms)
}

func TestLayerKindAndParamNames(t *testing.T) {
	require.Equal(t, "call", LayerCall.String())
	require.Equal(t, "spec", LayerSpec.String())
	require.Equal(t, "session", LayerSession.String())
	require.Equal(t, "unknown", LayerUnspecified.String())

	require.Equal(t, "timestamp", KindTime.String())
	require.Equal(t, "unknown", KindUnknown.String())

	p, ok := Lookup("batch_size")
	require.True(t, ok)
	require.Equal(t, "batch_size", p.String())
	require.Equal(t, "WithBatchSize", p.OptionName)
	require.Equal(t, "batch-size", p.FlagName)
	require.Equal(t, "MIKYU_BATCH_SIZE", p.EnvName)

	tls, ok := Lookup("tls_cert_file")
	require.True(t, ok)
	require.Equal(t, "WithTLSCertFile", tls.OptionName)

	require.Panics(t, func() { _ = sampleValue(KindBool).Time() })
	require.Panics(t, func() { Builtin().Field(tls) })
}

// The wire carries the spec schema number, not a protocol version.
func TestCheckSpecVersion(t *testing.T) {
	require.Equal(t, uint16(1), CurrentSpecVersion)
	require.NoError(t, CheckSpecVersion(0))
	require.NoError(t, CheckSpecVersion(CurrentSpecVersion))

	err := CheckSpecVersion(CurrentSpecVersion + 1)
	require.ErrorIs(t, err, ErrSpecTooNew)
	require.Contains(t, err.Error(), "2")
}

// Equal is presence-aware: a field set to zero differs from a field left unset.
func TestSpecEqual(t *testing.T) {
	seven := 7
	hour := Duration(time.Hour)
	a := Spec{
		Engine:    EngineSpec{BatchSize: &seven, SegmentAge: &hour},
		Retention: RetentionSpec{Age: new(Duration)}, // explicitly zero
		Task:      TaskSpec{DefaultMaxAttempts: new(uint32)},
	}
	require.True(t, Equal(a, a))
	require.True(t, Equal(Spec{}, Spec{}))

	// A different value is a mismatch.
	eight := 8
	b := a
	b.Engine.BatchSize = &eight
	require.False(t, Equal(a, b))

	// Set-to-zero differs from unset, in both directions.
	c := a
	c.Retention.Age = nil
	require.False(t, Equal(a, c))
	require.False(t, Equal(c, a))
}

// The kind-checked accessors panic on mismatch; String must not, because it is
// what fmt and testify use when reporting a Value.
func TestValueRendersAnyKind(t *testing.T) {
	require.Equal(t, "128", sampleValue(KindInt).String())
	require.Equal(t, "1m30s", sampleValue(KindDuration).String())
	require.Equal(t, "true", sampleValue(KindBool).String())
	require.Equal(t, "sample", sampleValue(KindString).String())
	require.Equal(t, "2026-01-02T03:04:05Z", sampleValue(KindTime).String())
	require.Equal(t, "<unset>", Value{}.String())

	require.Equal(t, "sample", sampleValue(KindString).Text())
	require.Panics(t, func() { _ = sampleValue(KindInt).Text() })
	require.Panics(t, func() { _ = sampleValue(KindInt).Duration() })
}

// UnixNano overflows outside the years 1678-2262, so a Value stores Times whole.
func TestValueTimeRoundTrip(t *testing.T) {
	for _, ts := range []time.Time{{}, time.Date(1200, 6, 5, 4, 3, 2, 0, time.UTC)} {
		require.Equal(t, ts.UTC(), NewTime(ts).Time())
	}

	now := time.Now()
	require.WithinDuration(t, now.UTC(), NewTime(now).Time(), time.Millisecond)
}

func TestSentinelsAreDistinct(t *testing.T) {
	all := []error{ErrInvalidConfig, ErrImmutableField, ErrSpecTooLarge, ErrSpecTooNew}
	for i, a := range all {
		for j, b := range all {
			if i != j {
				require.NotErrorIs(t, a, b)
			}
		}
	}
}

func withoutFuncs(p Param) Param {
	p.Apply = nil
	p.ApplySession = nil
	return p
}

func tagName(tag string) string {
	name, _, _ := strings.Cut(tag, ",")
	return name
}

type leafField struct {
	path  string
	group string
	key   string
	typ   reflect.Type
}

// specLeafFields walks Spec and reports each serialized leaf, sorted by path.
func specLeafFields() []leafField {
	var out []leafField
	var walk func(reflect.Type, string)
	walk = func(typ reflect.Type, group string) {
		for field := range typ.Fields() {
			key := tagName(field.Tag.Get("json"))
			if field.Type.Kind() == reflect.Struct {
				walk(field.Type, key)
				continue
			}
			out = append(out, leafField{path: group + "." + key, group: group, key: key, typ: field.Type})
		}
	}
	walk(reflect.TypeFor[Spec](), "")
	slices.SortFunc(out, func(a, b leafField) int { return strings.Compare(a.path, b.path) })
	return out
}

func lookupByPath(path string) (Param, bool) {
	for _, p := range Table {
		if p.Path == path {
			return p, true
		}
	}
	return Param{}, false
}

func kindOf(typ reflect.Type) Kind {
	switch typ {
	case reflect.TypeFor[int]():
		return KindInt
	case reflect.TypeFor[uint32]():
		return KindUint32
	case reflect.TypeFor[int64]():
		return KindBytes
	case reflect.TypeFor[Duration]():
		return KindDuration
	case reflect.TypeFor[bool]():
		return KindBool
	case reflect.TypeFor[string]():
		return KindString
	case reflect.TypeFor[time.Time]():
		return KindTime
	default:
		return KindUnknown
	}
}

func sampleValue(k Kind) Value {
	switch k {
	case KindInt:
		return NewInt(128)
	case KindUint32:
		return NewUint32(7)
	case KindDuration:
		return NewDuration(90 * time.Second)
	case KindBytes:
		return NewBytes(1 << 20)
	case KindBool:
		return NewBool(true)
	case KindString:
		return NewString("sample")
	case KindTime:
		return NewTime(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))
	default:
		panic("no sample value for kind " + k.String())
	}
}

func sampleText(k Kind) string {
	switch k {
	case KindInt:
		return "128"
	case KindUint32:
		return "7"
	case KindDuration:
		return "90s"
	case KindBytes:
		return "4096"
	case KindBool:
		return "true"
	case KindString:
		return "sample"
	case KindTime:
		return "2026-01-02T03:04:05Z"
	default:
		panic("no sample text for kind " + k.String())
	}
}
