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
		var f FlushSpec
		require.NoError(t, json.Unmarshal([]byte(`{"max_wait":"10ms","max_bytes":1048576}`), &f))
		require.Equal(t, 10*time.Millisecond, f.MaxWait.Duration())
		require.Equal(t, int64(1048576), *f.MaxBytes)

		f.MaxBytes = nil
		data, err := json.Marshal(f)
		require.NoError(t, err)
		require.Equal(t, `{"max_wait":"10ms"}`, string(data))
	})

	t.Run("json nanos form", func(t *testing.T) {
		var f FlushSpec
		require.NoError(t, json.Unmarshal([]byte(`{"max_wait":10000000}`), &f))
		require.Equal(t, 10*time.Millisecond, f.MaxWait.Duration())
	})

	t.Run("yaml forms", func(t *testing.T) {
		for _, doc := range []string{
			"max_wait: 10ms\n",
			"max_wait: \"10ms\"\n",
			"max_wait: 10000000\n",
		} {
			var f FlushSpec
			require.NoError(t, yaml.Unmarshal([]byte(doc), &f), doc)
			require.NotNil(t, f.MaxWait, doc)
			require.Equal(t, 10*time.Millisecond, f.MaxWait.Duration(), doc)
		}
	})

	t.Run("rejects junk", func(t *testing.T) {
		var f FlushSpec
		require.Error(t, json.Unmarshal([]byte(`{"max_wait":"soon"}`), &f))

		err := yaml.UnmarshalWithOptions([]byte("max_wait: soon\n"), &f, yaml.DisallowUnknownField())
		require.Error(t, err)
		require.Contains(t, err.Error(), "soon")
	})
}

func TestMergePresence(t *testing.T) {
	builtin := Builtin()

	queue := Spec{}
	queue.Storage.MaxSegment = new(int64(4096))
	queue.Task.MaxAttempts = new(uint32(2))

	merged := queue.Merge(builtin)
	require.Equal(t, int64(4096), *merged.Storage.MaxSegment, "a set field wins")
	require.Equal(t, *builtin.Storage.Durability, *merged.Storage.Durability, "an unset field inherits")
	require.Equal(t, uint32(2), *merged.Task.MaxAttempts)

	require.Nil(t, Spec{}.Flush.MaxWait, "nil stays nil over nil")
	require.NotNil(t, Spec{}.Merge(builtin).Flush.MaxWait)

	require.Nil(t, queue.Storage.MaxCache, "merge must not mutate the receiver")
	require.Equal(t, Builtin(), builtin, "merge must not mutate the argument")
}

func TestEffectivePrecedence(t *testing.T) {
	builtin := Builtin()

	session := Spec{}
	session.Storage.Compaction = new(CompactionKeys)
	session.Storage.MaxSegmentAge = new(Duration(time.Hour))
	session.Task.Lease = new(Duration(time.Minute))

	queue := Spec{}
	queue.Storage.MaxSegmentAge = new(Duration(2 * time.Hour))

	eff := queue.Effective(session, builtin)
	require.Equal(t, 2*time.Hour, eff.Storage.MaxSegmentAge.Duration(), "queue spec beats session")
	require.Equal(t, CompactionKeys, *eff.Storage.Compaction, "session beats built-in")
	require.Equal(t, *builtin.Flush.MaxWait, *eff.Flush.MaxWait, "built-in fills the rest")

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

// Session nests TLS, Auth, and Metrics, so the table paths are walked recursively.
func TestTableMatchesSessionFields(t *testing.T) {
	var fromStruct []string
	collectSessionTypeLeaves(reflect.TypeFor[Session](), "server.", &fromStruct)
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
	paths := map[string]bool{}
	for _, p := range Table {
		id := p.Layer.String() + "/" + p.Key
		require.NotContains(t, seen, id, "duplicate canonical key on a layer")
		seen[id] = p

		require.False(t, paths[p.Path], "duplicate path "+p.Path)
		paths[p.Path] = true

		require.NotEmpty(t, p.Key)
		require.NotEmpty(t, p.Path)
		require.NotEqual(t, KindUnknown, p.Kind, p.Key)
		require.NotEqual(t, LayerUnspecified, p.Layer, p.Key)
		require.NotEqual(t, MutUnspecified, p.Mutability, p.Key)

		require.Equal(t, flagName(p.Key), p.FlagName, p.Key)
		require.Equal(t, optionName(p.Key), p.OptionName, p.Key)
		if p.Layer != LayerCall {
			require.Equal(t, envName(p.Key), p.EnvName, p.Key)
		}

		if p.Kind == KindEnum {
			require.NotEmpty(t, p.Allowed, p.Key+" must declare its constants")
		} else {
			require.Empty(t, p.Allowed, p.Key)
		}

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
			require.NotEqual(t, ScopeUnspecified, p.Scope, p.Key)
		}

		got, ok := lookupByLayer(p.Layer, p.Key)
		require.True(t, ok, p.Key)
		require.Equal(t, withoutFuncs(p), withoutFuncs(got))
	}

	layers := len(Params(LayerCall)) + len(Params(LayerSpec)) + len(Params(LayerSession))
	require.Equal(t, len(Table), layers)

	for _, leaf := range specLeafFields() {
		p, ok := lookupByPath(leaf.path)
		require.True(t, ok, leaf.path+" has no table entry")
		require.Equal(t, kindOf(leaf.typ.Elem()), p.Kind, leaf.path+" kind disagrees with its field type")
	}
}

func TestSettersAndGettersAgree(t *testing.T) {
	for _, p := range Params(LayerSpec) {
		want := sampleValueFor(p)

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
		p.ApplySession(&s, sampleValueFor(p))

		var changed []string
		collectSessionLeaves(reflect.ValueOf(s), "", &changed)
		require.Equal(t, []string{strings.TrimPrefix(p.Path, "server.")}, changed,
			p.Key+" must write exactly its own field")
	}
}

func TestBuiltinIsTotal(t *testing.T) {
	builtin := Builtin()
	for _, p := range Params(LayerSpec) {
		_, set := builtin.Field(p)
		require.True(t, set, "built-in layer must set "+p.Key)
	}
	require.Equal(t, Resolve(Spec{}), builtin, "built-in is the resolved empty spec")
}

func TestSparseEncoding(t *testing.T) {
	var s Spec
	s.Flush.MaxWait = new(Duration(10 * time.Millisecond))

	data, err := EncodeSpec(s)
	require.NoError(t, err)
	require.Equal(t, `{"flush":{"max_wait":"10ms"}}`, string(data))

	back, err := DecodeSpec(data)
	require.NoError(t, err)
	require.Equal(t, s, back)

	empty, err := EncodeSpec(Spec{})
	require.NoError(t, err)
	require.Equal(t, "{}", string(empty))
}

func TestDecodeYAMLMatchesBuiltin(t *testing.T) {
	doc := `
storage:
  durability: sync
  encryption: none
  compaction: none
  max_segment: 67108864
  max_segment_age: "1h"
  max_payload: 1048576
  max_cache: 33554432
flush:
  max_bytes: 1048576
  max_wait: "10ms"
retention:
  max_age: "0s"
  max_bytes: 0
task:
  lease: "30s"
  max_attempts: 5
  max_in_flight: 100000
  max_dead_letters: 100000
`
	s, err := DecodeSpecYAML([]byte(doc))
	require.NoError(t, err)
	require.Equal(t, Builtin(), s)
}

func TestDecodeRejectsUnknownKey(t *testing.T) {
	_, err := DecodeSpec([]byte(`{"storage":{"max_segment":1,"bogus":2}}`))
	require.ErrorIs(t, err, ErrInvalidConfig)
	require.Contains(t, err.Error(), "bogus")

	_, err = DecodeSpecYAML([]byte("storage:\n  max_segment: 1\n  bogus: 2\n"))
	require.ErrorIs(t, err, ErrInvalidConfig)
	require.Contains(t, err.Error(), "bogus")
}

func TestDecodeRejectsOversizedPayload(t *testing.T) {
	data := []byte(`{"padding":"` + strings.Repeat("x", MaxSpecPayload) + `"}`)
	_, err := DecodeSpec(data)
	require.ErrorIs(t, err, ErrSpecTooLarge)
}

func TestDecodeRejectsTrailingData(t *testing.T) {
	_, err := DecodeSpec([]byte(`{"flush":{"max_wait":"10ms"}} {"flush":{"max_wait":"20ms"}}`))
	require.ErrorIs(t, err, ErrInvalidConfig)
	require.Contains(t, err.Error(), "trailing")
}

func TestParseValueFromText(t *testing.T) {
	for _, p := range Table {
		text := sampleTextFor(p)
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

// KindFloat carries ratios such as the bench ack fraction; it round-trips
// through text and through the kind's string form on the wire.
func TestFloatValues(t *testing.T) {
	v, err := ParseValue(KindFloat, "0.9")
	require.NoError(t, err)
	require.Equal(t, KindFloat, v.Kind())
	require.InDelta(t, 0.9, v.Float(), 1e-9)
	require.Equal(t, "0.9", v.String())
	require.Panics(t, func() { _ = v.Int() })

	_, err = ParseValue(KindFloat, "most")
	require.Error(t, err)

	var k Kind
	require.NoError(t, json.Unmarshal([]byte(`"float"`), &k))
	require.Equal(t, KindFloat, k)
	encoded, err := json.Marshal(KindFloat)
	require.NoError(t, err)
	require.JSONEq(t, `"float"`, string(encoded))
}

func TestEnumValuesAreChecked(t *testing.T) {
	p, ok := Lookup("durability")
	require.True(t, ok)

	v, err := p.Parse("sync")
	require.NoError(t, err)
	require.Equal(t, "sync", v.Enum())

	_, err = p.Parse("eventual")
	require.ErrorIs(t, err, ErrInvalidConfig)
	require.Contains(t, err.Error(), "sync|async")

	require.Equal(t, "sync", DurabilitySync.String())
	require.Equal(t, "async", DurabilityAsync.String())
	require.False(t, Durability("eventual").Valid())
	require.Equal(t, "os", EncryptionOS.String())
	require.False(t, Encryption("vault").Valid())
	require.Equal(t, "keys", CompactionKeys.String())
	require.False(t, Compaction("eventually").Valid())
}

func TestSpecYAMLRoundTrip(t *testing.T) {
	var s Spec
	s.Storage.MaxCache = new(int64(4096))
	s.Task.Lease = new(Duration(90 * time.Second))

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
	require.Equal(t, 10, s.MaxConnections)
	require.False(t, s.TLS.Enabled)
	require.Equal(t, "none", s.Auth.Method)
}

func TestMetricsSessionDefaults(t *testing.T) {
	s := BuiltinSession()
	require.True(t, s.Metrics.Enabled)
	require.Equal(t, time.Second, s.Metrics.Interval.Duration())
	require.Equal(t, 300, s.Metrics.History)
	require.Empty(t, s.Metrics.Listen)
	require.False(t, s.Metrics.PerQueuePrometheus)
	require.True(t, s.Metrics.Histograms)
}

func TestLayerKindAndParamNames(t *testing.T) {
	require.Equal(t, "call", LayerCall.String())
	require.Equal(t, "spec", LayerSpec.String())
	require.Equal(t, "session", LayerSession.String())
	require.Equal(t, "unknown", LayerUnspecified.String())

	require.Equal(t, "timestamp", KindTime.String())
	require.Equal(t, "enum", KindEnum.String())
	require.Equal(t, "float", KindFloat.String())
	require.Equal(t, "unknown", KindUnknown.String())

	p, ok := Lookup("durability")
	require.True(t, ok)
	require.Equal(t, "durability", p.String())
	require.Equal(t, "WithDurability", p.OptionName)
	require.Equal(t, "durability", p.FlagName)
	require.Equal(t, "MIKYU_DURABILITY", p.EnvName)

	maxConn, ok := Lookup("max_connections")
	require.True(t, ok)
	require.Equal(t, "WithMaxConnections", maxConn.OptionName)
	require.Equal(t, "max-connections", maxConn.FlagName)

	tls, ok := Lookup("tls_cert")
	require.True(t, ok)
	require.Equal(t, "WithTLSCert", tls.OptionName)
	require.Equal(t, "server.tls.cert", tls.Path)

	ca, ok := Lookup("tls_client_ca")
	require.True(t, ok)
	require.Equal(t, "WithTLSClientCA", ca.OptionName)

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
	require.Contains(t, err.Error(), "1")
}

// Equal is presence-aware: a field set to zero differs from a field left unset.
func TestSpecEqual(t *testing.T) {
	seg := int64(4096)
	hour := Duration(time.Hour)
	a := Spec{
		Storage:   StorageSpec{MaxSegment: &seg, MaxSegmentAge: &hour},
		Retention: RetentionSpec{MaxAge: new(Duration)}, // explicitly zero
		Task:      TaskSpec{MaxAttempts: new(uint32)},
	}
	require.True(t, Equal(a, a))
	require.True(t, Equal(Spec{}, Spec{}))

	// A different value is a mismatch.
	bigger := int64(8192)
	b := a
	b.Storage.MaxSegment = &bigger
	require.False(t, Equal(a, b))

	// Set-to-zero differs from unset, in both directions.
	c := a
	c.Retention.MaxAge = nil
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
	require.Equal(t, "sync", sampleValue(KindEnum).String())
	require.Equal(t, "2026-01-02T03:04:05Z", sampleValue(KindTime).String())
	require.Equal(t, "0.9", sampleValue(KindFloat).String())
	require.Equal(t, "<unset>", Value{}.String())

	require.Equal(t, "sample", sampleValue(KindString).Text())
	require.Equal(t, "sync", sampleValue(KindEnum).Enum())
	require.Panics(t, func() { _ = sampleValue(KindInt).Text() })
	require.Panics(t, func() { _ = sampleValue(KindInt).Duration() })
	require.Panics(t, func() { _ = sampleValue(KindInt).Enum() })
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

// collectSessionLeaves walks Session-shaped values, appending the dotted path of every non-zero leaf.
func collectSessionLeaves(v reflect.Value, prefix string, out *[]string) {
	typ := v.Type()
	for i := range typ.NumField() {
		field := typ.Field(i)
		fv := v.Field(i)
		path := prefix + tagName(field.Tag.Get("yaml"))
		if fv.Kind() == reflect.Struct {
			collectSessionLeaves(fv, path+".", out)
			continue
		}
		if !fv.IsZero() {
			*out = append(*out, path)
		}
	}
}

// collectSessionTypeLeaves walks a Session-shaped type, appending the dotted path of every leaf.
func collectSessionTypeLeaves(typ reflect.Type, prefix string, out *[]string) {
	for field := range typ.Fields() {
		path := prefix + tagName(field.Tag.Get("yaml"))
		if field.Type.Kind() == reflect.Struct {
			collectSessionTypeLeaves(field.Type, path+".", out)
			continue
		}
		*out = append(*out, path)
	}
}

// lookupByLayer resolves a key within one layer, since call and spec layers share some canonical keys.
func lookupByLayer(layer Layer, key string) (Param, bool) {
	for _, p := range Table {
		if p.Layer == layer && p.Key == key {
			return p, true
		}
	}
	return Param{}, false
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
	case reflect.TypeFor[Durability](), reflect.TypeFor[Encryption](), reflect.TypeFor[Compaction]():
		return KindEnum
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
	case KindEnum:
		return NewEnum("sync")
	case KindTime:
		return NewTime(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))
	case KindFloat:
		return NewFloat(0.9)
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
	case KindEnum:
		return "sync"
	case KindTime:
		return "2026-01-02T03:04:05Z"
	default:
		panic("no sample text for kind " + k.String())
	}
}

func sampleValueFor(p Param) Value {
	if p.Kind == KindEnum {
		return NewEnum(p.Allowed[0])
	}
	return sampleValue(p.Kind)
}

func sampleTextFor(p Param) string {
	if p.Kind == KindEnum {
		return p.Allowed[0]
	}
	return sampleText(p.Kind)
}
