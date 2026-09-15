package spec

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/goccy/go-yaml"
)

const (
	// CurrentSpecVersion is the schema number of the JSON encoded Spec.

	CurrentSpecVersion uint16 = 1

	// MaxSpecPayload bounds any encoded Spec, patch, or describe payload in bytes.
	MaxSpecPayload = 1 << 20
)

// Duration serializes as a Go duration string ("10ms") and accepts int64 nanoseconds on the way in, in both JSON and YAML.
type Duration time.Duration

// Duration returns the wrapped value.
func (d Duration) Duration() time.Duration { return time.Duration(d) }

func (d Duration) String() string { return d.Duration().String() }

func (d Duration) MarshalJSON() ([]byte, error) { return json.Marshal(d.String()) }

func (d *Duration) UnmarshalJSON(data []byte) error {
	text := string(bytes.TrimSpace(data))
	if text == "null" {
		return nil
	}
	if len(text) > 1 && text[0] == '"' {
		var s string
		if err := json.Unmarshal(data, &s); err != nil {
			return err
		}
		return d.parse(s)
	}
	return d.parseNanos(text)
}

func (d Duration) MarshalYAML() (any, error) { return d.String(), nil }

func (d *Duration) UnmarshalYAML(data []byte) error {
	return d.parse(string(bytes.TrimSpace(data)))
}

// parse accepts a duration string, a quoted duration string, or bare nanos.
func (d *Duration) parse(text string) error {
	if unquoted, err := strconv.Unquote(text); err == nil {
		text = unquoted
	}
	switch text {
	case "", "null", "~":
		*d = 0
		return nil
	}
	if parsed, err := time.ParseDuration(text); err == nil {
		*d = Duration(parsed)
		return nil
	}
	return d.parseNanos(text)
}

func (d *Duration) parseNanos(text string) error {
	nanos, err := strconv.ParseInt(text, 10, 64)
	if err != nil {
		return fmt.Errorf("%w: bad duration %q: want \"10ms\" or nanoseconds", ErrInvalidConfig, text)
	}
	*d = Duration(nanos)
	return nil
}

// CheckSpecVersion rejects a wire schema number this build cannot decode.
func CheckSpecVersion(version uint16) error {
	if version > CurrentSpecVersion {
		return fmt.Errorf("%w: frame claims %d, this build knows %d", ErrSpecTooNew, version, CurrentSpecVersion)
	}
	return nil
}

// EncodeSpec renders s as sparse JSON: unset fields are absent.
func EncodeSpec(s Spec) ([]byte, error) {
	data, err := json.Marshal(s)
	if err != nil {
		return nil, fmt.Errorf("spec: encode: %w", err)
	}
	return data, nil
}

// EncodeSpecYAML renders s as sparse YAML: unset fields are absent.
func EncodeSpecYAML(s Spec) ([]byte, error) {
	data, err := yaml.Marshal(s)
	if err != nil {
		return nil, fmt.Errorf("spec: encode yaml: %w", err)
	}
	return data, nil
}

// DecodeSpec parses JSON, rejecting unknown keys, trailing data, and any payload larger than MaxSpecPayload.
func DecodeSpec(data []byte) (Spec, error) {
	if len(data) > MaxSpecPayload {
		return Spec{}, fmt.Errorf("%w: spec is %d bytes, limit is %d", ErrSpecTooLarge, len(data), MaxSpecPayload)
	}
	var s Spec
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&s); err != nil {
		return Spec{}, fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}
	if err := dec.Decode(new(json.RawMessage)); !errors.Is(err, io.EOF) {
		return Spec{}, fmt.Errorf("%w: trailing data after spec object", ErrInvalidConfig)
	}
	return s, nil
}

// DecodeSpecYAML parses a Spec from YAML, rejecting unknown keys.
func DecodeSpecYAML(data []byte) (Spec, error) {
	var s Spec
	if err := yaml.UnmarshalWithOptions(data, &s, yaml.DisallowUnknownField()); err != nil {
		return Spec{}, fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}
	return s, nil
}
