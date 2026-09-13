package spec

import "fmt"

// ValidateCreate checks a proposed spec.
func ValidateCreate(s Spec) error { return validate(s) }

// ValidateUpdate rejects a patch that sets a create-only field, naming it, then validates the fields the patch does set.
func ValidateUpdate(patch Spec) error {
	for _, p := range Params(LayerSpec) {
		if p.Mutability != MutCreateOnly {
			continue
		}
		if _, set := patch.Field(p); set {
			return errImmutable(p.Key)
		}
	}
	return validate(patch)
}

func validate(s Spec) error {
	e, r, t := s.Engine, s.Retention, s.Task
	if err := positive("batch_size", e.BatchSize); err != nil {
		return err
	}
	if err := positive("max_interval", e.MaxInterval); err != nil {
		return err
	}
	if err := nonNegative("min_interval", e.MinInterval); err != nil {
		return err
	}
	if err := positive("segment_size", e.SegmentSize); err != nil {
		return err
	}
	if err := positive("segment_age", e.SegmentAge); err != nil {
		return err
	}
	if err := positive("max_message_bytes", e.MaxMessageBytes); err != nil {
		return err
	}
	if err := nonNegative("retention_age", r.Age); err != nil {
		return err
	}
	if err := nonNegative("retention_bytes", r.Bytes); err != nil {
		return err
	}
	if err := positive("default_visibility", t.DefaultVisibility); err != nil {
		return err
	}
	if err := positive("default_max_attempts", t.DefaultMaxAttempts); err != nil {
		return err
	}
	return validatePairs(s)
}

func validatePairs(s Spec) error {
	size, msg := s.Engine.SegmentSize, s.Engine.MaxMessageBytes
	if size != nil && msg != nil && *msg > *size {
		return fmt.Errorf("%w: max_message_bytes (%d) exceeds segment_size (%d)", ErrInvalidConfig, *msg, *size)
	}
	return nil
}

// numeric bounds the magnitude checks to the types Spec fields actually use;
// Duration's underlying type is int64.
type numeric interface {
	~int | ~int64 | ~uint32
}

func positive[T numeric](key string, p *T) error {
	if p != nil && *p <= 0 {
		return errInvalid(key, rulePositive, *p)
	}
	return nil
}

func nonNegative[T numeric](key string, p *T) error {
	if p != nil && *p < 0 {
		return errInvalid(key, ruleNonNegative, *p)
	}
	return nil
}
