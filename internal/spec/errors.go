package spec

import (
	"errors"
	"fmt"
)

var (
	// ErrInvalidConfig reports a value that is out of range or unparseable.
	ErrInvalidConfig = errors.New("spec: invalid configuration")
	// ErrImmutableField reports an update patch touching a create-only field; the message names the field.

	ErrImmutableField = errors.New("spec: immutable field")
	// ErrSpecTooLarge reports an encoded spec beyond MaxSpecPayload.
	ErrSpecTooLarge = errors.New("spec: payload too large")
	// ErrSpecTooNew reports a wire spec version this build cannot decode.
	ErrSpecTooNew = errors.New("spec: spec version too new")
)

const (
	rulePositive    = "greater than 0"
	ruleNonNegative = "0 or greater"
	ruleAtLeastOne  = "1 or greater"
)

func errInvalid(key, rule string, got any) error {
	return fmt.Errorf("%w: %s must be %s, got %v", ErrInvalidConfig, key, rule, got)
}

func errImmutable(key string) error {
	return fmt.Errorf("%w: %s cannot be changed after create", ErrImmutableField, key)
}
