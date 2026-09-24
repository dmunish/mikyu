package daemon

import (
	"fmt"

	"github.com/dmunish/mikyu/internal/spec"
)

// enumValue is a flag.Value that accepts only the constants a table row declares.
type enumValue struct {
	param   spec.Param
	value   string
	changed bool
}

func newEnumValue(key string) *enumValue {
	p, _ := spec.Lookup(key)
	return &enumValue{param: p}
}

func (e *enumValue) String() string { return e.value }

func (e *enumValue) Set(text string) error {
	v, err := e.param.Parse(text)
	if err != nil {
		return fmt.Errorf("%v", err)
	}
	e.value = v.Enum()
	e.changed = true
	return nil
}
