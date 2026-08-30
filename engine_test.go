package mikyu

import (
	"testing"
)

func TestEngineContract(t *testing.T) {
	var _ Engine = LastOffsetAdapter{}
}
