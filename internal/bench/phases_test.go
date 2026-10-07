package bench

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPhaseSequence(t *testing.T) {
	assert.Equal(t, []Phase{PhaseMeasure, PhaseDrain}, phaseSequence(ModeProduce, 0))
	assert.Equal(t, []Phase{PhasePopulate, PhaseMeasure, PhaseDrain}, phaseSequence(ModeConsume, 4096))
	assert.Equal(t, []Phase{PhaseMeasure, PhaseDrain}, phaseSequence(ModeConsume, 0))
	assert.Equal(t, []Phase{PhaseMeasure, PhaseDrain}, phaseSequence(ModeMix, 0))
	assert.Equal(t, []Phase{PhasePopulate, PhaseAbort}, phaseSequence(ModeRecovery, 0))
}
