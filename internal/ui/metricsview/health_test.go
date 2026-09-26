package metricsview

import (
	"strings"
	"testing"

	"charm.land/lipgloss/v2"
	"github.com/charmbracelet/x/ansi"
	"github.com/dmunish/mikyu/internal/metrics"
	"github.com/dmunish/mikyu/internal/ui/style"
	"github.com/stretchr/testify/assert"
)

// TestHealthColorMapsEveryLevel pins the one place health meets color.
func TestHealthColorMapsEveryLevel(t *testing.T) {
	theme := style.NewTheme(true)
	ok := HealthColor(theme, metrics.HealthOK)
	warn := HealthColor(theme, metrics.HealthWarn)
	critical := HealthColor(theme, metrics.HealthCritical)
	assert.NotEqual(t, ok, warn, "healthy and degraded must not share a color")
	assert.NotEqual(t, warn, critical, "degraded and critical must not share a color")
	assert.Equal(t, theme.Ok, ok)
	assert.Equal(t, theme.Warn, warn)
	assert.Equal(t, theme.Danger, critical)
	assert.Equal(t, ok, HealthColor(theme, metrics.Health(-1)), "an unknown level reads healthy")
}

// TestStateCellIsColoredByState is the invariant I2 check, one glyph at a time.
func TestStateCellIsColoredByState(t *testing.T) {
	theme := style.NewTheme(true)
	seen := map[string]bool{}
	for _, h := range []metrics.Health{metrics.HealthOK, metrics.HealthWarn, metrics.HealthCritical} {
		cell := stateCell(theme, h)
		assert.Contains(t, cell, "\x1b[", "a state cell is always colored on a terminal")
		assert.Equal(t, style.HealthGlyph(int(h)), ansi.Strip(cell), "the glyph carries the state by shape too")
		seen[sgrOf(cell)] = true
	}
	assert.Len(t, seen, 3, "each state gets its own color")
}

// TestStateCellIsBareInPlainMode: a pipe gets the glyph and nothing else.
func TestStateCellIsBareInPlainMode(t *testing.T) {
	theme := style.PlainTheme()
	for _, h := range []metrics.Health{metrics.HealthOK, metrics.HealthWarn, metrics.HealthCritical} {
		assert.Equal(t, style.HealthGlyph(int(h)), stateCell(theme, h))
		assert.NotContains(t, stateCell(theme, h), "\x1b")
	}
}

// TestStateGlyphsAreDistinct keeps the tables readable in grayscale.
func TestStateGlyphsAreDistinct(t *testing.T) {
	assert.NotEqual(t, style.HealthGlyph(0), style.HealthGlyph(1))
	assert.NotEqual(t, style.HealthGlyph(1), style.HealthGlyph(2))
	assert.NotEqual(t, style.HealthGlyph(0), style.HealthGlyph(2))
}

// sgrOf returns the escape sequence a styled fragment opens with.
func sgrOf(s string) string {
	if !strings.HasPrefix(s, "\x1b[") {
		return ""
	}
	if i := strings.IndexByte(s, 'm'); i >= 0 {
		return s[:i+1]
	}
	return ""
}

// TestStateGlyphCellWidth: every glyph must measure one cell, or the STATE
// column stops lining up.
func TestStateGlyphCellWidth(t *testing.T) {
	for _, level := range []int{0, 1, 2} {
		assert.Equal(t, 1, lipgloss.Width(style.HealthGlyph(level)),
			"glyph for level %d must be one cell wide", level)
	}
}
