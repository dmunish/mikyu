package metricsview

import (
	"image/color"

	"github.com/dmunish/mikyu/internal/metrics"
	"github.com/dmunish/mikyu/internal/ui/style"
)

// HealthColor is the single place a health level becomes a color, which is what keeps the style package ignorant of the domain.
func HealthColor(t style.Theme, h metrics.Health) color.Color {
	switch h {
	case metrics.HealthCritical:
		return t.Danger
	case metrics.HealthWarn:
		return t.Warn
	default:
		return t.Ok
	}
}

// stateCell renders the state glyph for a health level, colored to match.
func stateCell(t style.Theme, h metrics.Health) string {
	return t.Paint(HealthColor(t, h)).Render(style.HealthGlyph(int(h)))
}
