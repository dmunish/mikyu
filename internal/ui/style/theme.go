package style

import (
	"charm.land/lipgloss/v2"
	"image/color"
)

// Theme holds the colors one render needs, resolved by the caller.
type Theme struct {
	Plain   bool
	Accent  color.Color
	Accent2 color.Color
	Label   color.Color
	Text    color.Color
	Ok      color.Color
	Warn    color.Color
	Danger  color.Color
}

// NewTheme returns the shipped palette for a light or dark background.
func NewTheme(dark bool) Theme {
	// The brand roles extend the fire ramp the rest of the UI already uses; the
	// state roles are the semantic red/amber/green that invariant I2 reserves
	// for state.
	ld := lipgloss.LightDark(dark)
	return Theme{
		Accent:  ld(lipgloss.Color("#B45309"), lipgloss.Color("#ffca86")),
		Accent2: ld(lipgloss.Color("#BE185D"), lipgloss.Color("#e33f6a")),
		Label:   ld(lipgloss.Color("#52525B"), lipgloss.Color("#a1a1aa")),
		Text:    ld(lipgloss.Color("#18181B"), lipgloss.Color("#fafafa")),
		Ok:      ld(lipgloss.Color("#15803D"), lipgloss.Color("#4ADE80")),
		Warn:    ld(lipgloss.Color("#A16207"), lipgloss.Color("#FACC15")),
		Danger:  ld(lipgloss.Color("#B91C1C"), lipgloss.Color("#F87171")),
	}
}

// PlainTheme returns a colorless theme for pipes, dumb terminals, and goldens.
func PlainTheme() Theme {
	theme := NewTheme(false)
	theme.Plain = true
	return theme
}

// paint returns a style carrying only the theme's color, or a bare style in
// plain mode. Primitives build every attribute through paint or emph so a
// piped render cannot leak a bold or faint escape (invariant I3).
func (t Theme) paint(c color.Color) lipgloss.Style {
	if t.Plain {
		return lipgloss.NewStyle()
	}
	return lipgloss.NewStyle().Foreground(c)
}

// emph returns a bold style in the theme's color, or a bare style in plain mode.
func (t Theme) emph(c color.Color) lipgloss.Style {
	if t.Plain {
		return lipgloss.NewStyle()
	}
	return lipgloss.NewStyle().Bold(true).Foreground(c)
}

// State glyphs. Shape carries the state as well as color, so the tables stay
// readable in grayscale and to a reader who cannot see hue (invariant I2).
const (
	GlyphOK       = "●"
	GlyphWarn     = "◐"
	GlyphCritical = "○"
)

// HealthGlyph returns the state glyph for a health level index.
func HealthGlyph(level int) string {
	switch level {
	case 1:
		return GlyphWarn
	case 2:
		return GlyphCritical
	default:
		return GlyphOK
	}
}
