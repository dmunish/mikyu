package command

import (
	"image/color"

	"charm.land/lipgloss/v2"
)

// Palette is the single source of color tokens for the CLI, REPL, and the
// help renderer. Every visual surface in ui/ draws from these names, so a
// palette change propagates in one edit.
var (
	AccentColor  = lipgloss.Color("#ffca86") // warm amber: headers, prompts, labels
	DangerColor  = lipgloss.Color("#e33f6a") // hot pink: required args, irreversible tags
	SuccessColor = lipgloss.Color("42")      // green: confirmations
	ErrorColor   = lipgloss.Color("196")     // red: errors
	WarnColor    = lipgloss.Color("214")     // amber: warnings
	MutedColor   = lipgloss.Color("243")     // grey: dim prose, secondary text
	SubtleColor  = lipgloss.Color("245")     // light grey: sub-labels, flag kinds
	BrightColor  = lipgloss.Color("255")     // near-white: primary values, verb names
)

// LogoGradient is the top-to-bottom fire ramp behind the REPL banner.
var LogoGradient = []color.Color{
	lipgloss.Color("#ffca86"),
	lipgloss.Color("#ff7a55"),
	lipgloss.Color("#e33f6a"),
	lipgloss.Color("#a12789"),
	lipgloss.Color("#5b0f89"),
	lipgloss.Color("#170d44"),
}

// helpStyles bundles every style a help page uses. A zero value is the
// no-color form: every field is a no-op lipgloss.Style.
type helpStyles struct {
	header      lipgloss.Style
	name        lipgloss.Style
	arg         lipgloss.Style
	reqArg      lipgloss.Style
	optArg      lipgloss.Style
	summary     lipgloss.Style
	section     lipgloss.Style
	flagSection lipgloss.Style
	flagName    lipgloss.Style
	flagKind    lipgloss.Style
	flagDoc     lipgloss.Style
	flagDef     lipgloss.Style
	dim         lipgloss.Style
	usage       lipgloss.Style
	example     lipgloss.Style
	exampleC    lipgloss.Style
}

// newHelpStyles returns a helpStyles configured for color or plain output.
func newHelpStyles(color bool) helpStyles {
	if !color {
		return helpStyles{}
	}
	return helpStyles{
		header:      lipgloss.NewStyle().Foreground(AccentColor).Bold(true),
		name:        lipgloss.NewStyle().Foreground(BrightColor).Bold(true),
		arg:         lipgloss.NewStyle().Foreground(SubtleColor),
		reqArg:      lipgloss.NewStyle().Foreground(DangerColor),
		optArg:      lipgloss.NewStyle().Foreground(MutedColor),
		summary:     lipgloss.NewStyle().Foreground(MutedColor),
		section:     lipgloss.NewStyle().Foreground(AccentColor).Bold(true),
		flagSection: lipgloss.NewStyle().Foreground(AccentColor),
		flagName:    lipgloss.NewStyle().Foreground(BrightColor),
		flagKind:    lipgloss.NewStyle().Foreground(SubtleColor),
		flagDoc:     lipgloss.NewStyle().Foreground(MutedColor),
		flagDef:     lipgloss.NewStyle().Foreground(SubtleColor),
		dim:         lipgloss.NewStyle().Foreground(MutedColor),
		usage:       lipgloss.NewStyle().Foreground(BrightColor),
		example:     lipgloss.NewStyle().Foreground(AccentColor),
		exampleC:    lipgloss.NewStyle().Foreground(MutedColor),
	}
}
