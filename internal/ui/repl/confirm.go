package repl

import (
	"charm.land/huh/v2"
	"charm.land/lipgloss/v2"
)

// confirm shows an inline yes/no prompt and returns the answer.
func confirm(prompt string) bool {
	var isConfirmed bool
	fullTitle := confirmArrow + " " + prompt

	theme := huh.ThemeFunc(func(isDark bool) *huh.Styles {
		s := huh.ThemeCharm(isDark)
		s.Focused.Base = lipgloss.NewStyle().Padding(0).Margin(0)
		s.Blurred.Base = lipgloss.NewStyle().Padding(0).Margin(0)
		s.Focused.Title = lipgloss.NewStyle().Foreground(lipgloss.Color("255")).Bold(true)
		s.Blurred.Title = lipgloss.NewStyle().Foreground(lipgloss.Color("255"))
		s.Focused.FocusedButton = confirmFocusedButton
		s.Focused.BlurredButton = confirmBlurredButton
		return s
	})

	err := huh.NewForm(
		huh.NewGroup(
			huh.NewConfirm().
				Title(fullTitle).
				Affirmative("Yes").
				Negative("No").
				Value(&isConfirmed).
				Inline(true).
				WithButtonAlignment(lipgloss.Left),
		).WithShowHelp(false),
	).WithTheme(theme).Run()

	if err != nil {
		return false
	}
	return isConfirmed
}
