package benchview

import (
	"io"

	"github.com/dmunish/mikyu/internal/bench/result"
	"github.com/dmunish/mikyu/internal/ui/human"
	"github.com/dmunish/mikyu/internal/ui/style"
)

// Show loads dir's envelope and writes its summary.
func Show(w io.Writer, dir string, theme style.Theme, fm human.Formatter) error {
	r, err := result.ReadResult(dir)
	if err != nil {
		return err
	}
	return Summary(w, r, dir, theme, fm)
}
