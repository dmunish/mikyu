package daemon

import (
	"log/slog"

	charmlog "charm.land/log/v2"
	"github.com/dmunish/mikyu/internal/spec"
)

// setupLogging makes charm.land/log the presentation for every slog call in
// the process: the daemon's lifecycle lines and the warnings internal
// packages already emit. Only the level is configurable; everything else
// stays the library default — stderr, timestamped, text, styled only on a
// terminal.
func setupLogging(sess spec.Session) {
	if level, err := charmlog.ParseLevel(sess.LogLevel); err == nil {
		charmlog.SetLevel(level)
	}
	slog.SetDefault(slog.New(charmlog.Default()))
}
