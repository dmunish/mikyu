package daemon

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/dmunish/mikyu/internal/spec"
	"golang.org/x/term"
)

// confirmStop asks the user on a terminal; tests override it.
var confirmStop = func(target string) (bool, error) {
	if !term.IsTerminal(int(os.Stdin.Fd())) {
		return false, errors.New("refusing to stop without --yes when stdin is not a terminal")
	}
	fmt.Fprintf(os.Stderr, "stop daemon at %s? [y/N]: ", target)
	line, err := bufio.NewReader(os.Stdin).ReadString('\n')
	if err != nil && line == "" {
		return false, err
	}
	answer := strings.ToLower(strings.TrimSpace(line))
	return answer == "y" || answer == "yes", nil
}

// runStop asks the daemon at the session's target to drain and exit. The
// confirmation follows delete and truncate: prompted on a terminal, required
// as --yes everywhere else. Stopping an already-dead target reports it.
func runStop(base *spec.Session, args []string, stdout, stderr io.Writer) error {
	var yes bool
	sess, err := clientSession("mikyu daemon stop", args, stdout, stderr, base, func(fs *flag.FlagSet) {
		fs.BoolVar(&yes, "yes", false, "Skip the confirmation")
	})
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}

	if !yes {
		ok, err := confirmStop(sess.Target)
		if err != nil {
			return fmt.Errorf("%w: %v", ErrUsage, err)
		}
		if !ok {
			fmt.Fprintln(stdout, "canceled")
			return nil
		}
	}

	if err := SendShutdownSignal(sess.Target, sess); err != nil {
		return err
	}
	fmt.Fprintf(stdout, "stopped %s\n", sess.Target)
	return nil
}
