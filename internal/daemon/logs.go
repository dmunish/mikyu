package daemon

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"

	"github.com/dmunish/mikyu/internal/transport/protocol"
)

// runLogs replays the daemon's log ring and, with --follow, keeps the stream
// open: lines arrive until the client interrupts, the daemon stops, or the
// tap drops this follower for falling behind. Ctrl+C detaches only — the
// daemon never notices beyond a closed socket.
func runLogs(args []string, stdout, stderr io.Writer) error {
	var follow bool
	sess, err := clientSession("mikyu daemon logs", args, stdout, stderr, func(fs *flag.FlagSet) {
		fs.BoolVar(&follow, "follow", false, "Keep the stream open and print new lines until interrupted")
	})
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}

	conn, err := connect(sess.Target, sess)
	if err != nil {
		return err
	}
	defer conn.Close()

	var b protocol.Buffer
	protocol.EncodeLogsSubscribe(&b, follow)
	if err := protocol.WriteFrame(conn, protocol.OpLogs, b.Bytes()); err != nil {
		return fmt.Errorf("log stream: %w", err)
	}

	// The first Ctrl+C detaches: closing the connection unblocks the reader
	// and the daemon keeps running.
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	defer signal.Stop(interrupt)

	type frame struct {
		op   byte
		body []byte
		err  error
	}
	frames := make(chan frame, 1)
	go func() {
		r := bufio.NewReader(conn)
		for {
			op, body, err := protocol.ReadFrame(r)
			frames <- frame{op, body, err}
			if err != nil {
				return
			}
		}
	}()

	for {
		select {
		case <-interrupt:
			return nil
		case f := <-frames:
			if f.err != nil {
				// A stopped daemon ends a follow stream; anything else is an error.
				if follow && errors.Is(f.err, io.EOF) {
					return nil
				}
				return fmt.Errorf("log stream: %w", f.err)
			}
			if f.op == protocol.OpError {
				return replyError("logs", f.body)
			}
			if f.op != protocol.OpLogs {
				return fmt.Errorf("logs: unexpected reply opcode 0x%02x", f.op)
			}
			if len(f.body) == 0 {
				// Empty frame: end of stream.
				if !follow {
					return nil
				}
				return errors.New("log stream ended: the daemon dropped this follower for falling behind; reattach to replay")
			}
			if _, err := stdout.Write(f.body); err != nil {
				return err
			}
			if _, err := io.WriteString(stdout, "\n"); err != nil {
				return err
			}
		}
	}
}
