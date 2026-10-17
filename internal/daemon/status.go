package daemon

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"

	"github.com/dmunish/mikyu/internal/manager"
	"github.com/dmunish/mikyu/internal/spec"
	"github.com/dmunish/mikyu/internal/transport/protocol"
)

// statusQueue is one entry of the status JSON view.
type statusQueue struct {
	Name string `json:"name"`
	Kind string `json:"kind"`
}

// statusView is the machine-readable shape of `mikyu daemon status --json`.
type statusView struct {
	Target string        `json:"target"`
	Queues []statusQueue `json:"queues"`
}

// runStatus probes the daemon at the session's target: it completes the auth
// handshake, asks for the queue list, and reports. A live daemon exits 0; a
// dead one, an unauthorized one, and any other failure exit non-zero with a
// message on the error path.
func runStatus(base *spec.Session, args []string, stdout, stderr io.Writer) error {
	var asJSON bool
	sess, err := clientSession("mikyu daemon status", args, stdout, stderr, base, func(fs *flag.FlagSet) {
		fs.BoolVar(&asJSON, "json", false, "Emit the machine-readable status")
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

	if err := protocol.WriteFrame(conn, protocol.OpList, nil); err != nil {
		return fmt.Errorf("list queues: %w", err)
	}
	op, body, err := protocol.ReadFrame(bufio.NewReader(conn))
	if err != nil {
		return fmt.Errorf("list queues: %w", err)
	}
	if op == protocol.OpError {
		return replyError("status", body)
	}
	if op != protocol.OpList {
		return fmt.Errorf("status: unexpected reply opcode 0x%02x", op)
	}

	view := statusView{Target: sess.Target, Queues: decodeQueueList(body)}
	if asJSON {
		if err := json.NewEncoder(stdout).Encode(view); err != nil {
			return err
		}
		return nil
	}
	plural := "queues"
	if len(view.Queues) == 1 {
		plural = "queue"
	}
	fmt.Fprintf(stdout, "serving %s — %d %s\n", view.Target, len(view.Queues), plural)
	return nil
}

// decodeQueueList reads an OpList body: u32 count, then per entry bytes(name) u8 kind.
func decodeQueueList(body []byte) []statusQueue {
	r := protocol.NewReader(body)
	count, err := r.ReadU32()
	if err != nil {
		return nil
	}
	queues := make([]statusQueue, 0, count)
	for range count {
		name, err := r.ReadBytes()
		if err != nil {
			break
		}
		kind, err := r.ReadU8()
		if err != nil {
			break
		}
		queues = append(queues, statusQueue{Name: string(name), Kind: manager.Kind(kind).String()})
	}
	return queues
}
