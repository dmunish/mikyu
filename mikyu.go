package mikyu

import "github.com/dmunish/mikyu/internal/client"

// Open starts an in-process Client over the data directory dir and
// returns queues backed directly by the local storage engine. Nothing
// listens on the network; the queues are yours alone.
//
// The directory is exclusive property. Open takes a lock file inside it,
// so a second process (or a daemon) pointed at the same directory fails
// with ErrLocked rather than corrupting anything.
//
// Opening the same directory twice inside one process shares the same
// underlying node, and the reference count drops by one every time a
// shared client closes. The node shuts down when the last client does.
// Two opens must agree on session defaults and the encryption key file,
// or the second open fails with ErrSessionDefaultsMismatch.
func Open(dir string, opts ...ClientOption) (Client, error) {
	c, err := client.NewEmbedded(dir, opts...)
	if err != nil {
		return nil, err
	}
	return newClient(c), nil
}

// Dial returns a Client that talks to a remote Mikyu daemon at target.
//
// The target is a URL-style address: "tcp://host:port", "unix:///path/
// to/socket", "pipe://name" for a Windows named pipe, or a bare
// "host:port" treated as TCP.
//
// Dial is lazy. It does not contact the server; the first operation
// does, and that is where connection problems surface. With AutoStart
// enabled (WithAutoStart), the first operation also spawns a daemon if
// nothing is serving the target yet.
//
// Pass credentials with WithAuthMethod, WithAuthToken, and
// WithAuthTokenFile, and TLS settings with WithTLSEnabled and friends.
func Dial(target string, opts ...ClientOption) (Client, error) {
	c, err := client.NewRemote(target, opts...)
	if err != nil {
		return nil, err
	}
	return newClient(c), nil
}
