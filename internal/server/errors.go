package server

import (
	"github.com/dmunish/mikyu/internal/errs"
	"github.com/dmunish/mikyu/internal/transport/protocol"
)

func opErr(code byte, msg string) (byte, []byte) {
	return protocol.OpError, protocol.EncodeError(code, msg)
}

// taskErr maps a driver, spec, or node error to its wire error code via the shared taxonomy.
func taskErr(err error) (byte, []byte) {
	if code, ok := errs.CodeFor(err); ok {
		return opErr(code, err.Error())
	}
	return opErr(protocol.ErrCodeUnknown, err.Error())
}