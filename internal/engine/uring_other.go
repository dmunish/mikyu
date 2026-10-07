//go:build !linux

package engine

import "os"

// uringRing is a stub on every platform without io_uring: newUringRing always
// fails, so Ring is always nil and durable sinks take the syncSink path.
type uringRing struct{}
type uringSinkHandle struct{}

func newUringRing() (*uringRing, error) { return nil, errUringUnsupported }

func (*uringRing) Submit([]uringOp) error     { return errUringUnsupported }
func (*uringRing) Register() *uringSinkHandle { return nil }
func (*uringRing) Unregister(*uringSinkHandle) {}
func (*uringRing) Close() error               { return nil }

func (*uringSinkHandle) NextTag() uint32        { return 0 }
func (*uringSinkHandle) Submit([]uringOp) error { return errUringUnsupported }
func (*uringSinkHandle) Wait(uint32) error      { return errUringUnsupported }
func (*uringSinkHandle) fsync(*os.File) error   { return errUringUnsupported }
func (*uringSinkHandle) Close() error           { return nil }
