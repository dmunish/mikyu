package server

import (
	"sync"

	"golang.org/x/sys/windows"
)

// verNTWorkstation marks client Windows editions, which cap concurrent TransmitFile calls.
const verNTWorkstation = 0x1

var sendfileSupported = sync.OnceValue(func() bool {
	v := windows.RtlGetVersion()
	return v != nil && v.ProductType != verNTWorkstation
})
