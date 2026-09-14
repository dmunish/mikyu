//go:build windows

package node

import (
	"syscall"
	"unsafe"
)

var (
	kernel32 = syscall.NewLazyDLL("kernel32.dll")
	psapi    = syscall.NewLazyDLL("psapi.dll")

	procGetCurrentProcess    = kernel32.NewProc("GetCurrentProcess")
	procGetProcessTimes      = kernel32.NewProc("GetProcessTimes")
	procGetProcessMemoryInfo = psapi.NewProc("GetProcessMemoryInfo")
)

type processMemoryCounters struct {
	cb                         uint32
	PageFaultCount             uint32
	PeakWorkingSetSize         uintptr
	WorkingSetSize             uintptr
	QuotaPeakPagedPoolUsage    uintptr
	QuotaPagedPoolUsage        uintptr
	QuotaPeakNonPagedPoolUsage uintptr
	QuotaNonPagedPoolUsage     uintptr
	PagefileUsage              uintptr
	PeakPagefileUsage          uintptr
}

// readProcessRSS returns the process working set in bytes via GetProcessMemoryInfo.
func readProcessRSS() int64 {
	h, _, _ := procGetCurrentProcess.Call()
	var pmc processMemoryCounters
	pmc.cb = uint32(unsafe.Sizeof(pmc))
	if r, _, _ := procGetProcessMemoryInfo.Call(h, uintptr(unsafe.Pointer(&pmc)), uintptr(pmc.cb)); r == 0 {
		return 0
	}
	return int64(pmc.WorkingSetSize)
}

// windowsFiletime mirrors the Win32 FILETIME structure: a 64-bit signed value in 100-ns units.
type windowsFiletime struct {
	LowDateTime  uint32
	HighDateTime uint32
}

func (ft windowsFiletime) nanos100() int64 {
	return int64(ft.HighDateTime)<<32 | int64(ft.LowDateTime)
}

// readProcessCPU reports process CPU utilization via GetProcessTimes.
func readProcessCPU() float64 {
	h, _, _ := procGetCurrentProcess.Call()
	var creation, exit, kernel, user windowsFiletime
	if r, _, _ := procGetProcessTimes.Call(h,
		uintptr(unsafe.Pointer(&creation)),
		uintptr(unsafe.Pointer(&exit)),
		uintptr(unsafe.Pointer(&kernel)),
		uintptr(unsafe.Pointer(&user)),
	); r == 0 {
		return 0
	}
	return cpuPercent((kernel.nanos100() + user.nanos100()) * 100)
}
