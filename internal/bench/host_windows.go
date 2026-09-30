//go:build windows

package bench

import (
	"sync"
	"syscall"
	"time"
	"unsafe"
)

var (
	kernel32 = syscall.NewLazyDLL("kernel32.dll")
	psapi    = syscall.NewLazyDLL("psapi.dll")

	procGetCurrentProcess        = kernel32.NewProc("GetCurrentProcess")
	procGetCurrentProcessId      = kernel32.NewProc("GetCurrentProcessId")
	procGetProcessTimes          = kernel32.NewProc("GetProcessTimes")
	procGetProcessMemoryInfo     = psapi.NewProc("GetProcessMemoryInfo")
	procGetProcessHandleCount    = kernel32.NewProc("GetProcessHandleCount")
	procCreateToolhelp32Snapshot = kernel32.NewProc("CreateToolhelp32Snapshot")
	procThread32First            = kernel32.NewProc("Thread32First")
	procThread32Next             = kernel32.NewProc("Thread32Next")
	procCloseHandle              = kernel32.NewProc("CloseHandle")
)

const (
	th32csSnapThread   = 0x00000004
	invalidHandleValue = ^uintptr(0)
)

// processMemoryCounters mirrors the Win32 PROCESS_MEMORY_COUNTERS structure.
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

// windowsFiletime mirrors the Win32 FILETIME structure: 64-bit 100-ns units.
type windowsFiletime struct {
	LowDateTime  uint32
	HighDateTime uint32
}

func (ft windowsFiletime) nanos100() int64 {
	return int64(ft.HighDateTime)<<32 | int64(ft.LowDateTime)
}

// threadEntry32 mirrors the Win32 THREADENTRY32 structure.
type threadEntry32 struct {
	size           uint32
	cntUsage       uint32
	threadID       uint32
	ownerProcessID uint32
	basePri        int32
	deltaPri       int32
	flags          uint32
}

// readPlatform reads the process's memory, CPU, handle, and thread counters through Win32.
func readPlatform() HostStats {
	rss, vsz := windowsMemory()
	user, kernel := windowsProcessTimes()
	return HostStats{
		RSSBytes:   rss,
		VSZBytes:   vsz,
		CPUPercent: cpuPercentDelta(int64((user + kernel) * float64(time.Second))),
		OpenFDs:    windowsHandleCount(),
		Threads:    windowsThreads(),
	}
}

// windowsMemory reports the working set and pagefile usage in bytes.
func windowsMemory() (rss, vsz int64) {
	h, _, _ := procGetCurrentProcess.Call()
	var pmc processMemoryCounters
	pmc.cb = uint32(unsafe.Sizeof(pmc))
	r, _, _ := syscall.SyscallN(procGetProcessMemoryInfo.Addr(), h, uintptr(unsafe.Pointer(&pmc)), uintptr(pmc.cb))
	if r == 0 {
		return 0, 0
	}
	return int64(pmc.WorkingSetSize), int64(pmc.PagefileUsage)
}

// windowsProcessTimes reports cumulative user and kernel CPU seconds.
func windowsProcessTimes() (user, kernel float64) {
	h, _, _ := procGetCurrentProcess.Call()
	var creation, exit, kern, usr windowsFiletime
	r, _, _ := syscall.SyscallN(procGetProcessTimes.Addr(), h,
		uintptr(unsafe.Pointer(&creation)),
		uintptr(unsafe.Pointer(&exit)),
		uintptr(unsafe.Pointer(&kern)),
		uintptr(unsafe.Pointer(&usr)),
	)
	if r == 0 {
		return 0, 0
	}
	return float64(usr.nanos100()) / 1e7, float64(kern.nanos100()) / 1e7
}

// windowsHandleCount reports the process's open handle count.
func windowsHandleCount() int64 {
	h, _, _ := procGetCurrentProcess.Call()
	var count uint32
	r, _, _ := syscall.SyscallN(procGetProcessHandleCount.Addr(), h, uintptr(unsafe.Pointer(&count)))
	if r == 0 {
		return 0
	}
	return int64(count)
}

// threadCache throttles the system-wide toolhelp snapshot to one scan per five seconds.
var threadCache struct {
	mu sync.Mutex
	at time.Time
	n  int64
}

// windowsThreads returns the process's thread count, cached for five seconds.
func windowsThreads() int64 {
	threadCache.mu.Lock()
	if time.Since(threadCache.at) < 5*time.Second {
		n := threadCache.n
		threadCache.mu.Unlock()
		return n
	}
	threadCache.mu.Unlock()

	n := countThreads()
	threadCache.mu.Lock()
	threadCache.at = time.Now()
	threadCache.n = n
	threadCache.mu.Unlock()
	return n
}

// countThreads enumerates system threads and counts this process's own.
func countThreads() int64 {
	snap, _, _ := syscall.SyscallN(procCreateToolhelp32Snapshot.Addr(), th32csSnapThread, 0)
	if snap == invalidHandleValue {
		return 0
	}
	defer func() { _, _, _ = syscall.SyscallN(procCloseHandle.Addr(), snap) }()

	pid, _, _ := procGetCurrentProcessId.Call()
	var entry threadEntry32
	entry.size = uint32(unsafe.Sizeof(entry))
	ok, _, _ := syscall.SyscallN(procThread32First.Addr(), snap, uintptr(unsafe.Pointer(&entry)))
	var count int64
	for ok != 0 {
		if entry.ownerProcessID == uint32(pid) {
			count++
		}
		entry.size = uint32(unsafe.Sizeof(entry))
		ok, _, _ = syscall.SyscallN(procThread32Next.Addr(), snap, uintptr(unsafe.Pointer(&entry)))
	}
	return count
}
