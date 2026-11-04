package main

import (
	"os"
	"strings"
	"syscall"
	"unsafe"
)

// FileInfo identities for Windows consoles, pipes and NUL are not distinct.
// Query only a non-console character device's NT name rather than treating
// every character device as a black hole (MQLITE-148).
func stdoutUndeliverable(f *os.File) bool {
	if _, err := f.Stat(); err != nil {
		return true
	}
	handle := syscall.Handle(f.Fd())
	kind, err := syscall.GetFileType(handle)
	if err != nil || kind != syscall.FILE_TYPE_CHAR {
		return false
	}
	var mode uint32
	if syscall.GetConsoleMode(handle, &mode) == nil {
		return false
	}
	return strings.EqualFold(stdoutObjectName(handle), `\Device\Null`)
}

var stdoutNtQueryObject = syscall.NewLazyDLL("ntdll.dll").NewProc("NtQueryObject")

func stdoutObjectName(handle syscall.Handle) string {
	// A uintptr array keeps the returned UNICODE_STRING header pointer-aligned.
	var storage [1024]uintptr
	var size uint32
	result, _, _ := stdoutNtQueryObject.Call(uintptr(handle), 1,
		uintptr(unsafe.Pointer(&storage[0])), unsafe.Sizeof(storage), uintptr(unsafe.Pointer(&size)))
	if result != 0 {
		return ""
	}
	type unicodeString struct {
		length, maximumLength uint16
		buffer                *uint16
	}
	name := (*unicodeString)(unsafe.Pointer(&storage[0]))
	if name.buffer == nil || name.length == 0 || name.length%2 != 0 {
		return ""
	}
	start := uintptr(unsafe.Pointer(&storage[0]))
	address := uintptr(unsafe.Pointer(name.buffer))
	if address < start || address > start+unsafe.Sizeof(storage)-uintptr(name.length) || name.length > 2048 {
		return ""
	}
	text := (*[1024]uint16)(unsafe.Pointer(name.buffer))[: name.length/2 : name.length/2]
	return syscall.UTF16ToString(text)
}
