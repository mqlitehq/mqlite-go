//go:build !windows
// +build !windows

package main

import "os"

// The Go runtime may reopen an inherited closed stdout as /dev/null. Checking
// its identity also rejects that silent discard before a Receive (MQLITE-148).
func stdoutUndeliverable(f *os.File) bool {
	info, err := f.Stat()
	if err != nil {
		return true
	}
	nullInfo, err := os.Stat(os.DevNull)
	return err == nil && os.SameFile(info, nullInfo)
}
