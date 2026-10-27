// Package mqlite is a dependency-free, remote-only Go client for MQLite brokers.
//
// It implements the broker's JSON-over-HTTP API without embedding SQLite or the
// MQLite engine. The package intentionally supports Go 1.13 as a source-compatibility
// floor; applications should still build production binaries with a supported,
// security-patched Go release.
package mqlite
