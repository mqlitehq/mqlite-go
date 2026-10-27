# Security policy

Security fixes are provided for the latest tagged `mqlite-go` release. Broker
security and supported broker versions are tracked by the main
[MQLite repository](https://github.com/mqlitehq/mqlite).

Do not disclose a suspected vulnerability in a public issue. Use GitHub's
private vulnerability reporting for this repository. Include affected versions,
reproduction steps, impact, and any evidence that a credential or destructive
request crossed an unexpected boundary. Do not include live tokens.

The `go 1.13` directive is a source-compatibility floor. Go 1.13 itself is
end-of-life; official CLI artifacts use current stable, and consumers should use
a supported Go toolchain for production builds.
