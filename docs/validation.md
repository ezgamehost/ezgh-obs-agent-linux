# Validation record

Initial implementation, 2026-10-12. This record distinguishes implemented behavior
from operational qualification. No production endpoint or customer credential was
used for testing.

## Executed locally

- macOS portable unit/integration tests, including TLS daemon file delivery and shutdown.
- Linux arm64 unit/integration suite with Go race detector, including embedded node collectors.
- Durable enqueue/checkpoint/reopen, target binding, quota refusal, FIFO splitting and queue reuse.
- OTLP success/partial success, retry status codes, interrupted response, credential pause
  classification, redirect refusal and malformed-response handling.
- File rotation/restart, long lines, dense-line batch bounds, symlink rejection and
  invalid journal identifier normalization.

## CI qualification

The CI workflow builds native Linux amd64 and arm64 packages and runs real-systemd
journal/metrics TLS forwarding, non-root execution, reload, restart, package upgrade
and removal on Ubuntu 22.04 amd64 and Ubuntu 24.04 arm64. Refer to the actual Actions
run for status; the presence of a workflow does not mean it passed.

## Remaining qualification

- Complete Debian 12 and Enterprise Linux 9 real-systemd distribution matrix.
- Round-trip names, counters, resets and two-host identity through the selected EZGH
  backend's ingest and query paths (the current tests use a local OTLP receiver).
- 30-minute 1,000-record/second outage/recovery benchmark with CPU, RSS and disk results.
- Journal vacuum/reboot fault matrix and process kill at every storage boundary.

These are release-hardening tasks, not claims of passing tests. The project should
be evaluated on test hosts before a broad production rollout.
