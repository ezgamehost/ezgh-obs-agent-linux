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

The [CI run for `06c3588`](https://github.com/ezgamehost/ezgh-obs-agent-linux/actions/runs/38340983655)
passed all four jobs: native Linux amd64/arm64 race tests, vet and package builds,
plus real-systemd journal/metrics TLS forwarding, non-root execution, reload,
restart, package upgrade and removal on Ubuntu 22.04 amd64 and Ubuntu 24.04 arm64.
The [v0.1.0 preview](https://github.com/ezgamehost/ezgh-obs-agent-linux/releases/tag/v0.1.0)
contains those exact build artifacts; downloaded checksums were verified before publication.

## Remaining qualification

- Complete Debian 12 and Enterprise Linux 9 real-systemd distribution matrix.
- Round-trip names, counters, resets and two-host identity through the selected EZGH
  backend's ingest and query paths (the current tests use a local OTLP receiver).
- 30-minute 1,000-record/second outage/recovery benchmark with CPU, RSS and disk results.
- Journal vacuum/reboot fault matrix and process kill at every storage boundary.

These are release-hardening tasks, not claims of passing tests. The project should
be evaluated on test hosts before a broad production rollout.

## Outage/recovery load smoke

Executed against the `06c3588` implementation on Linux arm64 (apple/container,
Debian 12 userspace), with the default host collectors, a 30-second metric interval,
a configured 8 MiB log database ceiling, and a local TLS OTLP receiver.

| Measurement | Result |
| --- | --- |
| Input period | 60 seconds |
| Input rate and record size | approximately 1,000 records/second, 1 KiB each |
| Simulated ingest failure | HTTP 503 for 15 seconds, with Retry-After |
| Records generated / accepted after recovery | 59,951 / 59,951 |
| Rejected records | 0 |
| Peak observed RSS during input | 31,965,184 bytes (30.5 MiB) |
| Peak observed log database size | 4,259,840 bytes (4.1 MiB), below 8 MiB hard limit |
| Agent CPU consumed during input | 3.5 CPU-seconds |
| Final log and metric queues | empty |

The test checks exact record counts, metric delivery, queue size and a 150 MiB RSS
ceiling. This is a short load smoke test, not the proposed 30-minute qualification
or a production performance claim. Reproduce on Linux with:

```sh
go build -o /tmp/ezgh-agent-load ./cmd/ezgh-obs-agent-linux
python3 scripts/load-smoke.py /tmp/ezgh-agent-load 60
```
