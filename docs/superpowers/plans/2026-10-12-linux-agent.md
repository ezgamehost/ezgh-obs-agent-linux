# Linux Agent Implementation Plan

**Goal:** Ship the approved standalone Linux metrics/log daemon in the public repository.
**Architecture:** Native journal and file sources commit OTLP batches with checkpoints;
embedded node collectors commit metrics; independent workers export each durable queue.
**Tech stack:** Go, node_exporter, go-systemd/sdjournal, OTLP protobuf, bbolt, systemd.
**Spec:** [Approved design](../../design.md).

The user approved the design and explicitly requested repository creation and implementation.
Execution is inline, with an independent final code review. Main is the established shipping branch.

## Global constraints

- No server-side state caches, subprocess collectors, public listeners, or secret logging.
- One project/environment per state directory; reject target changes while data is pending.
- Journal/file checkpoints advance in the same synchronous transaction as their batches.
- Default 30-second gathers, 1 MiB batches, 64 KiB log bodies, bounded disk and memory.
- Native libsystemd dependency, amd64/arm64 Linux builds, non-root systemd unit.
- No backend or production deployment changes.

## Review focus

1. Crash between enqueue and checkpoint: recovery must never skip unqueued records (task 2).
2. Partial acceptance: never replay accepted records (task 3).
3. Rotation, truncation and invalid UTF-8: bounded reads and resumable identity (task 4).
4. Permissions, redirects and credential rotation: never expose secrets (tasks 1, 3, 5).
5. Blocked collectors and full disk: bounded work and independent diagnostics (tasks 2, 4, 5).

## Tasks

- [ ] 1. Configuration/identity: internal/config and internal/identity; tests for strict parsing,
  defaults, durations, reserved attributes, file permissions, stable UUID and invalid targets.
- [ ] 2. Storage/transform: internal/spool and internal/telemetry; tests for atomic enqueue/checkpoint,
  queue replay, ack, binding, format version, quota, bounded UTF-8 records and size-based splitting.
- [ ] 3. Export: internal/export; real HTTP test server covers protobuf, success/partial success,
  malformed response, retries, Retry-After, 401/403 pause, 413 split and redirect refusal.
- [ ] 4. Collection: internal/source/{journal,file,node}; tests for journal mapping/recovery,
  file rename/truncate/restart/long lines, and exact node gauge/counter conversion.
- [ ] 5. Runtime/CLI: internal/app, internal/diagnostics, cmd/ezgh-obs-agent-linux;
  tests for state locking, local status, graceful stop, configuration validation and doctor.
- [ ] 6. Packaging/qualification: packaging, scripts, .github/workflows, README;
  Linux race/vet/build, end-to-end daemon smoke, distribution package lifecycle,
  resource benchmark and final review. Document precisely any unavailable qualification.

For each task: write behavioral tests, observe failure, implement, run the relevant suite,
then commit and push the verified chunk. Linux-only collection is verified on Linux.
