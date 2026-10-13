# ezgh-obs-agent-linux

A small Linux systemd daemon that sends host metrics and logs to
[EZGH Cloud](https://ezghcloud.com) over OTLP/HTTPS.

- Embedded Prometheus node-exporter collectors; no exporter subprocess or HTTP listener.
- Native journald reader through libsystemd; optional explicitly configured text files.
- Separate durable log and metric queues, persistent source checkpoints and host identity.
- Bounded batches, retry/backpressure handling, credential reload and local diagnostics.

This is an initial implementation. See [validation](docs/validation.md) for what has
actually been tested and the remaining release qualification. It is independent of
the Kubernetes `ezgh-obs-agent` chart. There is no remote control or self-update service.

## Install

Download the package matching your architecture from a verified build's artifacts.
Verify its SHA-256 against the accompanying `SHA256SUMS` before installing:

```sh
sudo dpkg -i ezgh-obs-agent-linux_0.1.0_amd64.deb
# Or, on an RPM system:
sudo rpm -Uvh ezgh-obs-agent-linux-0.1.0-1.x86_64.rpm
```

The service runs as `ezgh-obs-agent`. Packages grant membership in `systemd-journal`
when that group exists. Native journal support requires the distribution's
libsystemd library. Package installation does not start a new agent automatically.

Create a project ingest key in EZGH Cloud with permission to ingest the enabled
signals. Store it in `/etc/ezgh-obs-agent-linux/ingest-key` using a secure editor or
secret-management tool. Use `root:ezgh-obs-agent` ownership and mode `0640` (or a
service-owned `0600` file). Do not pass the key on a command line or put it in YAML.

Edit `/etc/ezgh-obs-agent-linux/config.yaml`:

```yaml
version: 1
target: production-hosts
endpoint: https://obs-ingest.ezghcloud.com
environment: production
credential_file: /etc/ezgh-obs-agent-linux/ingest-key
state_dir: /var/lib/ezgh-obs-agent-linux
metrics:
  enabled: true
  interval: 30s
logs:
  journal:
    enabled: true
    start_at: end
    exclude_units: [ezgh-obs-agent-linux.service]
  files: []
```

```sh
sudo ezgh-obs-agent-linux validate
sudo systemctl enable --now ezgh-obs-agent-linux
sudo -u ezgh-obs-agent ezgh-obs-agent-linux doctor
```

`validate` checks configuration only; it does not send data or read the credential.
`doctor` checks the credential, local journal access, state writability and the
running daemon's private Unix status socket. No diagnostic probe telemetry is sent.
The service's own diagnostics go to journald and are excluded from forwarding by default.

## Collection and identity

Metrics collect every 30 seconds with an initial randomized delay. The default
collectors are `cpu`, `meminfo`, `loadavg`, `diskstats`, `filesystem`, `netdev`,
`netstat`, `stat`, `time`, `uname`, `vmstat` and `os`. Configure `metrics.collectors`
to select a subset. Other upstream collectors are deliberately unavailable.

Names and labels are preserved, including `node_cpu_seconds_total` and
`node_memory_MemAvailable_bytes`. Counters are cumulative; query rates in EZGH.
Resources carry a persistent random `host.id`, current `host.name`,
`service.instance.id`, `ezgh.metadata.application=linux`, and the configured
environment. Host metrics use `service.name=node-exporter`; journal logs use the
systemd unit or syslog identifier. The ingest key selects the project; environment
selects the dataset. Self metrics use `service.name=ezgh-obs-agent-linux`.

Journal collection begins at the current tail on first start. Set
`logs.journal.start_at: beginning` to ingest retained history. Saved checkpoints
always take precedence. Vacuumed cursors fall back to their saved timestamp and
increment a gap counter; duplicates at that boundary are possible.

Optional file collection:

```yaml
logs:
  files:
    - paths: [/var/log/myapp/*.log]
      exclude: [/var/log/myapp/debug.log]
      service: myapp
      start_at: end
```

Grant read access to the service user explicitly. Avoid collecting a syslog file
that mirrors the journal. Files must be regular files; symlinks are ignored. File
identity and offsets survive restarts. Renamed open files are drained; copytruncate
can lose writes during its truncate window. A renamed file no longer matching a
configured glob cannot be recovered after a process restart. Lines are UTF-8 with
replacement of invalid bytes, truncated at 64 KiB while consuming the full line.
An unterminated line is emitted after 5 seconds of inactivity; later writes form a
new record. Multiline and structured-file parsing are not implemented.

## Delivery and operation

The default state budget allocates 256 MiB to logs and 128 MiB to metrics, leaving
128 MiB of a recommended 512 MiB state allocation for maintenance and bookkeeping.
A separate 64 MiB filesystem-free reserve is enforced. Override
`storage.log_bytes`, `storage.metric_bytes` and `storage.reserve_bytes` as integer
bytes. Admission is conservative because database pages need write headroom.

When logs fill the queue, source reading pauses. When metrics fill it, new scrapes
are skipped. Source retention can expire during an outage. A crash after server
acceptance but before local acknowledgment can duplicate data. There is no unlimited
outage buffer or exactly-once guarantee.

Transient network failures and HTTP 429/502/503/504 retry with jitter and
`Retry-After`. Authentication failures retain data and pause until credential reload.
Partial OTLP success acknowledges the batch and counts rejected items without
replaying accepted ones. Permanent payload errors are counted and discarded.

```sh
# After atomically replacing the restricted credential file:
sudo systemctl reload ezgh-obs-agent-linux
# Other configuration changes:
sudo systemctl restart ezgh-obs-agent-linux
```

`target` is an operator-assigned installation destination name, not a credential.
Changing it, the endpoint, environment, or host identity with pending data is
rejected. Keep the same target only when rotating a key for the same project. The
agent cannot infer the project from an opaque key. Moving projects requires draining
the old queue or choosing a new state directory.

Preserve `/var/lib/ezgh-obs-agent-linux` through upgrades. Do not bake its contents
into VM images: each clone needs a new ID and empty state. Corrupt/unknown-format
queues are never silently deleted. Package removal preserves state and the credential;
retiring an installation requires explicit cleanup. Configured `host_id` overrides
the generated ID and must be unique per host.

Additional limits: `logs.max_open_files` defaults to 128, `metrics.max_points` to
50,000, and `metrics.max_bytes` to 16 MiB per scrape. Over-limit scrapes are rejected
whole. One collector gather may run at a time; a blocked gather does not launch
replacement goroutines. Local diagnostics remain responsive and report its age.

## Build and test

Go is pinned in `go.mod`. Build on Linux with a C compiler, libsystemd headers,
pkg-config and CA certificates. On Debian/Ubuntu:

```sh
sudo apt-get install build-essential libsystemd-dev pkg-config
scripts/test.sh
# Also requires python3, rpm and dpkg-deb:
scripts/build.sh 0.1.0
```

`dist/` receives tar/deb/rpm artifacts, CycloneDX module inventories and checksums.
Release CI builds natively for amd64 and arm64 on a Debian 11 userspace (glibc 2.31),
then runs real-systemd smoke tests on Ubuntu runners. Local builds inherit their
build host's libc requirements; do not relabel a newer-libc binary as portable.

Portable logic tests run on macOS, but native journal/host collection needs Linux.
For macOS development use apple/container, not Docker Compose:

```sh
container run -d --name ezgh-linux-agent-build -m 4G -c 4 \
  -v "$PWD:/src" -w /src golang:1.27-bullseye sleep infinity
container exec ezgh-linux-agent-build sh -c \
  'apt-get update && apt-get install -y libsystemd-dev pkg-config python3 rpm'
container exec ezgh-linux-agent-build scripts/test.sh
```

Architecture and delivery invariants are in [docs/design.md](docs/design.md).
License: Apache-2.0. Dependency versions and licenses remain those of their upstream projects.
