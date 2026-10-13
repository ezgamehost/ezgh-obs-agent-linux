# EZGH Linux observability agent design

Status: **approved design; initial implementation present**. Written 2026-10-12.
See [validation.md](validation.md) for executed checks and remaining qualification. The owner confirmed
the name `ezgh-obs-agent-linux`, Go, one executable with embedded node-exporter
collectors, and permission to depend on the host's `libsystemd`. Other choices below
are the proposed v1 specification for review.

The agent collects system logs and host metrics from standalone Linux machines and
sends them to EZGH Observability. Installation requires one agent executable, a
configuration file, a credential file and a systemd unit. No separate node exporter,
Collector, Prometheus server or database service is required.

## Scope and approach

Target systemd-based Linux on amd64 and arm64. Use the native journal API through
`coreos/go-systemd/v22/sdjournal`; ship architecture-specific builds compatible with
the oldest supported distribution's libc. This is one executable with system
library dependencies, not a fully static executable.

Use a small Go application with upstream node-exporter collectors and OTLP protobuf
types. Keep upstream collector initialization and its process-global flags behind
one adapter, initialize it once, and pin the dependency. A custom Collector
distribution would supply more processors but expand the configuration and runtime
surface. Reimplementing node-exporter's collectors would create unnecessary
compatibility and maintenance work.

V1 includes journald, explicitly configured text files, host metrics, durable
delivery and agent health. Application OTLP reception, traces, container discovery,
remote configuration, automatic updates, fleet management, new backend routes and
new console pages are outside this design. The existing Kubernetes agent remains
an independent product. The implementation lives in this separate `ezgh-obs-agent-linux` repository.
The original proposal also lives beside the EZGH backend designs.

## Process and package boundaries

```text
libsystemd journal reader ----+
                             +--> log mapping --> durable log queue ----+
explicit text file readers ---+                                         |
                                                                       +--> OTLP HTTPS
embedded node collectors --> metric mapping --> durable metric queue ---+    EZGH ingest
agent self metrics ----------+
```

| Package | Responsibility |
| --- | --- |
| `cmd/ezgh-obs-agent-linux` | CLI and exit codes |
| `internal/app` | Startup, supervision and shutdown |
| `internal/config` | Strict YAML parsing and validation |
| `internal/identity` | Persistent host identity and resource attributes |
| `internal/source/journal` | Native journal reads, cursor recovery and filtering |
| `internal/source/file` | File discovery, framing, rotation and offsets |
| `internal/source/node` | Private Prometheus registry and upstream collector adapter |
| `internal/transform` | Log mapping, metric conversion and size enforcement |
| `internal/spool` | Durable batches, source checkpoints, quotas and acknowledgments |
| `internal/export` | OTLP/HTTP, TLS, credentials and retry classification |
| `internal/diagnostics` | Local status and self metrics |

Sources deliver records and their checkpoint to the spool through a bounded
interface. Only a durable commit permits a source to advance its committed
checkpoint. Exporters read committed batches; network calls never hold a storage
transaction. One process exclusively locks the state directory.

## Host identity and routing

One installation targets one project and one environment. The ingest key selects
the project; the agent overwrites `ezgh.metadata.environment` with the configured
environment, validated as `[a-z0-9][a-z0-9._-]{0,62}`.

Use a configured `host.id`, or generate a random UUID on first start and persist it
with an atomic write and directory sync. Do not send raw machine-id. Preserve state
through upgrades; do not bake generated state into machine images. Cloned state
must be removed before first boot or replaced with an explicitly unique ID.

Attach `host.id`, `host.name` and `os.type=linux`. Hostname is descriptive and can
change; the ID is stable. Host metrics use `service.name=node-exporter` and
`service.instance.id=<host.id>`. Self metrics use
`service.name=ezgh-obs-agent-linux` and the same instance ID. Both set
`ezgh.metadata.application=linux`.

Journal logs use `_SYSTEMD_UNIT` as `service.name`, falling back to
`SYSLOG_IDENTIFIER`, then `linux`; files use a configured service name. Logs use
the host ID as their service instance and `ezgh.metadata.application=linux`.
Retain unit names verbatim, including instance names.

Keep host metadata on resources. Metric identity relies on the stable instance,
not arbitrary host attributes promoted into labels. Preserve all upstream metric
labels; reject configuration that collides with reserved routing/identity keys.
No per-process IDs or journal fields become metric labels.

## Host metrics

Collect every 30 seconds, with randomized initial delay and no overlapping
collections. Start with the pinned upstream collectors `cpu`, `meminfo`,
`loadavg`, `diskstats`, `filesystem`, `netdev`, `netstat`, `stat`, `time`, `uname`,
`vmstat` and `os`. Preserve their emitted `node_*` names, HELP text and labels.
Reuse upstream filesystem/device exclusion defaults. Additional collectors,
including systemd unit metrics and textfile metrics, are outside the v1 allowlist.
Unknown collector names are configuration errors.

Use a private registry. Gather metric families directly, without an HTTP listener
or parsing Prometheus text. Convert gauges to OTLP gauges and counters to
cumulative monotonic sums; do not calculate rates. Leave start timestamps unset
when upstream does not supply a meaningful start time rather than inventing the
agent's startup as the host counter's origin. Counter decreases remain visible
for normal PromQL reset handling. Preserve sample timestamps when present,
otherwise use the collection timestamp.

Retain original names and leave OTLP unit empty when Prometheus provides none;
unit suffixes in names still carry the original meaning. Do not infer units that
could cause the backend translator to rename a metric. Known supplied units must
pass the same name round-trip tests before inclusion. V1 supports gauge and counter families. Implementation verification found that
the pinned upstream netstat and vmstat collectors emit untyped families. Those
are explicitly represented as raw OTLP gauges, preserving names and values
without inferring monotonicity or counter suffixes. Other unexpected family
types are counted as unsupported and omitted. This is an explicit mapping
decision made during implementation, not a claim that upstream supplied types.

Preserve `node_scrape_collector_success` and duration metrics. A collector failure
must not discard successful collectors. Upstream Collect calls are not generally
context-cancellable: never claim a timeout kills a blocked syscall or spawn
replacement gathers indefinitely. With one gather still running, skip subsequent
ticks, expose its age through independent diagnostics and mark metrics degraded
after 60 seconds. Use upstream filesystem timeout controls where available.

Limit each converted scrape to 50,000 points and 16 MiB of serialized payload.
An over-limit scrape is rejected as a whole and counted; do not send an arbitrary
subset. Split admitted scrapes into export batches. These limits are configurable
and must be load-tested; they are not measured capacity claims.

## System logs

Read the local journal through libsystemd, including system and kernel messages.
Default first-start position is the current tail to avoid unexpected historical
ingestion. An explicit `start_at: beginning` reads retained history. A saved cursor
always takes precedence. A dedicated journal thread owns its native handle.

Map `MESSAGE` to a string body and journal realtime to event time; record observation
time separately. Map priorities 0 through 7 to FATAL, FATAL, FATAL, ERROR, WARN,
INFO, INFO and DEBUG with matching OTLP severity numbers. Preserve a bounded
allowlist of unit, identifier, PID, UID, GID, transport and boot ID attributes.
Exclude `ezgh-obs-agent-linux.service` by default; diagnostics use self metrics and
local journal output rather than forwarding the agent's own logs.

Checkpoint both opaque cursor and realtime timestamp. On resume, validate the
cursor and begin after it. If it was vacuumed, seek by the saved timestamp and
accept possible duplicates at the boundary; report a source-gap event/counter
without claiming to know how many records were lost. Preserve the boot ID so
reboots are diagnosable. Restrict journal data reads with a native data threshold
and test oversized fields; truncate bodies to 64 KiB on a valid UTF-8 boundary and
mark truncation. Replace invalid UTF-8, count replacements and cap each retained
attribute value at 4 KiB and the full serialized record at 128 KiB. Remove optional
attributes first if needed to fit the record bound.

Optional files are newline-delimited text with explicit globs, exclusions and a
service name; no recursive scan of `/var/log`. Maximum 128 open files by default,
with an actionable diagnostic when matches exceed it. Do not enable journal and
its syslog mirror together by default. Multiline parsing and structured JSON/logfmt
parsing are deferred; file severity remains unset.

Track device, inode, a generation fingerprint and offset. Drain a renamed open
file while discovering its replacement; save checkpoints for both. Detect
truncation and restart that generation at offset zero. Copytruncate has an
unavoidable loss window and is documented as such. Bound line framing memory;
truncate an oversized line while consuming through its delimiter, then checkpoint
the whole line. Emit an unterminated trailing line after a 5-second idle timeout,
documenting that later writes become another record. Default new-file position is
tail; `beginning` is opt-in. Reject symlinks and non-regular files, and verify the
opened descriptor's identity to avoid path substitution during discovery.

## Durable delivery

Use an embedded transactional store (bbolt) for batches and source checkpoints.
It is local durable state, not a cache or external service. Atomically insert each
bounded log batch and update its source checkpoints with synchronous commits.
Never disable fsync in production. Keep log and metric databases separate so one
signal cannot consume the other's budget; a single writer serializes each store.

Default total state budget is 512 MiB: at most 256 MiB for the log database,
128 MiB for the metric database, and 128 MiB reserved for bookkeeping and storage
maintenance. Admission checks account for physical database size, page reuse and
growth overhead, not only live payload bytes. Keep a 64 MiB free-filesystem reserve.
Do not compact by temporarily doubling disk use. Reuse freed pages; oversized or
corrupt stores fail closed with repair guidance, never auto-delete.

When log storage is full, stop reading and leave source checkpoints unchanged.
When metric storage is full, skip new collections and count missed intervals.
Source retention can expire while paused, causing loss. There is no unlimited
outage guarantee. At measured incoming rate R bytes/second and available queue
capacity Q bytes, Q/R estimates the remaining buffering duration before overhead.

Send FIFO per signal with one request in flight per signal. Delete batches only
after successful acknowledgment or an explicitly accounted permanent rejection.
Retries reuse original timestamps and payloads. A crash after upstream acceptance
but before local deletion can duplicate records; delivery is at least once only
within source retention, queue capacity and backend acceptance constraints.

Batch at most 1 MiB of uncompressed serialized OTLP per request and flush at least
every second while data is available. Split by actual encoded size, not record
count. Independently check compressed size before sending. The backend baseline
allows 5 MiB wire / 64 MiB decoded requests and 64 KiB log bodies.

| Outcome | Action |
| --- | --- |
| Complete OTLP success | Durably acknowledge batch |
| OTLP partial success | Acknowledge batch, count rejected items, surface bounded diagnostic; do not replay accepted items |
| Network error, HTTP 429/502/503/504 | Retry with exponential backoff and full jitter, 1 second to 60 seconds; honor longer valid Retry-After |
| HTTP 401/403 | Retain queue, pause exports until credential reload; expose authentication failure |
| HTTP 404 or unsupported protocol | Retain queue, pause exports until configuration correction |
| HTTP 413 | Split batch durably; reject and count an indivisible record if still refused |
| Other permanent protocol rejection | Count dropped items and acknowledge offending batch; never log body or credentials |

Use a 15-second request timeout and a bounded response reader. Follow the OTLP
specification's classification for malformed responses. Never forward credentials
across HTTP redirects; reject redirects. TLS verification is mandatory outside
tests, with an optional CA file. Proxy support uses the standard Go HTTP transport.

Store a binding of destination, environment and explicit installation target name
alongside queues. Refuse to reuse nonempty queues if this binding changes. A key
rotation keeps the same target name; moving to another project requires a new
state directory or draining the old one. The opaque key cannot tell the agent
whether two credentials select the same project; document this operator obligation.

## Configuration and commands

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

Unknown keys, invalid environment, empty target, invalid durations and overlapping
file source definitions fail validation. At least one signal must be enabled.
Accept credentials only from a restricted regular file, never a CLI token argument.
An owner-created file readable by the service user (0600 or root/service-group 0640)
is the normal installation path. Do not print credentials, payloads or full
configuration in errors.

Commands are `run --config`, `validate --config`, `version` and `doctor --config`.
Validate checks syntax without sending telemetry. Doctor checks permissions,
libsystemd, writable state and local status; it does not emit probe telemetry or
modify source checkpoints. Run publishes a bounded status snapshot over a
permission-restricted Unix socket for doctor, independent of collection. No TCP
listener is enabled in v1.

SIGHUP reloads only the credential file and resumes a paused authentication export;
other configuration changes require restart. SIGTERM stops readers, commits their
pending accepted work, permits up to 20 seconds for delivery, closes storage and
exits; any undelivered committed batches survive. Systemd StopTimeout is 30 seconds.

## Packaging and operations

Provide tar archives, checksums, SBOM and deb/rpm packages for amd64 and arm64, with
an explicitly tested libc/libsystemd compatibility baseline. Initial test targets
are Ubuntu 22.04/24.04 and Debian 12 plus a supported Enterprise Linux 9 derivative.
Pin Go and upstream dependency versions when implementing; do not label artifacts
as universally compatible Linux binaries.

Packages create a dedicated user and grant journal access through the distribution's
journal group/ACL convention. Additional file access is explicitly granted by the
operator. Avoid running as root. The unit uses NoNewPrivileges, ProtectSystem=strict,
ProtectHome and a writable StateDirectory. Do not hide `/proc`, `/sys` or the host
mount table needed by collectors. Verify filesystem metrics through the hardened
unit, because mount namespace hardening can change what collectors observe.

The agent starts only after a credential and valid configuration are supplied.
Upgrades preserve identity, queues and checkpoints. Uninstall preserves state;
explicit package purge is destructive. Version queue formats and refuse an
unsupported future format on downgrade. No self-updater or remote command channel.

Self metrics include collected/exported/rejected items, retries, queue bytes and
oldest age, last successful export, truncated records, cursor gaps, skipped scrapes,
collection age, process memory and CPU. Use bounded reason and signal labels.
Export them every 60 seconds through the metric queue without reading them from
the journal; retain independent local status even when exports fail.

## Compatibility and validation

The comparison baseline is obs-stack
`bce4bba06a0a87eea6982b3eb20b0d2788c84a50`, the revision pinned by platform when
this design was written. Its `internal/otlp/validate.go` enforces the log body cap;
`internal/query/prom/names.go` uses the OTLP translator and derives job/instance
from resource identity. This baseline is not a claim that production was probed.
Recheck pins and feature configuration before releasing the agent.

No platform API response changes are proposed. The consumers are the existing
ingest routes, PromQL and log search, rather than a new console decoder. Release
qualification must use disposable backend dependencies and never production keys.

1. Compare fixture metric families and live Linux gathers against the exact pinned
   upstream node-exporter build. Verify names, labels, types and counter values.
2. Round-trip through the pinned EZGH ingest/query implementation. Assert familiar
   names such as `node_cpu_seconds_total` and `node_memory_MemAvailable_bytes`,
   CPU rate/reset behavior, and separation of two hosts sharing a hostname.
3. Test journal priority mapping, reboot, vacuumed cursor, first-start policy,
   oversized/binary fields, permissions and exclusion of the agent's own service.
4. Test file rename/replacement, inode reuse, truncation, restart, partial lines,
   symlink replacement and long lines with bounded memory.
5. Kill the process at each checkpoint/enqueue/ack boundary. Verify no checkpoint
   outruns durable data, accepted batches replay after ambiguous acknowledgment,
   and partial success does not replay accepted items.
6. Exercise TLS failure, retry responses, Retry-After forms, credential rotation,
   malformed responses, request splitting, full disk and corrupt storage.
7. Run real systemd package installation, restart, upgrade, downgrade refusal and
   uninstall tests on each supported architecture/distribution, with no exporter
   or journalctl subprocess. Use Linux VMs where the development host is macOS.
8. Benchmark 1,000 log records/second at 1 KiB each plus the default collectors for
   30 minutes, including a capped-queue outage. Report CPU, RSS, disk overhead,
   queue growth and recovery throughput. Initial acceptance target is under
   150 MiB steady RSS; CPU results and any exceptions require review. Run Go race,
   vet and relevant parser/protocol fuzz tests before release.

The remaining implementation work is agent code, packaging and qualification.
Backend or console work is only added if compatibility tests expose a concrete
gap; such a gap requires its own reviewed change. This design does not authorize
a production deployment.

## References

- EZGH service-template metrics ingest and PromQL designs, checked against the implementation baseline above.
- [Upstream node collectors](https://github.com/prometheus/node_exporter/blob/master/collector/collector.go).
- [Native journal Go bindings](https://github.com/coreos/go-systemd).
- [OTLP protocol and retry rules](https://opentelemetry.io/docs/specs/otlp/).
