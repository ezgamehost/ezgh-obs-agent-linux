// Package app owns daemon resources, supervision and bounded shutdown.
package app

import (
	"context"
	"errors"
	"fmt"
	"github.com/ezgamehost/ezgh-obs-agent-linux/internal/config"
	"github.com/ezgamehost/ezgh-obs-agent-linux/internal/credential"
	"github.com/ezgamehost/ezgh-obs-agent-linux/internal/diagnostics"
	exporter "github.com/ezgamehost/ezgh-obs-agent-linux/internal/export"
	"github.com/ezgamehost/ezgh-obs-agent-linux/internal/identity"
	files "github.com/ezgamehost/ezgh-obs-agent-linux/internal/source/file"
	"github.com/ezgamehost/ezgh-obs-agent-linux/internal/source/journal"
	"github.com/ezgamehost/ezgh-obs-agent-linux/internal/spool"
	"github.com/ezgamehost/ezgh-obs-agent-linux/internal/telemetry"
	"golang.org/x/sys/unix"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

// keyPoll is how often the agent asks for its key. Metadata decides when that reaches the service.
var keyPoll = 30 * time.Second

// drainWindow bounds delivery of committed batches at shutdown.
var drainWindow = 20 * time.Second

func privateDir(dir string) error {
	if e := os.MkdirAll(dir, 0700); e != nil {
		return e
	}
	st, e := os.Lstat(dir)
	if e != nil || !st.IsDir() || st.Mode().Perm()&0077 != 0 {
		return errors.New("state directory must be a private directory (0700)")
	}
	return nil
}
func lockState(dir string) (*os.File, error) {
	if e := privateDir(dir); e != nil {
		return nil, e
	}
	p := filepath.Join(dir, "agent.lock")
	if st, e := os.Lstat(p); e == nil && !st.Mode().IsRegular() {
		return nil, errors.New("invalid lock file")
	}
	f, e := os.OpenFile(p, os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		return nil, e
	}
	if e = unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); e != nil {
		f.Close()
		return nil, errors.New("another agent holds this state directory")
	}
	return f, nil
}

// credentialState is what the status socket and doctor show for the last answer about the key.
func credentialState(e error) string {
	switch {
	case e == nil:
		return "loaded"
	case errors.Is(e, credential.ErrNoBot):
		return "no_bot"
	case errors.Is(e, credential.ErrNoProject):
		return "idle:project_not_prefixed"
	}
	return "waiting:metadata"
}

// removeStale deletes what other instances left under stateDir: a clone of this disk inherits
// their queues, which this identity must never send. Only real directories named exactly like an
// instance ID go. A failure only leaves disk in use, so it's reported and the agent carries on.
func removeStale(stateDir, id string, state *diagnostics.State) {
	entries, e := os.ReadDir(stateDir)
	if e != nil {
		state.Set("stale_state", "unreadable")
		return
	}
	for _, d := range entries {
		if d.IsDir() && d.Name() != id && credential.IsInstance(d.Name()) {
			if e = os.RemoveAll(filepath.Join(stateDir, d.Name())); e != nil {
				state.Set("stale_state", "remove_failed")
			}
		}
	}
}

// awaitKey holds a metadata-mode agent idle, writing no data, until the instance has a key. It
// never gives up on the metadata service: a boot can beat it, and a Bot can be attached at any
// time. The exception is an instance that has already sent, whose state directory exists: it
// carries on with no key, so sources keep running. Told there's no Bot (noBot), they drop as on a
// live detach, because waiting would send the whole gap once a Bot came back. Otherwise it's an
// outage, and they spool for the next key.
func awaitKey(ctx context.Context, m *credential.Metadata, stateDir string, state *diagnostics.State) (id, project, key string, noBot bool, e error) {
	for {
		if id, project, e = m.Instance(ctx); e == nil {
			state.Set("instance", id)
			state.Set("project", project)
			removeStale(stateDir, id, state)
			if key, e = m.Key(ctx); e == nil {
				state.Set("credential", credentialState(nil))
				return id, project, key, false, nil
			}
			if _, err := os.Stat(filepath.Join(stateDir, id)); err == nil {
				state.Set("credential", credentialState(e))
				return id, project, "", errors.Is(e, credential.ErrNoBot), nil
			}
		}
		state.Set("credential", credentialState(e))
		if !sleep(ctx, keyPoll) {
			return "", "", "", false, ctx.Err()
		}
	}
}
func Run(ctx context.Context, c config.Config, reload <-chan os.Signal) error {
	return run(ctx, c, reload, &credential.Metadata{})
}
func run(ctx context.Context, c config.Config, reload <-chan os.Signal, m *credential.Metadata) error {
	telemetry.UseAttributesFile(c.ResourceAttributesFile)
	telemetry.UsePointAttributes(c.Metrics.PointAttributes)
	metadata := c.Credential == config.CredentialMetadata
	if metadata && !m.Available() {
		return errors.New("credential: metadata works only on an EZGH Compute instance")
	}
	lock, e := lockState(c.StateDir)
	if e != nil {
		return e
	}
	defer lock.Close()
	state := diagnostics.New()
	closeStatus, e := state.Serve(filepath.Join(c.StateDir, "status.sock"))
	if e != nil {
		return e
	}
	defer closeStatus()
	var id, project, key string
	var noBot bool
	dir := c.StateDir
	if metadata {
		if id, project, key, noBot, e = awaitKey(ctx, m, c.StateDir, state); e != nil {
			return nil // stopped while idle
		}
		// A clone of this disk has a new ID, so it starts clean rather than sending this state.
		dir = filepath.Join(c.StateDir, id)
		if e = privateDir(dir); e != nil {
			return e
		}
	} else {
		if id, e = identity.Load(c.StateDir, c.HostID); e != nil {
			return e
		}
		if key, e = c.ReadKey(); e != nil {
			return e
		}
		project = c.Project
	}
	hostname, e := os.Hostname()
	if e != nil {
		return e
	}
	client, e := exporter.New(c.Endpoint, c.CAFile, key)
	if e != nil {
		return e
	}
	client.SetProject(project)
	client.DropForbidden = metadata
	client.SetNoBot(noBot)
	wake := make(chan struct{}, 1)
	if metadata {
		client.Unauthorized = func(refused string) {
			m.Invalidate(refused)
			select {
			case wake <- struct{}{}:
			default:
			}
		}
	}
	binding := c.Target + "|" + c.Endpoint + "|" + c.Environment + "|" + id
	if project != "" {
		// Only then: a changed binding refuses to start, and ingest-key queues predate this.
		binding += "|" + project
	}
	lq, e := spool.Open(filepath.Join(dir, "logs.db"), binding, c.Storage.LogBytes, c.Storage.ReserveBytes)
	if e != nil {
		return e
	}
	defer lq.Close()
	mq, e := spool.Open(filepath.Join(dir, "metrics.db"), binding, c.Storage.MetricBytes, c.Storage.ReserveBytes)
	if e != nil {
		return e
	}
	defer mq.Close()
	sourceCtx, stopSources := context.WithCancel(ctx)
	defer stopSources()
	deliveryCtx, stopDelivery := context.WithCancel(context.Background())
	defer stopDelivery()
	var sources, deliveries sync.WaitGroup
	errs := make(chan error, 8)
	var generation atomic.Uint64
	start := func(wg *sync.WaitGroup, fn func() error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if e := fn(); e != nil && !errors.Is(e, context.Canceled) {
				select {
				case errs <- e:
				default:
				}
			}
		}()
	}
	for signal, q := range map[string]*spool.Queue{"logs": lq, "metrics": mq} {
		start(&deliveries, func() error { return deliver(deliveryCtx, signal, q, client, state, &generation) })
	}
	sink := func(ctx context.Context, rows []*telemetry.LogItem, cp map[string][]byte) error {
		payloads, e := telemetry.EncodeLogs(rows, telemetry.MaxBatch)
		if e != nil {
			return e
		}
		for {
			e = lq.Enqueue(payloads, cp)
			if e == nil {
				state.Add("logs_collected", uint64(len(rows)))
				for _, row := range rows {
					for _, a := range row.Record.Attributes {
						if a.Value.GetStringValue() == "true" {
							switch a.Key {
							case "ezgh.log.truncated":
								state.Add("logs_truncated", 1)
							case "ezgh.log.invalid_utf8":
								state.Add("logs_invalid_utf8", 1)
							}
						}
					}
				}
				state.Set("log_storage", "ready")
				return nil
			}
			if !errors.Is(e, spool.ErrFull) {
				return e
			}
			state.Set("log_storage", "full")
			if !sleep(ctx, time.Second) {
				return ctx.Err()
			}
		}
	}
	if c.Logs.Journal.Enabled {
		start(&sources, func() error {
			return journal.Run(sourceCtx, c.Logs.Journal.StartAt, c.Logs.Journal.ExcludeUnits, id, hostname, c.Environment, lq.Checkpoint, sink, func() { state.Add("journal_cursor_gaps", 1) })
		})
	}
	if len(c.Logs.Files) > 0 {
		start(&sources, func() error {
			s := files.New(c.Logs.Files, c.Logs.MaxOpenFiles, id, hostname, c.Environment, lq.Checkpoint, sink)
			defer s.Close()
			for {
				if e := s.Poll(sourceCtx, time.Now()); e != nil {
					return e
				}
				if !sleep(sourceCtx, 100*time.Millisecond) {
					return sourceCtx.Err()
				}
			}
		})
	}
	if c.Metrics.Enabled {
		start(&sources, func() error { return collectMetrics(sourceCtx, c, id, hostname, mq, state) })
	}
	if metadata {
		reload = nil // no credential file to read again
		start(&sources, func() error {
			current, detached := key, noBot
			t := time.NewTicker(keyPoll)
			defer t.Stop()
			var woken time.Time
			for {
				select {
				case <-sourceCtx.Done():
					return nil
				case <-t.C:
				case <-wake:
					// Once per poll: ingest refusing every key must not mint keys in a loop.
					if time.Since(woken) < keyPoll {
						continue
					}
					woken = time.Now()
				}
				k, e := m.Key(sourceCtx)
				if sourceCtx.Err() != nil {
					return nil // says nothing about the key, and the drain below still needs it
				}
				bot := errors.Is(e, credential.ErrNoBot)
				if k != current || bot != detached {
					client.SetNoBot(bot)
					client.SetCredential(k)
					generation.Add(1)
					current, detached = k, bot
				}
				state.Set("credential", credentialState(e))
			}
		})
	}
	start(&sources, func() error {
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			select {
			case <-sourceCtx.Done():
				return nil
			case <-reload:
				k, e := c.ReadKey()
				if e != nil {
					state.Set("credential", "reload_failed")
					continue
				}
				client.SetCredential(k)
				generation.Add(1)
				state.Set("credential", "loaded")
			case <-t.C:
				for signal, q := range map[string]*spool.Queue{"logs": lq, "metrics": mq} {
					v, e := q.Stats()
					if e != nil {
						return e
					}
					state.Gauge(signal+"_queue_bytes", float64(v.Bytes))
					state.Gauge(signal+"_queue_batches", float64(v.Batches))
					age := 0.0
					if !v.Oldest.IsZero() {
						age = time.Since(v.Oldest).Seconds()
					}
					state.Gauge(signal+"_queue_oldest_seconds", age)
				}
			}
		}
	})
	var result error
	select {
	case <-ctx.Done():
	case result = <-errs:
	}
	stopSources()
	sources.Wait()
	// Readers stop first. Committed work gets a bounded drain window; uncommitted reads replay.
	deadline := time.NewTimer(drainWindow)
	defer deadline.Stop()
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
draining:
	for {
		a, e1 := lq.Stats()
		b, e2 := mq.Stats()
		if e1 != nil || e2 != nil || a.Batches+b.Batches == 0 {
			break
		}
		select {
		case <-deadline.C:
			break draining
		case e := <-errs:
			if result == nil {
				result = e
			}
			break draining
		case <-tick.C:
		}
	}
	stopDelivery()
	deliveries.Wait()
	if result != nil {
		return fmt.Errorf("agent stopped: %w", result)
	}
	return nil
}
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
