// Package app owns daemon resources, supervision and bounded shutdown.
package app

import (
	"context"
	"errors"
	"fmt"
	"github.com/ezgamehost/ezgh-obs-agent-linux/internal/config"
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

func lockState(dir string) (*os.File, error) {
	if e := os.MkdirAll(dir, 0700); e != nil {
		return nil, e
	}
	st, e := os.Lstat(dir)
	if e != nil || !st.IsDir() || st.Mode().Perm()&0077 != 0 {
		return nil, errors.New("state directory must be a private directory (0700)")
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
func Run(ctx context.Context, c config.Config, reload <-chan os.Signal) error {
	lock, e := lockState(c.StateDir)
	if e != nil {
		return e
	}
	defer lock.Close()
	id, e := identity.Load(c.StateDir, c.HostID)
	if e != nil {
		return e
	}
	hostname, e := os.Hostname()
	if e != nil {
		return e
	}
	key, e := config.ReadCredential(c.CredentialFile)
	if e != nil {
		return e
	}
	client, e := exporter.New(c.Endpoint, c.CAFile, key)
	if e != nil {
		return e
	}
	binding := c.Target + "|" + c.Endpoint + "|" + c.Environment + "|" + id
	lq, e := spool.Open(filepath.Join(c.StateDir, "logs.db"), binding, c.Storage.LogBytes, c.Storage.ReserveBytes)
	if e != nil {
		return e
	}
	defer lq.Close()
	mq, e := spool.Open(filepath.Join(c.StateDir, "metrics.db"), binding, c.Storage.MetricBytes, c.Storage.ReserveBytes)
	if e != nil {
		return e
	}
	defer mq.Close()
	state := diagnostics.New()
	closeStatus, e := state.Serve(filepath.Join(c.StateDir, "status.sock"))
	if e != nil {
		return e
	}
	defer closeStatus()
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
	start(&sources, func() error {
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			select {
			case <-sourceCtx.Done():
				return nil
			case <-reload:
				k, e := config.ReadCredential(c.CredentialFile)
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
	deadline := time.NewTimer(20 * time.Second)
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
