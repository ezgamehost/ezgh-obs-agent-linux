package app

import (
	"context"
	"errors"
	"github.com/ezgamehost/ezgh-obs-agent-linux/internal/diagnostics"
	exporter "github.com/ezgamehost/ezgh-obs-agent-linux/internal/export"
	"github.com/ezgamehost/ezgh-obs-agent-linux/internal/spool"
	"github.com/ezgamehost/ezgh-obs-agent-linux/internal/telemetry"
	"math/rand/v2"
	"sync/atomic"
	"time"
)

func deliver(ctx context.Context, signal string, q *spool.Queue, client *exporter.Client, state *diagnostics.State, generation *atomic.Uint64) error {
	delay := time.Second
	for ctx.Err() == nil {
		b, e := q.Peek()
		if errors.Is(e, spool.ErrEmpty) {
			state.Set(signal, "idle")
			if !sleep(ctx, 100*time.Millisecond) {
				break
			}
			continue
		}
		if e != nil {
			return e
		}
		gen := generation.Load()
		o := client.Send(ctx, signal, b.Payload)
		if ctx.Err() != nil {
			break
		}
		count := uint64(telemetry.Count(signal, b.Payload))
		switch o.Kind {
		case exporter.Success:
			if e = q.Ack(b.ID); e != nil {
				return e
			}
			rejected := min(count, uint64(o.Rejected))
			state.Add(signal+"_rejected", rejected)
			state.Add(signal+"_exported", count-rejected)
			state.Gauge(signal+"_last_success_unixtime", float64(time.Now().Unix()))
			state.Set(signal, "ready")
			delay = time.Second
		case exporter.Drop:
			if e = q.Ack(b.ID); e != nil {
				return e
			}
			state.Add(signal+"_rejected", count)
			state.Set(signal, "rejected:"+o.Reason)
			delay = time.Second
		case exporter.Split:
			parts, e := telemetry.Split(signal, b.Payload)
			if e != nil || len(parts) != 2 {
				if e = q.Ack(b.ID); e != nil {
					return e
				}
				state.Add(signal+"_rejected", count)
			} else if e = q.Replace(b.ID, parts); errors.Is(e, spool.ErrFull) {
				state.Set(signal, "split_storage_full")
				sleep(ctx, time.Second)
			} else if e != nil {
				return e
			}
		case exporter.Pause:
			state.Set(signal, "paused:"+o.Reason)
			for ctx.Err() == nil && (o.Reason != "credential" || generation.Load() == gen) {
				if !sleep(ctx, time.Second) {
					break
				}
			}
		case exporter.Retry:
			state.Add(signal+"_retries", 1)
			state.Set(signal, "retrying:"+o.Reason)
			wait := time.Duration(rand.Int64N(int64(delay)))
			if o.After > wait {
				wait = o.After
			}
			if !sleep(ctx, wait) {
				return nil
			}
			delay = min(delay*2, 60*time.Second)
		}
	}
	return nil
}
