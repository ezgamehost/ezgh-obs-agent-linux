package app

import (
	"context"
	"errors"
	"github.com/ezgamehost/ezgh-obs-agent-linux/internal/config"
	"github.com/ezgamehost/ezgh-obs-agent-linux/internal/diagnostics"
	"github.com/ezgamehost/ezgh-obs-agent-linux/internal/source/node"
	"github.com/ezgamehost/ezgh-obs-agent-linux/internal/spool"
	"github.com/ezgamehost/ezgh-obs-agent-linux/internal/telemetry"
	dto "github.com/prometheus/client_model/go"
	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/proto"
	"math/rand/v2"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"
)

func collectMetrics(ctx context.Context, c config.Config, id, host string, q *spool.Queue, s *diagnostics.State) error {
	collector, e := node.New(c.Metrics.Collectors)
	if e != nil {
		return e
	}
	type result struct {
		f  []*dto.MetricFamily
		e  error
		at time.Time
	}
	done := make(chan result, 1)
	t := time.NewTimer(time.Duration(rand.Int64N(int64(c.Metrics.Interval.Duration))))
	defer t.Stop()
	health := time.NewTicker(time.Second)
	defer health.Stop()
	self := time.NewTicker(60 * time.Second)
	defer self.Stop()
	var started time.Time
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			t.Reset(c.Metrics.Interval.Duration)
			if !started.IsZero() || !q.CanAccept() {
				s.Add("metrics_skipped_scrapes", 1)
				continue
			}
			started = time.Now()
			go func(at time.Time) { f, e := collector.Gather(); done <- result{f, e, at} }(started)
		case r := <-done:
			started = time.Time{}
			s.Gauge("metrics_collection_age_seconds", 0)
			if r.e != nil {
				s.Add("metrics_collection_errors", 1)
			}
			for _, f := range r.f {
				if f.GetType() != dto.MetricType_GAUGE && f.GetType() != dto.MetricType_COUNTER && f.GetType() != dto.MetricType_UNTYPED {
					s.Add("metrics_unsupported_families", 1)
				}
			}
			payloads, e := telemetry.EncodeMetrics(r.f, telemetry.Resource(id, host, c.Environment, "node-exporter"), r.at, c.Metrics.MaxPoints, c.Metrics.MaxBytes)
			if e != nil {
				s.Add("metrics_rejected_scrapes", 1)
				continue
			}
			if e = q.Enqueue(payloads, nil); errors.Is(e, spool.ErrFull) {
				s.Add("metrics_skipped_scrapes", 1)
			} else if e != nil {
				return e
			} else {
				for _, p := range payloads {
					s.Add("metrics_collected", uint64(telemetry.Count("metrics", p)))
				}
				s.Set("metrics_collection", "ready")
			}
		case <-health.C:
			if !started.IsZero() {
				age := time.Since(started)
				s.Gauge("metrics_collection_age_seconds", age.Seconds())
				if age > 60*time.Second {
					s.Set("metrics_collection", "blocked")
				}
			}
		case <-self.C:
			var mem runtime.MemStats
			runtime.ReadMemStats(&mem)
			s.Gauge("go_heap_bytes", float64(mem.Alloc))
			if data, e := os.ReadFile("/proc/self/statm"); e == nil {
				fields := strings.Fields(string(data))
				if len(fields) > 1 {
					if pages, e := strconv.ParseUint(fields[1], 10, 64); e == nil {
						s.Gauge("process_resident_memory_bytes", float64(pages)*float64(os.Getpagesize()))
					}
				}
			}
			var ru unix.Rusage
			if unix.Getrusage(unix.RUSAGE_SELF, &ru) == nil {
				s.Gauge("process_cpu_seconds", float64(ru.Utime.Sec+ru.Stime.Sec)+float64(ru.Utime.Usec+ru.Stime.Usec)/1e6)
			}
			snap := s.Snapshot()
			var f []*dto.MetricFamily
			for name, value := range snap.Counters {
				f = append(f, &dto.MetricFamily{Name: proto.String("ezgh_agent_" + name + "_total"), Type: dto.MetricType_COUNTER.Enum(), Metric: []*dto.Metric{{Counter: &dto.Counter{Value: proto.Float64(float64(value))}}}})
			}
			for name, value := range snap.Gauges {
				f = append(f, &dto.MetricFamily{Name: proto.String("ezgh_agent_" + name), Type: dto.MetricType_GAUGE.Enum(), Metric: []*dto.Metric{{Gauge: &dto.Gauge{Value: proto.Float64(value)}}}})
			}
			payloads, e := telemetry.EncodeMetrics(f, telemetry.Resource(id, host, c.Environment, "ezgh-obs-agent-linux"), time.Now(), 50000, 16<<20)
			if e != nil {
				return e
			}
			if e = q.Enqueue(payloads, nil); e != nil && !errors.Is(e, spool.ErrFull) {
				return e
			}
		}
	}
}
