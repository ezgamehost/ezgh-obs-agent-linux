// Package telemetry implements bounded OTLP mapping and size-based batching.
package telemetry

import (
	"errors"
	dto "github.com/prometheus/client_model/go"
	logservice "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	metricservice "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	common "go.opentelemetry.io/proto/otlp/common/v1"
	logs "go.opentelemetry.io/proto/otlp/logs/v1"
	metrics "go.opentelemetry.io/proto/otlp/metrics/v1"
	resource "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/protobuf/proto"
	"sort"
	"strings"
	"time"
	"unicode/utf8"
)

const MaxBatch = 1 << 20

func Attribute(k, v string) *common.KeyValue {
	return &common.KeyValue{Key: k, Value: &common.AnyValue{Value: &common.AnyValue_StringValue{StringValue: v}}}
}
func Resource(id, hostname, environment, service string) *resource.Resource {
	return &resource.Resource{Attributes: []*common.KeyValue{Attribute("host.id", id), Attribute("host.name", hostname), Attribute("os.type", "linux"), Attribute("service.name", service), Attribute("service.instance.id", id), Attribute("ezgh.metadata.application", "linux"), Attribute("ezgh.metadata.environment", environment)}}
}
func Bounded(s string, n int) (string, bool) {
	v := strings.ToValidUTF8(s, "�")
	changed := v != s
	if len(v) > n {
		v = v[:n]
		for !utf8.ValidString(v) {
			v = v[:len(v)-1]
		}
		changed = true
	}
	return v, changed
}
func Log(body string, at time.Time, severity logs.SeverityNumber, attrs map[string]string) *logs.LogRecord {
	body, truncated := Bounded(body, 64<<10)
	r := &logs.LogRecord{TimeUnixNano: uint64(at.UnixNano()), ObservedTimeUnixNano: uint64(time.Now().UnixNano()), Body: &common.AnyValue{Value: &common.AnyValue_StringValue{StringValue: body}}, SeverityNumber: severity}
	keys := make([]string, 0, len(attrs))
	for k := range attrs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		v, _ := Bounded(attrs[k], 4096)
		r.Attributes = append(r.Attributes, Attribute(k, v))
		if proto.Size(r) > 120<<10 {
			r.Attributes = r.Attributes[:len(r.Attributes)-1]
			break
		}
	}
	if truncated {
		r.Attributes = append(r.Attributes, Attribute("ezgh.log.truncated", "true"))
	}
	return r
}

type LogItem struct {
	Resource *resource.Resource
	Record   *logs.LogRecord
}

func EncodeLogs(rows []*LogItem, limit int) ([][]byte, error) {
	var out [][]byte
	req := new(logservice.ExportLogsServiceRequest)
	size := 0
	flush := func() error {
		if len(req.ResourceLogs) == 0 {
			return nil
		}
		p, e := proto.Marshal(req)
		if e != nil {
			return e
		}
		if len(p) > limit {
			return errors.New("log batch too large")
		}
		out = append(out, p)
		req = new(logservice.ExportLogsServiceRequest)
		size = 0
		return nil
	}
	for _, row := range rows {
		v := &logs.ResourceLogs{Resource: row.Resource, ScopeLogs: []*logs.ScopeLogs{{Scope: &common.InstrumentationScope{Name: "ezgh-obs-agent-linux"}, LogRecords: []*logs.LogRecord{row.Record}}}}
		n := proto.Size(v) + 10
		if n > limit {
			return nil, errors.New("log record too large")
		}
		if size+n > limit {
			if e := flush(); e != nil {
				return nil, e
			}
		}
		req.ResourceLogs = append(req.ResourceLogs, v)
		size += n
	}
	if e := flush(); e != nil {
		return nil, e
	}
	return out, nil
}
func EncodeMetrics(families []*dto.MetricFamily, res *resource.Resource, at time.Time, maxPoints, maxBytes int) ([][]byte, error) {
	var points []*metrics.Metric
	for _, f := range families {
		for _, m := range f.Metric {
			t := uint64(at.UnixNano())
			if m.TimestampMs != nil {
				t = uint64(m.GetTimestampMs()) * 1000000
			}
			p := &metrics.NumberDataPoint{TimeUnixNano: t}
			for _, a := range m.Label {
				p.Attributes = append(p.Attributes, Attribute(a.GetName(), a.GetValue()))
			}
			v := &metrics.Metric{Name: f.GetName(), Description: f.GetHelp()}
			switch f.GetType() {
			case dto.MetricType_GAUGE:
				p.Value = &metrics.NumberDataPoint_AsDouble{AsDouble: m.GetGauge().GetValue()}
				v.Data = &metrics.Metric_Gauge{Gauge: &metrics.Gauge{DataPoints: []*metrics.NumberDataPoint{p}}}
			case dto.MetricType_COUNTER:
				p.Value = &metrics.NumberDataPoint_AsDouble{AsDouble: m.GetCounter().GetValue()}
				v.Data = &metrics.Metric_Sum{Sum: &metrics.Sum{DataPoints: []*metrics.NumberDataPoint{p}, IsMonotonic: true, AggregationTemporality: metrics.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE}}
			default:
				continue
			}
			points = append(points, v)
			if len(points) > maxPoints {
				return nil, errors.New("scrape exceeds point limit")
			}
		}
	}
	var out [][]byte
	req := metricRequest(res, nil)
	size := proto.Size(req)
	total := 0
	flush := func() error {
		if len(req.ResourceMetrics[0].ScopeMetrics[0].Metrics) == 0 {
			return nil
		}
		b, e := proto.Marshal(req)
		if e != nil {
			return e
		}
		total += len(b)
		if len(b) > MaxBatch || total > maxBytes {
			return errors.New("scrape exceeds byte limit")
		}
		out = append(out, b)
		req = metricRequest(res, nil)
		size = proto.Size(req)
		return nil
	}
	for _, p := range points {
		n := proto.Size(p) + 16
		if n+proto.Size(metricRequest(res, nil)) > MaxBatch {
			return nil, errors.New("metric point too large")
		}
		if size+n > MaxBatch-32 {
			if e := flush(); e != nil {
				return nil, e
			}
		}
		req.ResourceMetrics[0].ScopeMetrics[0].Metrics = append(req.ResourceMetrics[0].ScopeMetrics[0].Metrics, p)
		size += n
	}
	if e := flush(); e != nil {
		return nil, e
	}
	return out, nil
}
func metricRequest(r *resource.Resource, m []*metrics.Metric) *metricservice.ExportMetricsServiceRequest {
	return &metricservice.ExportMetricsServiceRequest{ResourceMetrics: []*metrics.ResourceMetrics{{Resource: r, ScopeMetrics: []*metrics.ScopeMetrics{{Scope: &common.InstrumentationScope{Name: "ezgh-obs-agent-linux"}, Metrics: m}}}}}
}

// Split creates two smaller valid requests. A single item cannot be split.
func Split(signal string, b []byte) ([][]byte, error) {
	if signal == "logs" {
		r := new(logservice.ExportLogsServiceRequest)
		if e := proto.Unmarshal(b, r); e != nil {
			return nil, e
		}
		var rows []*LogItem
		for _, rl := range r.ResourceLogs {
			for _, sl := range rl.ScopeLogs {
				for _, l := range sl.LogRecords {
					rows = append(rows, &LogItem{rl.Resource, l})
				}
			}
		}
		if len(rows) < 2 {
			return nil, errors.New("indivisible record")
		}
		a, e := EncodeLogs(rows[:len(rows)/2], MaxBatch)
		if e != nil {
			return nil, e
		}
		c, e := EncodeLogs(rows[len(rows)/2:], MaxBatch)
		if e != nil {
			return nil, e
		}
		return append(a, c...), nil
	}
	r := new(metricservice.ExportMetricsServiceRequest)
	if e := proto.Unmarshal(b, r); e != nil {
		return nil, e
	}
	var rows []*metrics.ResourceMetrics
	for _, rm := range r.ResourceMetrics {
		for _, sm := range rm.ScopeMetrics {
			for _, m := range sm.Metrics {
				rows = append(rows, &metrics.ResourceMetrics{Resource: rm.Resource, ScopeMetrics: []*metrics.ScopeMetrics{{Scope: sm.Scope, Metrics: []*metrics.Metric{m}}}})
			}
		}
	}
	if len(rows) < 2 {
		return nil, errors.New("indivisible point")
	}
	a, e := proto.Marshal(&metricservice.ExportMetricsServiceRequest{ResourceMetrics: rows[:len(rows)/2]})
	if e != nil {
		return nil, e
	}
	c, e := proto.Marshal(&metricservice.ExportMetricsServiceRequest{ResourceMetrics: rows[len(rows)/2:]})
	return [][]byte{a, c}, e
}
func Count(signal string, b []byte) int64 {
	var n int64
	if signal == "logs" {
		r := new(logservice.ExportLogsServiceRequest)
		if proto.Unmarshal(b, r) != nil {
			return 0
		}
		for _, rl := range r.ResourceLogs {
			for _, sl := range rl.ScopeLogs {
				n += int64(len(sl.LogRecords))
			}
		}
	} else {
		r := new(metricservice.ExportMetricsServiceRequest)
		if proto.Unmarshal(b, r) != nil {
			return 0
		}
		for _, rm := range r.ResourceMetrics {
			for _, sm := range rm.ScopeMetrics {
				for _, m := range sm.Metrics {
					n += int64(len(m.GetGauge().GetDataPoints()) + len(m.GetSum().GetDataPoints()))
				}
			}
		}
	}
	return n
}
