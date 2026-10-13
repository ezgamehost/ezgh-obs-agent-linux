package telemetry

import (
	dto "github.com/prometheus/client_model/go"
	logs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	metrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/protobuf/proto"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

func TestBoundedLogAndBatches(t *testing.T) {
	r := Resource("host", "name", "production", "svc")
	rec := Log(strings.Repeat("€", 30000)+"\xff", time.Now(), 0, nil)
	if len(rec.GetBody().GetStringValue()) > 65536 || !utf8.ValidString(rec.GetBody().GetStringValue()) {
		t.Fatal("unbounded/invalid body")
	}
	var rows []*LogItem
	for range 40 {
		rows = append(rows, &LogItem{Resource: r, Record: rec})
	}
	b, e := EncodeLogs(rows, 1<<20)
	if e != nil || len(b) < 2 {
		t.Fatalf("%v %d", e, len(b))
	}
	n := 0
	for _, p := range b {
		if len(p) > 1<<20 {
			t.Fatal("oversized batch")
		}
		v := new(logs.ExportLogsServiceRequest)
		if e = proto.Unmarshal(p, v); e != nil {
			t.Fatal(e)
		}
		for _, rl := range v.ResourceLogs {
			for _, sl := range rl.ScopeLogs {
				n += len(sl.LogRecords)
			}
		}
	}
	if n != 40 {
		t.Fatal(n)
	}
}
func TestCounterConversion(t *testing.T) {
	f := &dto.MetricFamily{Name: proto.String("node_cpu_seconds_total"), Type: dto.MetricType_COUNTER.Enum(), Metric: []*dto.Metric{{Counter: &dto.Counter{Value: proto.Float64(42)}, Label: []*dto.LabelPair{{Name: proto.String("cpu"), Value: proto.String("0")}}}}}
	b, e := EncodeMetrics([]*dto.MetricFamily{f}, Resource("id", "name", "prod", "node-exporter"), time.Unix(10, 0), 50000, 16<<20)
	if e != nil {
		t.Fatal(e)
	}
	v := new(metrics.ExportMetricsServiceRequest)
	proto.Unmarshal(b[0], v)
	m := v.ResourceMetrics[0].ScopeMetrics[0].Metrics[0]
	if m.Name != "node_cpu_seconds_total" || !m.GetSum().IsMonotonic || m.GetSum().DataPoints[0].GetAsDouble() != 42 || m.GetSum().DataPoints[0].StartTimeUnixNano != 0 {
		t.Fatal(m)
	}
}

func TestMetricSplitShrinksGroupedRequest(t *testing.T) {
	r := metricRequest(Resource("id", "host", "prod", "svc"), nil)
	for range 5000 {
		r.ResourceMetrics[0].ScopeMetrics[0].Metrics = append(r.ResourceMetrics[0].ScopeMetrics[0].Metrics, &metricspb.Metric{Name: "x", Data: &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{DataPoints: []*metricspb.NumberDataPoint{{Value: &metricspb.NumberDataPoint_AsDouble{AsDouble: 1}}}}}})
	}
	b, _ := proto.Marshal(r)
	parts, e := Split("metrics", b)
	if e != nil || len(parts) != 2 {
		t.Fatalf("%v", e)
	}
	for _, p := range parts {
		if len(p) >= len(b) || len(p) > MaxBatch {
			t.Fatal("split expanded")
		}
	}
	if Count("metrics", parts[0])+Count("metrics", parts[1]) != 5000 {
		t.Fatal("lost points")
	}
}

func TestLogFlagsDistinguishReplacementFromTruncation(t *testing.T) {
	r := Log("bad\xff", time.Now(), 0, nil)
	flags := map[string]string{}
	for _, a := range r.Attributes {
		flags[a.Key] = a.Value.GetStringValue()
	}
	if flags["ezgh.log.invalid_utf8"] != "true" || flags["ezgh.log.truncated"] != "" {
		t.Fatal(flags)
	}
}

func TestUpstreamUntypedPreservesNameAndValue(t *testing.T) {
	f := &dto.MetricFamily{Name: proto.String("node_vmstat_pgpgin"), Type: dto.MetricType_UNTYPED.Enum(), Metric: []*dto.Metric{{Untyped: &dto.Untyped{Value: proto.Float64(123)}}}}
	b, e := EncodeMetrics([]*dto.MetricFamily{f}, Resource("id", "host", "prod", "node-exporter"), time.Now(), 100, 1<<20)
	if e != nil || len(b) != 1 {
		t.Fatalf("%v %d", e, len(b))
	}
	r := new(metrics.ExportMetricsServiceRequest)
	proto.Unmarshal(b[0], r)
	m := r.ResourceMetrics[0].ScopeMetrics[0].Metrics[0]
	if m.Name != "node_vmstat_pgpgin" || m.GetGauge().DataPoints[0].GetAsDouble() != 123 {
		t.Fatal(m)
	}
}
