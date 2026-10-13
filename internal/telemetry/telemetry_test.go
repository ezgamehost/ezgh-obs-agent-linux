package telemetry

import (
	dto "github.com/prometheus/client_model/go"
	logs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	metrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
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
