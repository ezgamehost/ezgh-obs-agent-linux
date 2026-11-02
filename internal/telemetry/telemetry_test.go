package telemetry

import (
	dto "github.com/prometheus/client_model/go"
	logs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	metrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/protobuf/proto"
	"os"
	"path/filepath"
	"strings"
	"syscall"
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

// PromQL makes series labels from point attributes, not the resource, so the file's labels go on
// every point; a point's own label of the same name wins.
func TestPointsCarryTheAttributesFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "attributes.json")
	if e := os.WriteFile(path, []byte(`{"lbr":"lbr_a","role":"active","proxy":"forged"}`), 0o644); e != nil {
		t.Fatal(e)
	}
	UseAttributesFile(path)
	defer UseAttributesFile("")
	f := &dto.MetricFamily{Name: proto.String("haproxy_up"), Type: dto.MetricType_GAUGE.Enum(), Metric: []*dto.Metric{{
		Label: []*dto.LabelPair{{Name: proto.String("proxy"), Value: proto.String("lsn_a")}}, Gauge: &dto.Gauge{Value: proto.Float64(1)}}}}
	b, e := EncodeMetrics([]*dto.MetricFamily{f}, Resource("id", "host", "prod", "haproxy"), time.Unix(10, 0), 100, 1<<20)
	if e != nil {
		t.Fatal(e)
	}
	v := new(metrics.ExportMetricsServiceRequest)
	proto.Unmarshal(b[0], v)
	got := map[string]string{}
	for _, a := range v.ResourceMetrics[0].ScopeMetrics[0].Metrics[0].GetGauge().DataPoints[0].Attributes {
		got[a.Key] = a.Value.GetStringValue()
	}
	if got["lbr"] != "lbr_a" || got["role"] != "active" || got["proxy"] != "lsn_a" || len(got) != 3 {
		t.Errorf("%v", got)
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

// What's at the path can't stop the signals that read it, or wipe what it said last.
func TestAttributesFileHeldToItsShape(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "attributes.json")
	write := func(body string) {
		t.Helper()
		os.Remove(path)
		if e := os.WriteFile(path, []byte(body), 0o644); e != nil {
			t.Fatal(e)
		}
		extra.Lock()
		extra.next = time.Time{}
		extra.Unlock()
	}
	get := func() (string, int) {
		lbr, size := "", 0
		for _, a := range Resource("i", "h", "e", "s").Attributes {
			if a.Key == "lbr" {
				lbr, size = a.Value.GetStringValue(), len(a.Value.GetStringValue())
			}
		}
		return lbr, size
	}
	UseAttributesFile(path)
	defer UseAttributesFile("")
	write(`{"lbr":"lbr_a"}`)
	if got, _ := get(); got != "lbr_a" {
		t.Fatal(got)
	}
	write(`null`)
	if got, _ := get(); got != "lbr_a" {
		t.Errorf("null wiped the labels: %q", got)
	}
	os.Remove(path)
	if e := syscall.Mkfifo(path, 0o644); e != nil {
		t.Fatal(e)
	}
	extra.Lock()
	extra.next = time.Time{}
	extra.Unlock()
	done := make(chan struct{})
	go func() { get(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("a FIFO blocked the reader")
	}
	write(`{"lbr":"` + strings.Repeat("x", 5000) + `"}`)
	if _, size := get(); size != 4096 {
		t.Errorf("a value of %d bytes", size)
	}
}

func TestResourceCarriesTheAttributesFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "attributes.json")
	if e := os.WriteFile(path, []byte(`{"lbr":"lbr_a","role":"active","host.id":"forged"}`), 0o644); e != nil {
		t.Fatal(e)
	}
	UseAttributesFile(path)
	defer UseAttributesFile("")
	get := func() map[string]string {
		m := map[string]string{}
		for _, a := range Resource("ins_a", "host", "production", "haproxy").Attributes {
			m[a.Key] = a.Value.GetStringValue()
		}
		return m
	}
	got := get()
	if got["lbr"] != "lbr_a" || got["role"] != "active" {
		t.Errorf("%v", got)
	}
	if got["host.id"] != "ins_a" {
		t.Errorf("a file changed host.id: %v", got)
	}
	if e := os.WriteFile(path, []byte(`not json`), 0o644); e != nil {
		t.Fatal(e)
	}
	extra.Lock()
	extra.next = time.Time{}
	extra.Unlock()
	if got := get(); got["role"] != "active" {
		t.Errorf("a bad rewrite lost what it had: %v", got)
	}
	if e := os.WriteFile(path, []byte(`{"role":"passive"}`), 0o644); e != nil {
		t.Fatal(e)
	}
	extra.Lock()
	extra.next = time.Time{}
	extra.Unlock()
	if got := get(); got["role"] != "passive" {
		t.Errorf("a rewrite didn't land: %v", got)
	}
}
