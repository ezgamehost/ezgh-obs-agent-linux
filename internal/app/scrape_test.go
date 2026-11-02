package app

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ezgamehost/ezgh-obs-agent-linux/internal/config"
)

const haproxyMetrics = `# HELP haproxy_frontend_current_sessions Number of current sessions.
# TYPE haproxy_frontend_current_sessions gauge
haproxy_frontend_current_sessions{proxy="lsn_a"} 3
# HELP haproxy_backend_connection_errors_total Total connection errors.
# TYPE haproxy_backend_connection_errors_total counter
haproxy_backend_connection_errors_total{proxy="tgp_a"} 7
`

func TestScrape(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte(haproxyMetrics)) }))
	defer s.Close()
	f, e := scrape(context.Background(), s.URL, 1<<20)
	if e != nil || len(f) != 2 {
		t.Fatalf("%v %v", f, e)
	}
}

func TestScrapeFollowsNoRedirect(t *testing.T) {
	away := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte(haproxyMetrics)) }))
	defer away.Close()
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, away.URL, http.StatusFound) }))
	defer s.Close()
	if _, e := scrape(context.Background(), s.URL, 1<<20); e == nil {
		t.Error("a redirect was followed")
	}
}

func TestScrapeAllCarriesItsError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { http.Error(w, "no", http.StatusServiceUnavailable) }))
	defer s.Close()
	var c config.Config
	c.Metrics.MaxBytes = 1 << 20
	c.Metrics.Scrape = []config.Scrape{{URL: s.URL, Service: "haproxy"}}
	got := scrapeAll(context.Background(), c)
	if len(got) != 1 || got[0].err == nil || got[0].service != "haproxy" {
		t.Errorf("%+v", got)
	}
}

func TestScrapeRefusesGarbage(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("{not metrics")) }))
	defer s.Close()
	if _, e := scrape(context.Background(), s.URL, 1<<20); e == nil {
		t.Error("parsed it")
	}
}

func TestScrapeOverTheLimitFails(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte(haproxyMetrics)) }))
	defer s.Close()
	if _, e := scrape(context.Background(), s.URL, 100); e == nil {
		t.Error("a truncated scrape passed")
	}
}
