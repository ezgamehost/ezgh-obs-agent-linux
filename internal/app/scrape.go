package app

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"

	"github.com/ezgamehost/ezgh-obs-agent-linux/internal/config"
)

const scrapeTimeout = 5 * time.Second

// scrapeClient goes nowhere but the loopback address, so proxies in the environment don't apply.
var scrapeClient = &http.Client{Timeout: scrapeTimeout, Transport: &http.Transport{DisableKeepAlives: true},
	CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }} // a redirect could leave loopback

type scraped struct {
	service  string
	families []*dto.MetricFamily
	err      error
}

func scrapeAll(ctx context.Context, c config.Config) []scraped {
	out := make([]scraped, 0, len(c.Metrics.Scrape))
	for _, s := range c.Metrics.Scrape {
		f, e := scrape(ctx, s.URL, c.Metrics.MaxBytes)
		out = append(out, scraped{s.Service, f, e})
	}
	return out
}

func scrape(ctx context.Context, url string, maxBytes int) ([]*dto.MetricFamily, error) {
	req, e := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if e != nil {
		return nil, e
	}
	resp, e := scrapeClient.Do(req)
	if e != nil {
		return nil, e
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("scrape: %s", resp.Status)
	}
	// Read one byte past the limit: cut at a line's end, a truncated body still parses, as a scrape
	// that quietly lost its last families.
	body, e := io.ReadAll(io.LimitReader(resp.Body, int64(maxBytes)+1))
	if e != nil {
		return nil, e
	}
	if len(body) > maxBytes {
		return nil, fmt.Errorf("scrape: more than %d bytes", maxBytes)
	}
	parser := expfmt.NewTextParser(model.UTF8Validation)
	families, e := parser.TextToMetricFamilies(bytes.NewReader(body))
	if e != nil {
		return nil, e
	}
	out := make([]*dto.MetricFamily, 0, len(families))
	for _, f := range families {
		out = append(out, f)
	}
	return out, nil
}
