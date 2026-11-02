package app

import (
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
var scrapeClient = &http.Client{Timeout: scrapeTimeout, Transport: &http.Transport{DisableKeepAlives: true}}

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
	parser := expfmt.NewTextParser(model.UTF8Validation)
	families, e := parser.TextToMetricFamilies(io.LimitReader(resp.Body, int64(maxBytes)))
	if e != nil {
		return nil, e
	}
	out := make([]*dto.MetricFamily, 0, len(families))
	for _, f := range families {
		out = append(out, f)
	}
	return out, nil
}
