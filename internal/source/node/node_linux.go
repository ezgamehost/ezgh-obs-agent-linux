//go:build linux

// Package node isolates upstream collector initialization and its global flags.
package node

import (
	"errors"
	"github.com/alecthomas/kingpin/v2"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/node_exporter/collector"
	"io"
	"log/slog"
	"slices"
	"sync"
)

var initMu sync.Mutex
var initialized bool

type Collector struct{ registry *prometheus.Registry }

// New runs the named collectors, and node_exporter's textfile collector on textfiles if it's set.
func New(names []string, textfiles string) (*Collector, error) {
	initMu.Lock()
	defer initMu.Unlock()
	if initialized {
		return nil, errors.New("node collectors may only be initialized once")
	}
	args := make([]string, 0, len(names))
	for _, n := range names {
		args = append(args, "--collector."+n)
	}
	if textfiles != "" {
		names = append(slices.Clone(names), "textfile")
		args = append(args, "--collector.textfile", "--collector.textfile.directory="+textfiles)
	}
	if _, e := kingpin.CommandLine.Parse(args); e != nil {
		return nil, e
	}
	c, e := collector.NewNodeCollector(slog.New(slog.NewTextHandler(io.Discard, nil)), names...)
	if e != nil {
		return nil, e
	}
	r := prometheus.NewRegistry()
	if e = r.Register(c); e != nil {
		return nil, e
	}
	initialized = true
	return &Collector{r}, nil
}
func (c *Collector) Gather() ([]*dto.MetricFamily, error) { return c.registry.Gather() }
