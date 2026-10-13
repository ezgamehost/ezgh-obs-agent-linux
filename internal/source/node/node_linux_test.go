//go:build linux

package node

import (
	"testing"
)

func TestEmbeddedCollectors(t *testing.T) {
	c, e := New([]string{"cpu", "meminfo", "loadavg"})
	if e != nil {
		t.Fatal(e)
	}
	f, e := c.Gather()
	if e != nil {
		t.Fatal(e)
	}
	seen := map[string]bool{}
	for _, m := range f {
		seen[m.GetName()] = true
	}
	for _, n := range []string{"node_cpu_seconds_total", "node_memory_MemAvailable_bytes", "node_load1", "node_scrape_collector_success"} {
		if !seen[n] {
			t.Errorf("missing %s", n)
		}
	}
}
