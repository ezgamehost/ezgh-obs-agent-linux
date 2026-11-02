//go:build linux

package node

import (
	"os"
	"path/filepath"
	"testing"
)

// One test: the collectors take their flags once per process.
func TestEmbeddedCollectors(t *testing.T) {
	dir := t.TempDir()
	if e := os.WriteFile(filepath.Join(dir, "lb.prom"), []byte("lb_node_info{lbr=\"lbr_a\",role=\"active\"} 1\n"), 0o644); e != nil {
		t.Fatal(e)
	}
	c, e := New([]string{"cpu", "meminfo", "loadavg"}, dir)
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
	for _, n := range []string{"node_cpu_seconds_total", "node_memory_MemAvailable_bytes", "node_load1", "node_scrape_collector_success", "lb_node_info"} {
		if !seen[n] {
			t.Errorf("missing %s", n)
		}
	}
}
