package file

import (
	"context"
	"github.com/ezgamehost/ezgh-obs-agent-linux/internal/config"
	"github.com/ezgamehost/ezgh-obs-agent-linux/internal/telemetry"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRotationAndCheckpoint(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "app.log")
	os.WriteFile(p, []byte("first\n"), 0600)
	cp := map[string][]byte{}
	var bodies []string
	s := New([]config.File{{Paths: []string{p}, Service: "app", StartAt: "beginning"}}, 128, "host", "name", "prod", func(k string) ([]byte, error) { return cp[k], nil }, func(_ context.Context, rows []*telemetry.LogItem, c map[string][]byte) error {
		for _, r := range rows {
			bodies = append(bodies, r.Record.Body.GetStringValue())
		}
		for k, v := range c {
			cp[k] = v
		}
		return nil
	})
	defer s.Close()
	if e := s.Poll(context.Background(), time.Now()); e != nil {
		t.Fatal(e)
	}
	os.Rename(p, p+".1")
	os.WriteFile(p, []byte("second\n"), 0600)
	if e := s.Poll(context.Background(), time.Now()); e != nil {
		t.Fatal(e)
	}
	if len(bodies) != 2 || bodies[0] != "first" || bodies[1] != "second" {
		t.Fatal(bodies)
	}
	s.Close()
	s = New([]config.File{{Paths: []string{p}, Service: "app", StartAt: "beginning"}}, 128, "host", "name", "prod", func(k string) ([]byte, error) { return cp[k], nil }, func(_ context.Context, rows []*telemetry.LogItem, c map[string][]byte) error {
		if len(rows) > 0 {
			t.Error("replayed acknowledged file")
		}
		return nil
	})
	defer s.Close()
	if e := s.Poll(context.Background(), time.Now()); e != nil {
		t.Fatal(e)
	}
}
func TestLongLineAndSymlink(t *testing.T) {
	d := t.TempDir()
	p := filepath.Join(d, "log")
	os.WriteFile(p, append(make([]byte, 100000), '\n'), 0600)
	os.Symlink(p, p+"link")
	count := 0
	s := New([]config.File{{Paths: []string{d + "/*"}, Service: "x", StartAt: "beginning"}}, 128, "id", "host", "prod", func(string) ([]byte, error) { return nil, nil }, func(_ context.Context, r []*telemetry.LogItem, _ map[string][]byte) error {
		for _, v := range r {
			count++
			if len(v.Record.Body.GetStringValue()) > 65536 {
				t.Error("oversized")
			}
		}
		return nil
	})
	defer s.Close()
	s.Poll(context.Background(), time.Now())
	if count != 1 {
		t.Fatal(count)
	}
}

func TestDenseLinesBoundBatch(t *testing.T) {
	d := t.TempDir()
	p := filepath.Join(d, "log")
	os.WriteFile(p, []byte(strings.Repeat("\n", 256<<10)), 0600)
	total := 0
	s := New([]config.File{{Paths: []string{p}, Service: "x", StartAt: "beginning"}}, 128, "id", "host", "prod", func(string) ([]byte, error) { return nil, nil }, func(_ context.Context, r []*telemetry.LogItem, _ map[string][]byte) error {
		if len(r) > 256 {
			t.Fatalf("unbounded batch: %d", len(r))
		}
		total += len(r)
		return nil
	})
	defer s.Close()
	if e := s.Poll(context.Background(), time.Now()); e != nil {
		t.Fatal(e)
	}
	if total == 0 {
		t.Fatal("no progress")
	}
}
