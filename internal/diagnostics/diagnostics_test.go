package diagnostics

import (
	"context"
	"path/filepath"
	"testing"
)

func TestLocalStatus(t *testing.T) {
	s := New()
	s.Add("logs_collected", 3)
	s.Set("logs", "paused:credential")
	p := filepath.Join(t.TempDir(), "status.sock")
	close, e := s.Serve(p)
	if e != nil {
		t.Fatal(e)
	}
	defer close()
	got, e := Read(context.Background(), p)
	if e != nil {
		t.Fatal(e)
	}
	if got.Counters["logs_collected"] != 3 || got.States["logs"] != "paused:credential" {
		t.Fatal(got)
	}
}
