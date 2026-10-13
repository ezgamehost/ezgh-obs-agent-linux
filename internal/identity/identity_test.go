package identity

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPersistentIdentity(t *testing.T) {
	d := t.TempDir()
	a, e := Load(d, "")
	if e != nil {
		t.Fatal(e)
	}
	b, e := Load(d, "")
	if e != nil || a != b || len(a) != 36 {
		t.Fatalf("%s %s %v", a, b, e)
	}
	os.WriteFile(filepath.Join(d, "host-id"), []byte("broken"), 0600)
	if _, e = Load(d, ""); e == nil {
		t.Fatal("silently replaced corrupt identity")
	}
}
