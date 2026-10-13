package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const base = "version: 1\ntarget: prod\nenvironment: production\ncredential_file: /tmp/key\n"

func TestStrictConfiguration(t *testing.T) {
	c, err := Parse(strings.NewReader(base))
	if err != nil {
		t.Fatal(err)
	}
	if c.Metrics.Interval.Duration != 30*time.Second || !c.Logs.Journal.Enabled || c.Endpoint != "https://obs-ingest.ezghcloud.com" {
		t.Fatalf("bad defaults: %+v", c)
	}
	for _, suffix := range []string{"unknown: true\n", "environment: Bad Space\n", "metrics:\n  interval: -1s\n", "endpoint: http://example.com\n", "endpoint: https://user:secret@example.com\n", "logs:\n  files:\n    - paths: [/var/log/a]\n      service: x\n      start_at: middle\n"} {
		if _, err := Parse(strings.NewReader(base + suffix)); err == nil {
			t.Errorf("accepted %q", suffix)
		}
	}
	if _, err := Parse(strings.NewReader(base + "---\nversion: 1\n")); err == nil {
		t.Fatal("accepted multiple documents")
	}
}
func TestCredentialPermissions(t *testing.T) {
	p := filepath.Join(t.TempDir(), "key")
	key := "ezgh_ik_" + strings.Repeat("a", 43)
	if err := os.WriteFile(p, []byte(key+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if got, err := ReadCredential(p); err != nil || got != key {
		t.Fatalf("%q %v", got, err)
	}
	os.Chmod(p, 0644)
	if _, err := ReadCredential(p); err == nil {
		t.Fatal("accepted world readable key")
	}
	os.Chmod(p, 0600)
	link := p + ".link"
	os.Symlink(p, link)
	if _, err := ReadCredential(link); err == nil {
		t.Fatal("accepted symlink")
	}
}
