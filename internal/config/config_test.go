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

const apiKey = "ezgh_0123456789abcdefghijABCDEFGHIJ0123456789"

var ingestKey = "ezgh_ik_" + strings.Repeat("a", 43)

const project = "prj_g7h8i9j0k1l2"

func TestCredentialModes(t *testing.T) {
	head := "version: 1\ntarget: prod\nenvironment: production\n"
	c, err := Parse(strings.NewReader(head + "credential: metadata\n"))
	if err != nil || c.Credential != CredentialMetadata || c.CredentialFile != "" {
		t.Fatalf("%+v %v", c, err)
	}
	if _, err := Parse(strings.NewReader(base + "project: " + project + "\n")); err != nil {
		t.Fatal(err)
	}
	for name, doc := range map[string]string{
		"metadata with credential_file": base + "credential: metadata\n",
		"metadata with project":         head + "credential: metadata\nproject: " + project + "\n",
		"metadata with host_id":         head + "credential: metadata\nhost_id: fixed\n",
		"unknown credential mode":       head + "credential: file\n",
		"no credential at all":          head,
		"project not a prj_ ID":         base + "project: my-project\n",
		"project a UUID":                base + "project: 2b5f2a64-1f6b-4d7e-9b53-3a1f8a8c5b11\n",
	} {
		if _, err := Parse(strings.NewReader(doc)); err == nil {
			t.Errorf("accepted %s", name)
		}
	}
}

func TestReadCredentialShapes(t *testing.T) {
	p := filepath.Join(t.TempDir(), "key")
	for _, key := range []string{ingestKey, apiKey} {
		os.WriteFile(p, []byte(key+"\n"), 0600)
		if got, err := ReadCredential(p); err != nil || got != key {
			t.Fatalf("%q %v", got, err)
		}
	}
	for _, bad := range []string{"ezgh_at_" + strings.Repeat("a", 37), "ezgh_" + strings.Repeat("a", 39), "ezgh_" + strings.Repeat("a", 41), "ezgh_ik_" + strings.Repeat("a", 42), "ezgh_sat_" + strings.Repeat("a", 43)} {
		os.WriteFile(p, []byte(bad), 0600)
		if _, err := ReadCredential(p); err == nil {
			t.Errorf("accepted %q", bad)
		}
	}
}

func TestKeyMustMatchProject(t *testing.T) {
	p := filepath.Join(t.TempDir(), "key")
	for _, tc := range []struct {
		key, project string
		ok           bool
	}{{ingestKey, "", true}, {ingestKey, project, false}, {apiKey, project, true}, {apiKey, "", false}} {
		os.WriteFile(p, []byte(tc.key), 0600)
		_, err := Config{CredentialFile: p, Project: tc.project}.ReadKey()
		if (err == nil) != tc.ok {
			t.Errorf("%.8s with project %q: %v", tc.key, tc.project, err)
		}
	}
}

func TestScrapeAndAttributes(t *testing.T) {
	base := "version: 1\ntarget: t\nenvironment: e\ncredential: metadata\n"
	good := base + "resource_attributes_file: /run/a.json\nmetrics:\n  scrape:\n    - url: http://127.0.0.1:8405/metrics\n      service: haproxy\n"
	if c, e := Parse(strings.NewReader(good)); e != nil || len(c.Metrics.Scrape) != 1 || c.ResourceAttributesFile != "/run/a.json" {
		t.Fatalf("%+v %v", c, e)
	}
	for name, cfg := range map[string]string{
		"remote":   "metrics:\n  scrape:\n    - url: http://example.com/metrics\n      service: x\n",
		"https":    "metrics:\n  scrape:\n    - url: https://127.0.0.1/metrics\n      service: x\n",
		"no name":  "metrics:\n  scrape:\n    - url: http://127.0.0.1/metrics\n",
		"relative": "resource_attributes_file: a.json\n",
	} {
		if _, e := Parse(strings.NewReader(base + cfg)); e == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}
