// Package config validates operator configuration without exposing its contents.
package config

import (
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"
)

type Duration struct{ time.Duration }

func (d *Duration) UnmarshalYAML(n *yaml.Node) error {
	v, e := time.ParseDuration(n.Value)
	d.Duration = v
	return e
}

type File struct {
	Paths   []string `yaml:"paths"`
	Exclude []string `yaml:"exclude"`
	Service string   `yaml:"service"`
	StartAt string   `yaml:"start_at"`
}
type Config struct {
	Version        int    `yaml:"version"`
	Target         string `yaml:"target"`
	Endpoint       string `yaml:"endpoint"`
	Environment    string `yaml:"environment"`
	CredentialFile string `yaml:"credential_file"`
	StateDir       string `yaml:"state_dir"`
	CAFile         string `yaml:"ca_file"`
	HostID         string `yaml:"host_id"`
	Metrics        struct {
		Enabled    bool     `yaml:"enabled"`
		Interval   Duration `yaml:"interval"`
		Collectors []string `yaml:"collectors"`
		MaxPoints  int      `yaml:"max_points"`
		MaxBytes   int      `yaml:"max_bytes"`
	} `yaml:"metrics"`
	Logs struct {
		Journal struct {
			Enabled      bool     `yaml:"enabled"`
			StartAt      string   `yaml:"start_at"`
			ExcludeUnits []string `yaml:"exclude_units"`
		} `yaml:"journal"`
		Files        []File `yaml:"files"`
		MaxOpenFiles int    `yaml:"max_open_files"`
	} `yaml:"logs"`
	Storage struct {
		LogBytes     int64  `yaml:"log_bytes"`
		MetricBytes  int64  `yaml:"metric_bytes"`
		ReserveBytes uint64 `yaml:"reserve_bytes"`
	} `yaml:"storage"`
}

var DefaultCollectors = []string{"cpu", "meminfo", "loadavg", "diskstats", "filesystem", "netdev", "netstat", "stat", "time", "uname", "vmstat", "os"}
var environmentRE = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,62}$`)

func Parse(r io.Reader) (Config, error) {
	var c Config
	c.Endpoint = "https://obs-ingest.ezghcloud.com"
	c.StateDir = "/var/lib/ezgh-obs-agent-linux"
	c.Metrics.Enabled = true
	c.Metrics.Interval.Duration = 30 * time.Second
	c.Metrics.Collectors = append([]string(nil), DefaultCollectors...)
	c.Metrics.MaxPoints = 50000
	c.Metrics.MaxBytes = 16 << 20
	c.Logs.Journal.Enabled = true
	c.Logs.Journal.StartAt = "end"
	c.Logs.Journal.ExcludeUnits = []string{"ezgh-obs-agent-linux.service"}
	c.Logs.MaxOpenFiles = 128
	c.Storage.LogBytes = 256 << 20
	c.Storage.MetricBytes = 128 << 20
	c.Storage.ReserveBytes = 64 << 20
	d := yaml.NewDecoder(io.LimitReader(r, 1<<20))
	d.KnownFields(true)
	if e := d.Decode(&c); e != nil {
		return c, errors.New("invalid configuration YAML (check fields, types and duplicates)")
	}
	var extra any
	if e := d.Decode(&extra); e != io.EOF {
		return c, errors.New("configuration must contain exactly one document")
	}
	if c.Version != 1 {
		return c, errors.New("version must be 1")
	}
	if !environmentRE.MatchString(c.Environment) || !environmentRE.MatchString(c.Target) {
		return c, errors.New("environment and target must be lowercase identifiers (1–63 characters)")
	}
	u, e := url.Parse(c.Endpoint)
	if e != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return c, errors.New("endpoint must be an HTTPS origin without credentials, path, query or fragment")
	}
	c.Endpoint = strings.TrimSuffix(c.Endpoint, "/")
	if !filepath.IsAbs(c.StateDir) || !filepath.IsAbs(c.CredentialFile) {
		return c, errors.New("state_dir and credential_file must be absolute paths")
	}
	if c.HostID != "" && (len(c.HostID) > 128 || strings.TrimSpace(c.HostID) != c.HostID || strings.ContainsAny(c.HostID, "\r\n\x00")) {
		return c, errors.New("invalid host_id")
	}
	if c.Metrics.Interval.Duration < time.Second || c.Metrics.Interval.Duration > 24*time.Hour {
		return c, errors.New("metrics interval must be between 1s and 24h")
	}
	if c.Metrics.MaxPoints < 1 || c.Metrics.MaxPoints > 1000000 || c.Metrics.MaxBytes < 1<<20 || c.Metrics.MaxBytes > 64<<20 {
		return c, errors.New("metric limits out of range")
	}
	allowed := map[string]bool{}
	for _, v := range DefaultCollectors {
		allowed[v] = true
	}
	seen := map[string]bool{}
	for _, v := range c.Metrics.Collectors {
		if !allowed[v] || seen[v] {
			return c, errors.New("unknown or duplicate collector")
		}
		seen[v] = true
	}
	if c.Metrics.Enabled && len(seen) == 0 {
		return c, errors.New("enabled metrics require collectors")
	}
	if c.Logs.Journal.StartAt != "end" && c.Logs.Journal.StartAt != "beginning" {
		return c, errors.New("journal start_at must be end or beginning")
	}
	if c.Logs.MaxOpenFiles < 1 || c.Logs.MaxOpenFiles > 4096 {
		return c, errors.New("max_open_files must be 1–4096")
	}
	paths := map[string]bool{}
	for i := range c.Logs.Files {
		f := &c.Logs.Files[i]
		if f.StartAt == "" {
			f.StartAt = "end"
		}
		if f.Service == "" || len(f.Service) > 128 || len(f.Paths) == 0 || (f.StartAt != "end" && f.StartAt != "beginning") {
			return c, errors.New("file source needs service, paths and valid start_at")
		}
		for _, p := range append(append([]string(nil), f.Paths...), f.Exclude...) {
			if !filepath.IsAbs(p) || strings.Contains(p, "**") {
				return c, errors.New("file patterns must be absolute non-recursive globs")
			}
			if _, e := filepath.Match(p, ""); e != nil {
				return c, errors.New("invalid file glob")
			}
		}
		for _, p := range f.Paths {
			if paths[p] {
				return c, errors.New("overlapping file sources")
			}
			paths[p] = true
		}
	}
	if !c.Metrics.Enabled && !c.Logs.Journal.Enabled && len(c.Logs.Files) == 0 {
		return c, errors.New("at least one signal must be enabled")
	}
	if c.Storage.LogBytes < 8<<20 || c.Storage.MetricBytes < 8<<20 || c.Storage.LogBytes > 1<<40 || c.Storage.MetricBytes > 1<<40 {
		return c, errors.New("queue budgets must be between 8 MiB and 1 TiB")
	}
	return c, nil
}
func Load(path string) (Config, error) {
	f, e := os.Open(path)
	if e != nil {
		return Config{}, errors.New("cannot open configuration")
	}
	defer f.Close()
	return Parse(f)
}
func ReadCredential(path string) (string, error) {
	st, e := os.Lstat(path)
	if e != nil || !st.Mode().IsRegular() || st.Mode().Perm()&0137 != 0 {
		return "", errors.New("credential must be a regular restricted file (0600 or 0640)")
	}
	f, e := os.Open(path)
	if e != nil {
		return "", errors.New("cannot open credential")
	}
	defer f.Close()
	opened, e := f.Stat()
	if e != nil || !os.SameFile(st, opened) || opened.Mode().Perm()&0137 != 0 {
		return "", errors.New("credential changed while opening")
	}
	b, e := io.ReadAll(io.LimitReader(f, 256))
	if e != nil {
		return "", errors.New("cannot read credential")
	}
	v := strings.TrimSpace(string(b))
	if !regexp.MustCompile(`^ezgh_ik_[a-zA-Z0-9]{43}$`).MatchString(v) {
		return "", fmt.Errorf("invalid project ingest key format")
	}
	return v, nil
}
