// Package credential supplies the key of the Bot attached to this compute instance.
package credential

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ezgamehost/ezgh-obs-agent-linux/internal/config"
)

const (
	defaultMetadataEndpoint = "http://169.254.169.254"
	defaultProductNamePath  = "/sys/class/dmi/id/product_name"
	instanceProductName     = "EZGH Compute"
	instanceRefreshMargin   = 5 * time.Minute
	metadataTokenTTL        = 6 * time.Hour
	metadataRetryAfter      = 5 * time.Second
)

// NoBotRetry is how long the agent waits before asking again when the instance has no Bot, so
// attaching one later is enough.
const NoBotRetry = 3 * time.Minute

// instanceRE keeps the ID safe to use as a directory name.
var instanceRE = regexp.MustCompile(`^ins_[a-z0-9]{12}$`)

// ErrNoBot means the instance has no Bot attached.
var ErrNoBot = errors.New("this instance has no Bot attached")

// ErrNoProject means the instance document doesn't name the project by its prj_ ID, as an older
// definition may not: ingest can't be told the project, so the agent stays idle.
var ErrNoProject = errors.New("the instance metadata gives no prj_ project ID")

// Metadata fetches the temporary `ezgh_` key of the Bot attached to this compute instance, from
// the metadata service. The key is used until 5 minutes before it expires. Copied from
// ezgh-fleets-agent's internal/creds.
type Metadata struct {
	Endpoint        string // default http://169.254.169.254
	ProductNamePath string // default /sys/class/dmi/id/product_name
	HTTP            *http.Client
	Now             func() time.Time

	mu          sync.Mutex
	token       string
	tokenExp    time.Time
	key         string
	expiresAt   time.Time
	nextTry     time.Time
	lastErr     error
	retry       time.Duration // the metadata service's Retry-After (503, 429) on the last answer
	id, project string
}

// Available reports whether this machine is an EZGH compute instance. No network is touched.
func (p *Metadata) Available() bool {
	path := p.ProductNamePath
	if path == "" {
		path = defaultProductNamePath
	}
	b, err := os.ReadFile(path)
	return err == nil && strings.TrimSpace(string(b)) == instanceProductName
}

func (p *Metadata) now() time.Time {
	if p.Now != nil {
		return p.Now()
	}
	return time.Now()
}

// Key returns the current key. ErrNoBot is the service saying there is no Bot; any other error
// means the key expired while the service wasn't answering.
func (p *Metadata) Key(ctx context.Context) (key string, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.now()
	if p.key != "" && now.Add(instanceRefreshMargin).Before(p.expiresAt) {
		return p.key, nil
	}
	if !now.Before(p.nextTry) {
		if err = p.fetch(ctx); err == nil {
			return p.key, nil
		}
		p.lastErr = err
		p.nextTry = now.Add(max(metadataRetryAfter, p.retry))
		if errors.Is(err, ErrNoBot) {
			p.key = ""
			p.nextTry = now.Add(NoBotRetry)
		}
	}
	// A failed fetch keeps the old key until it actually expires.
	if p.key != "" && now.Before(p.expiresAt) {
		return p.key, nil
	}
	if err == nil {
		// Backing off: report the failure that started it, without asking again.
		if err = p.lastErr; err == nil {
			err = errors.New("instance credentials expired and the metadata service is not answering")
		}
	}
	return "", err
}

// Instance is the instance's ID and project prj_ ID from GET /ezgh/v1/instance, which takes no
// session token. Kept once read: neither can change.
func (p *Metadata) Instance(ctx context.Context) (id, project string, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.id != "" {
		return p.id, p.project, nil
	}
	status, body, err := p.do(ctx, http.MethodGet, "/ezgh/v1/instance", nil)
	if err != nil {
		return "", "", err
	}
	if status != http.StatusOK {
		return "", "", fmt.Errorf("instance metadata answered %d", status)
	}
	var doc struct {
		ID      string `json:"id"`
		Project string `json:"project"`
	}
	if err := json.Unmarshal(body, &doc); err != nil || !instanceRE.MatchString(doc.ID) {
		return "", "", errors.New("instance metadata sent an unreadable answer")
	}
	if !config.IsProject(doc.Project) {
		return "", "", ErrNoProject
	}
	p.id, p.project = doc.ID, doc.Project
	return p.id, p.project, nil
}

func (p *Metadata) fetch(ctx context.Context) error {
	for attempt := 0; ; attempt++ {
		if p.token == "" || !p.now().Before(p.tokenExp) {
			if err := p.newToken(ctx); err != nil {
				return err
			}
		}
		status, body, err := p.do(ctx, http.MethodGet, "/ezgh/v1/credentials", nil)
		if err != nil {
			return err
		}
		switch {
		case status == http.StatusOK:
			var c struct {
				APIKey    string    `json:"apiKey"`
				ExpiresAt time.Time `json:"expiresAt"`
			}
			if err := json.Unmarshal(body, &c); err != nil || c.APIKey == "" || c.ExpiresAt.IsZero() {
				return errors.New("instance metadata sent an unreadable answer")
			}
			p.key, p.expiresAt = c.APIKey, c.ExpiresAt
			return nil
		case status == http.StatusUnauthorized && attempt == 0:
			p.token = "" // the token is gone (after a migration or a host restart): get a new one
		case status == http.StatusNotFound && errorCode(body) == "no_bot":
			return ErrNoBot
		default:
			return fmt.Errorf("instance metadata answered %d", status)
		}
	}
}

func errorCode(body []byte) string {
	var e struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	_ = json.Unmarshal(body, &e)
	return e.Error.Code
}

func (p *Metadata) newToken(ctx context.Context) error {
	status, body, err := p.do(ctx, http.MethodPut, "/ezgh/v1/token", http.Header{"Ezgh-Metadata-Token-Ttl-Seconds": {strconv.Itoa(int(metadataTokenTTL.Seconds()))}})
	if err != nil {
		return err
	}
	if status != http.StatusOK || len(body) == 0 {
		return fmt.Errorf("instance metadata answered %d to the token request", status)
	}
	p.token = strings.TrimSpace(string(body))
	p.tokenExp = p.now().Add(metadataTokenTTL - instanceRefreshMargin)
	return nil
}

// metadataClient is shared, so connections are reused. No proxy: the service is link-local.
var metadataClient = &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{
	DialContext:     (&net.Dialer{Timeout: time.Second}).DialContext,
	MaxIdleConns:    2,
	IdleConnTimeout: time.Minute,
}}

func (p *Metadata) do(ctx context.Context, method, path string, h http.Header) (int, []byte, error) {
	endpoint := p.Endpoint
	if endpoint == "" {
		endpoint = defaultMetadataEndpoint
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint+path, nil)
	if err != nil {
		return 0, nil, err
	}
	for k, v := range h {
		req.Header[k] = v
	}
	req.Header.Set("EZGH-Metadata", "true")
	if p.token != "" && method == http.MethodGet {
		req.Header.Set("EZGH-Metadata-Token", p.token)
	}
	c := p.HTTP
	if c == nil {
		c = metadataClient
	}
	resp, err := c.Do(req)
	if err != nil {
		return 0, nil, fmt.Errorf("instance metadata unreachable: %w", err)
	}
	defer resp.Body.Close()
	p.retry = 0
	if n, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && n > 0 {
		p.retry = time.Duration(n) * time.Second
	}
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	return resp.StatusCode, b, nil
}
