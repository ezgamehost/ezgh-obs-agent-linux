// Package export implements the OTLP/HTTP transport without leaking remote diagnostics.
package export

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	logs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	metrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"google.golang.org/protobuf/proto"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Kind int

const (
	Success Kind = iota
	Retry
	Pause
	Split
	Drop
)

type Outcome struct {
	Kind     Kind
	Rejected int64
	After    time.Duration
	Reason   string
}
type Client struct {
	endpoint string
	http     *http.Client
	// DropForbidden is metadata mode: nobody is there to fix a file or an endpoint, so a 403 is
	// final for its batch (a refused signal must not fill the disk) and a 404 or redirect retries
	// rather than pausing for good, where a later detach could never reach it. Set before the
	// first Send.
	DropForbidden bool
	// Unauthorized, when set, hears which key a 401 refused. Set before the first Send.
	Unauthorized func(key string)
	mu           sync.RWMutex
	key, project string
	noBot        bool
}

func New(endpoint, caFile, key string) (*Client, error) {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	if caFile != "" {
		b, e := os.ReadFile(caFile)
		if e != nil {
			return nil, errors.New("cannot read CA file")
		}
		roots, e := x509.SystemCertPool()
		if e != nil {
			roots = x509.NewCertPool()
		}
		if !roots.AppendCertsFromPEM(b) {
			return nil, errors.New("CA file has no certificates")
		}
		t.TLSClientConfig.RootCAs = roots
	}
	return NewWithClient(endpoint, &http.Client{Transport: t}, key), nil
}
func NewWithClient(endpoint string, h *http.Client, key string) *Client {
	copy := *h
	copy.Timeout = 15 * time.Second
	copy.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &Client{endpoint: strings.TrimSuffix(endpoint, "/"), http: &copy, key: key}
}
func (c *Client) SetCredential(key string) { c.mu.Lock(); defer c.mu.Unlock(); c.key = key }

// SetProject names the project on requests made with an API key.
func (c *Client) SetProject(project string) { c.mu.Lock(); defer c.mu.Unlock(); c.project = project }

// SetNoBot records that the instance has no Bot: Send drops without touching the network.
func (c *Client) SetNoBot(noBot bool) { c.mu.Lock(); defer c.mu.Unlock(); c.noBot = noBot }
func (c *Client) Send(ctx context.Context, signal string, payload []byte) Outcome {
	if signal != "logs" && signal != "metrics" {
		return Outcome{Kind: Drop, Reason: "signal"}
	}
	c.mu.RLock()
	key, project, noBot := c.key, c.project, c.noBot
	c.mu.RUnlock()
	if noBot {
		return Outcome{Kind: Drop, Reason: "no_bot"}
	}
	if key == "" {
		return Outcome{Kind: Pause, Reason: "credential"}
	}
	req, e := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint+"/v1/"+signal, bytes.NewReader(payload))
	if e != nil {
		return Outcome{Kind: Pause, Reason: "endpoint"}
	}
	req.Header.Set("Authorization", "Bearer "+key)
	if project != "" && !strings.HasPrefix(key, "ezgh_ik_") {
		req.Header.Set("x-ezgh-project", project)
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("User-Agent", "ezgh-obs-agent-linux/0.2")
	resp, e := c.http.Do(req)
	if e != nil {
		return Outcome{Kind: Retry, Reason: "network"}
	}
	defer resp.Body.Close()
	code := resp.StatusCode
	switch {
	case code == 429 || code == 502 || code == 503 || code == 504:
		return Outcome{Kind: Retry, After: retryAfter(resp.Header.Get("Retry-After")), Reason: "upstream"}
	case code == 403 && c.DropForbidden:
		return Outcome{Kind: Drop, Reason: "permission"}
	case code == 401 || code == 403:
		if code == 401 && c.Unauthorized != nil {
			c.Unauthorized(key)
		}
		return Outcome{Kind: Pause, Reason: "credential"}
	case (code == 404 || code >= 300 && code < 400) && c.DropForbidden:
		return Outcome{Kind: Retry, Reason: "endpoint"}
	case code == 404 || code >= 300 && code < 400:
		return Outcome{Kind: Pause, Reason: "endpoint"}
	case code == 413:
		return Outcome{Kind: Split, Reason: "size"}
	case code != 200:
		return Outcome{Kind: Drop, Reason: "http_rejection"}
	}
	b, e := io.ReadAll(io.LimitReader(resp.Body, (64<<10)+1))
	if e != nil {
		return Outcome{Kind: Retry, Reason: "response_transport"}
	}
	if len(b) > 64<<10 {
		return Outcome{Kind: Drop, Reason: "invalid_response"}
	}
	// Empty protobuf response is valid. Nonempty bodies must use the negotiated encoding.
	if len(b) > 0 && !strings.HasPrefix(resp.Header.Get("Content-Type"), "application/x-protobuf") {
		return Outcome{Kind: Drop, Reason: "invalid_response"}
	}
	var rejected int64
	if signal == "logs" {
		r := new(logs.ExportLogsServiceResponse)
		if e = proto.Unmarshal(b, r); e == nil {
			rejected = r.GetPartialSuccess().GetRejectedLogRecords()
		}
	} else {
		r := new(metrics.ExportMetricsServiceResponse)
		if e = proto.Unmarshal(b, r); e == nil {
			rejected = r.GetPartialSuccess().GetRejectedDataPoints()
		}
	}
	if e != nil || rejected < 0 {
		return Outcome{Kind: Drop, Reason: "invalid_response"}
	}
	return Outcome{Kind: Success, Rejected: rejected}
}
func retryAfter(v string) time.Duration {
	if n, e := strconv.ParseUint(v, 10, 32); e == nil {
		return time.Duration(n) * time.Second
	}
	if t, e := http.ParseTime(v); e == nil {
		if d := time.Until(t); d > 0 {
			return d
		}
	}
	return 0
}
