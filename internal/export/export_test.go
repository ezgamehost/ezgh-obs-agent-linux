package export

import (
	"context"
	logs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	"google.golang.org/protobuf/proto"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestHTTPOutcomes(t *testing.T) {
	for _, tc := range []struct {
		status int
		want   Kind
	}{{200, Success}, {429, Retry}, {502, Retry}, {503, Retry}, {504, Retry}, {401, Pause}, {403, Pause}, {404, Pause}, {413, Split}, {400, Drop}, {500, Drop}, {302, Pause}} {
		for _, dropForbidden := range []bool{false, true} {
			if tc.status == 403 && dropForbidden {
				continue // TestMetadata403Drops
			}
			want := tc.want
			if dropForbidden && (tc.status == 404 || tc.status == 302) {
				want = Retry // TestMetadataEndpointRefusalRetries
			}
			t.Run(http.StatusText(tc.status), func(t *testing.T) {
				s := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path != "/v1/logs" || r.Header.Get("Authorization") != "Bearer test" {
						t.Error("wrong request")
					}
					io.Copy(io.Discard, r.Body)
					w.Header().Set("Retry-After", "120")
					w.WriteHeader(tc.status)
				}))
				defer s.Close()
				c := NewWithClient(s.URL, s.Client(), "test")
				c.DropForbidden = dropForbidden
				o := c.Send(context.Background(), "logs", []byte{})
				if o.Kind != want {
					t.Fatalf("%+v", o)
				}
				if tc.status == 429 && o.After != 120*time.Second {
					t.Fatal(o)
				}
			})
		}
	}
}
func TestPartialSuccessAndMalformed(t *testing.T) {
	for _, malformed := range []bool{false, true} {
		s := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/x-protobuf")
			if malformed {
				w.Write([]byte{255})
				return
			}
			b, _ := proto.Marshal(&logs.ExportLogsServiceResponse{PartialSuccess: &logs.ExportLogsPartialSuccess{RejectedLogRecords: 3, ErrorMessage: "untrusted diagnostic"}})
			w.Write(b)
		}))
		c := NewWithClient(s.URL, s.Client(), "test")
		o := c.Send(context.Background(), "logs", nil)
		s.Close()
		if malformed {
			if o.Kind != Drop {
				t.Fatal(o)
			}
		} else if o.Kind != Success || o.Rejected != 3 {
			t.Fatal(o)
		}
	}
}
func TestNoCredentialRedirect(t *testing.T) {
	hits := 0
	target := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits++ }))
	defer target.Close()
	s := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, target.URL, 307) }))
	defer s.Close()
	c := NewWithClient(s.URL, s.Client(), "secret")
	o := c.Send(context.Background(), "logs", nil)
	if hits != 0 || o.Kind != Pause {
		t.Fatalf("redirect followed: %d %+v", hits, o)
	}
}

func TestInterruptedSuccessRetries(t *testing.T) {
	s := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.Header().Set("Content-Length", "100")
		w.Write([]byte{10, 2})
	}))
	defer s.Close()
	c := NewWithClient(s.URL, s.Client(), "test")
	if o := c.Send(context.Background(), "logs", nil); o.Kind != Retry {
		t.Fatal(o)
	}
}

const apiKey = "ezgh_0123456789abcdefghijABCDEFGHIJ0123456789"

// server answers status and counts the requests that reached it.
func server(t *testing.T, status int) (*httptest.Server, *atomic.Int32) {
	var hits atomic.Int32
	s := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(status)
	}))
	t.Cleanup(s.Close)
	return s, &hits
}

func TestMetadata403Drops(t *testing.T) {
	s, _ := server(t, 403)
	c := NewWithClient(s.URL, s.Client(), apiKey)
	c.DropForbidden = true
	if o := c.Send(context.Background(), "logs", nil); o.Kind != Drop || o.Reason != "permission" {
		t.Fatalf("%+v", o)
	}
}

func TestFile403Pauses(t *testing.T) {
	s, _ := server(t, 403)
	c := NewWithClient(s.URL, s.Client(), apiKey)
	if o := c.Send(context.Background(), "logs", nil); o.Kind != Pause || o.Reason != "credential" {
		t.Fatalf("%+v", o)
	}
}

func Test401PausesInEveryMode(t *testing.T) {
	for _, drop := range []bool{false, true} {
		s, _ := server(t, 401)
		c := NewWithClient(s.URL, s.Client(), apiKey)
		c.DropForbidden = drop
		if o := c.Send(context.Background(), "metrics", nil); o.Kind != Pause || o.Reason != "credential" {
			t.Fatalf("DropForbidden=%v: %+v", drop, o)
		}
	}
}

func TestNoBotDropsWithoutNetwork(t *testing.T) {
	s, hits := server(t, 200)
	c := NewWithClient(s.URL, s.Client(), apiKey)
	c.SetNoBot(true)
	if o := c.Send(context.Background(), "logs", nil); o.Kind != Drop || o.Reason != "no_bot" || hits.Load() != 0 {
		t.Fatalf("%+v, %d requests", o, hits.Load())
	}
	c.SetNoBot(false)
	if o := c.Send(context.Background(), "logs", nil); o.Kind != Success || hits.Load() != 1 {
		t.Fatalf("%+v, %d requests", o, hits.Load())
	}
}

func TestEmptyKeyPausesWithoutNetwork(t *testing.T) {
	s, hits := server(t, 200)
	c := NewWithClient(s.URL, s.Client(), "")
	if o := c.Send(context.Background(), "logs", nil); o.Kind != Pause || o.Reason != "credential" || hits.Load() != 0 {
		t.Fatalf("%+v, %d requests", o, hits.Load())
	}
}

func TestProjectHeaderSentForAPIKey(t *testing.T) {
	var got atomic.Value
	s := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got.Store(r.Header.Values("x-ezgh-project"))
	}))
	defer s.Close()
	for key, want := range map[string][]string{apiKey: {"prj_g7h8i9j0k1l2"}, "ezgh_ik_" + strings.Repeat("a", 43): nil} {
		c := NewWithClient(s.URL, s.Client(), key)
		c.SetProject("prj_g7h8i9j0k1l2")
		if o := c.Send(context.Background(), "logs", nil); o.Kind != Success {
			t.Fatalf("%+v", o)
		}
		if v := got.Load().([]string); !slices.Equal(v, want) {
			t.Fatalf("%.8s: header %v, want %v", key, v, want)
		}
	}
}

// Nobody on an instance can fix an endpoint, so metadata mode keeps trying, and each try reaches
// the no-Bot check.
func TestMetadataEndpointRefusalRetries(t *testing.T) {
	for _, status := range []int{404, 302} {
		s, hits := server(t, status)
		c := NewWithClient(s.URL, s.Client(), apiKey)
		c.DropForbidden = true
		if o := c.Send(context.Background(), "logs", nil); o.Kind != Retry || o.Reason != "endpoint" {
			t.Fatalf("%d: %+v", status, o)
		}
		c.SetNoBot(true)
		if o := c.Send(context.Background(), "logs", nil); o.Kind != Drop || hits.Load() != 1 {
			t.Fatalf("%d: %+v, %d requests", status, o, hits.Load())
		}
	}
}

func TestUnauthorizedNamesTheRefusedKey(t *testing.T) {
	for status, want := range map[int]string{401: apiKey, 403: "", 200: ""} {
		s, _ := server(t, status)
		c := NewWithClient(s.URL, s.Client(), apiKey)
		c.DropForbidden = true
		refused := ""
		c.Unauthorized = func(key string) { refused = key }
		c.Send(context.Background(), "logs", nil)
		if refused != want {
			t.Fatalf("%d: refused %q", status, refused)
		}
	}
	s, hits := server(t, 200)
	c := NewWithClient(s.URL, s.Client(), "")
	c.Unauthorized = func(string) { t.Error("called with no key") }
	if o := c.Send(context.Background(), "logs", nil); o.Kind != Pause || hits.Load() != 0 {
		t.Fatalf("%+v", o)
	}
}
