package export

import (
	"context"
	logs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	"google.golang.org/protobuf/proto"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestHTTPOutcomes(t *testing.T) {
	for _, tc := range []struct {
		status int
		want   Kind
	}{{200, Success}, {429, Retry}, {502, Retry}, {503, Retry}, {504, Retry}, {401, Pause}, {403, Pause}, {404, Pause}, {413, Split}, {400, Drop}, {500, Drop}, {302, Pause}} {
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
			o := c.Send(context.Background(), "logs", []byte{})
			if o.Kind != tc.want {
				t.Fatalf("%+v", o)
			}
			if tc.status == 429 && o.After != 120*time.Second {
				t.Fatal(o)
			}
		})
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
