package credential

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

var t0 = time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)

type fakeNow struct{ t time.Time }

func (f *fakeNow) Now() time.Time { return f.t }

func TestMetadataCredentials(t *testing.T) {
	var tokens, fetches atomic.Int32
	var noBot, refuseToken, outage atomic.Bool
	key := atomic.Value{}
	key.Store("ezgh_one")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("EZGH-Metadata") != "true" {
			t.Errorf("missing EZGH-Metadata header")
		}
		switch {
		case r.Method == http.MethodPut && r.URL.Path == "/ezgh/v1/token":
			if r.Header.Get("EZGH-Metadata-Token-TTL-Seconds") == "" {
				t.Errorf("token request without a TTL")
			}
			tokens.Add(1)
			_, _ = io.WriteString(w, "tok\n")
		case r.Method == http.MethodGet && r.URL.Path == "/ezgh/v1/credentials":
			fetches.Add(1)
			switch {
			case outage.Load():
				w.WriteHeader(http.StatusServiceUnavailable)
			case refuseToken.Swap(false) || r.Header.Get("EZGH-Metadata-Token") != "tok":
				w.WriteHeader(http.StatusUnauthorized)
			case noBot.Load():
				w.WriteHeader(http.StatusNotFound)
				_, _ = io.WriteString(w, `{"error":{"code":"no_bot"}}`)
			default:
				_ = json.NewEncoder(w).Encode(map[string]any{"apiKey": key.Load(), "expiresAt": expires.Load(), "bot": "bot_x"})
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	now := &fakeNow{t0}
	expires.Store(t0.Add(10 * time.Minute))
	m := &Metadata{Endpoint: srv.URL, Now: now.Now}
	ctx := context.Background()

	if k, err := m.Key(ctx); err != nil || k != "ezgh_one" {
		t.Fatalf("Key = %q, %v", k, err)
	}
	if k, _ := m.Key(ctx); k != "ezgh_one" || fetches.Load() != 1 {
		t.Fatalf("the key wasn't kept: %d fetches", fetches.Load())
	}

	// Refreshed 5 minutes before it expires.
	key.Store("ezgh_two")
	now.t = t0.Add(5*time.Minute + time.Second)
	expires.Store(now.t.Add(10 * time.Minute))
	if k, _ := m.Key(ctx); k != "ezgh_two" {
		t.Fatalf("not refreshed before expiry: %q", k)
	}

	// A failed fetch keeps the old key until it expires, then reports the outage: not ErrNoBot.
	outage.Store(true)
	now.t = now.t.Add(6 * time.Minute)
	if k, err := m.Key(ctx); err != nil || k != "ezgh_two" {
		t.Fatalf("old key not kept through an outage: %q, %v", k, err)
	}
	now.t = now.t.Add(5 * time.Minute)
	if k, err := m.Key(ctx); err == nil || k != "" || errors.Is(err, ErrNoBot) {
		t.Fatalf("an expired key: %q, %v", k, err)
	}
	outage.Store(false)

	// The metadata token is gone: one new token, then the key.
	now.t = now.t.Add(time.Minute)
	expires.Store(now.t.Add(10 * time.Minute))
	refuseToken.Store(true)
	before := tokens.Load()
	if _, err := m.Key(ctx); err != nil || tokens.Load() != before+1 {
		t.Fatalf("token not renewed after a 401: %v, %d tokens", err, tokens.Load())
	}

	// No Bot attached: the key goes, and it's asked again every 3 minutes.
	noBot.Store(true)
	now.t = now.t.Add(6 * time.Minute)
	if k, err := m.Key(ctx); !errors.Is(err, ErrNoBot) || k != "" {
		t.Fatalf("%q, %v, want ErrNoBot", k, err)
	}
	n := fetches.Load()
	now.t = now.t.Add(2 * time.Minute)
	if _, err := m.Key(ctx); !errors.Is(err, ErrNoBot) || fetches.Load() != n {
		t.Fatalf("asked again too soon: %v, %d fetches", err, fetches.Load()-n)
	}
	now.t = now.t.Add(time.Minute)
	expires.Store(now.t.Add(10 * time.Minute))
	noBot.Store(false)
	if k, err := m.Key(ctx); err != nil || k != "ezgh_two" {
		t.Fatalf("after the Bot was attached: %q, %v", k, err)
	}
}

// expires is what the fake service answers as the key's expiry.
var expires atomic.Value

func TestInstanceNeedsNoToken(t *testing.T) {
	var calls atomic.Int32
	doc := atomic.Value{}
	doc.Store(`{"id":"ins_a1b2c3d4e5f6","project":"prj_g7h8i9j0k1l2","projectId":"2b5f2a64-1f6b-4d7e-9b53-3a1f8a8c5b11"}`)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/ezgh/v1/instance" || r.Header.Get("EZGH-Metadata-Token") != "" {
			t.Errorf("%s %s", r.Method, r.URL.Path)
		}
		calls.Add(1)
		_, _ = io.WriteString(w, doc.Load().(string))
	}))
	defer srv.Close()
	m := &Metadata{Endpoint: srv.URL}
	id, project, err := m.Instance(context.Background())
	if err != nil || id != "ins_a1b2c3d4e5f6" || project != "prj_g7h8i9j0k1l2" {
		t.Fatalf("%q %q %v", id, project, err)
	}
	if _, _, _ = m.Instance(context.Background()); calls.Load() != 1 {
		t.Fatalf("not kept: %d calls", calls.Load())
	}

	// An older definition's project is a UUID: a state to report, not a fault.
	doc.Store(`{"id":"ins_a1b2c3d4e5f6","project":"2b5f2a64-1f6b-4d7e-9b53-3a1f8a8c5b11"}`)
	if _, _, err = (&Metadata{Endpoint: srv.URL}).Instance(context.Background()); !errors.Is(err, ErrNoProject) {
		t.Fatalf("err = %v, want ErrNoProject", err)
	}
	// The ID names a directory.
	for _, bad := range []string{`{"id":"../../etc","project":"prj_g7h8i9j0k1l2"}`, `{"project":"prj_g7h8i9j0k1l2"}`, `not json`} {
		doc.Store(bad)
		if _, _, err = (&Metadata{Endpoint: srv.URL}).Instance(context.Background()); err == nil || errors.Is(err, ErrNoProject) {
			t.Fatalf("%s: err = %v", bad, err)
		}
	}
}

func TestAvailable(t *testing.T) {
	p := filepath.Join(t.TempDir(), "product_name")
	if (&Metadata{ProductNamePath: p}).Available() {
		t.Fatal("no product name, yet available")
	}
	for name, want := range map[string]bool{"EZGH Compute\n": true, "Standard PC (Q35 + ICH9, 2009)\n": false} {
		if err := os.WriteFile(p, []byte(name), 0o600); err != nil {
			t.Fatal(err)
		}
		if got := (&Metadata{ProductNamePath: p}).Available(); got != want {
			t.Fatalf("%q: %v", strings.TrimSpace(name), got)
		}
	}
}
