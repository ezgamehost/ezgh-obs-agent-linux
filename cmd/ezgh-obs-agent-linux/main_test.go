package main

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/ezgamehost/ezgh-obs-agent-linux/internal/credential"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCLI(t *testing.T) {
	for _, args := range [][]string{{"version"}, {"help"}} {
		var b bytes.Buffer
		if code := execute(args, &b, &b); code != 0 || b.Len() == 0 {
			t.Fatalf("%v %d %s", args, code, &b)
		}
	}
	var b bytes.Buffer
	if code := execute([]string{"run", "--token", "secret"}, &b, &b); code != 2 {
		t.Fatal(code)
	}
	if bytes.Contains(b.Bytes(), []byte("secret")) {
		t.Fatal("leaked CLI value")
	}
}

func TestDoctorMetadata(t *testing.T) {
	bot := true
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ezgh/v1/instance":
			io.WriteString(w, `{"id":"ins_a1b2c3d4e5f6","project":"prj_g7h8i9j0k1l2"}`)
		case "/ezgh/v1/token":
			io.WriteString(w, "tok")
		case "/ezgh/v1/credentials":
			if bot {
				json.NewEncoder(w).Encode(map[string]any{"apiKey": "ezgh_secret", "expiresAt": time.Now().Add(10 * time.Minute)})
				return
			}
			w.WriteHeader(http.StatusNotFound)
			io.WriteString(w, `{"error":{"code":"no_bot"}}`)
		}
	}))
	defer srv.Close()
	product := filepath.Join(t.TempDir(), "product_name")
	os.WriteFile(product, []byte("EZGH Compute\n"), 0600)
	check := func() (map[string]string, []string) {
		checks, failed := map[string]string{}, []string{}
		m := &credential.Metadata{Endpoint: srv.URL, ProductNamePath: product}
		metadataChecks(context.Background(), m, checks, func(name string, e error) {
			if e != nil {
				failed = append(failed, name)
			}
		})
		return checks, failed
	}
	got, failed := check()
	if got["instance"] != "ins_a1b2c3d4e5f6" || got["project"] != "prj_g7h8i9j0k1l2" || got["bot"] != "present" || len(failed) != 0 {
		t.Fatalf("%v, failed %v", got, failed)
	}
	if b, _ := json.Marshal(got); strings.Contains(string(b), "secret") {
		t.Fatal("doctor printed the key")
	}
	bot = false
	if got, failed = check(); got["bot"] != "absent" || len(failed) != 0 {
		t.Fatalf("%v, failed %v", got, failed)
	}
	os.WriteFile(product, []byte("Standard PC\n"), 0600)
	if _, failed = check(); len(failed) != 1 {
		t.Fatalf("off compute: failed %v", failed)
	}
}
