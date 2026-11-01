package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ezgamehost/ezgh-obs-agent-linux/internal/app"
	"github.com/ezgamehost/ezgh-obs-agent-linux/internal/config"
	"github.com/ezgamehost/ezgh-obs-agent-linux/internal/credential"
	"github.com/ezgamehost/ezgh-obs-agent-linux/internal/diagnostics"
	"github.com/ezgamehost/ezgh-obs-agent-linux/internal/source/journal"
	"golang.org/x/sys/unix"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
)

var version = "dev"
var commit = "unknown"

func main() { os.Exit(execute(os.Args[1:], os.Stdout, os.Stderr)) }

// metadataChecks reports the instance, its project and whether a Bot is attached, never the key.
// No Bot is a working state, not a failure.
func metadataChecks(ctx context.Context, m *credential.Metadata, checks map[string]string, record func(string, error)) {
	if !m.Available() {
		record("credential", errors.New("credential: metadata works only on an EZGH Compute instance"))
		return
	}
	id, project, e := m.Instance(ctx)
	record("credential", e)
	if e != nil {
		return
	}
	checks["instance"], checks["project"] = id, project
	switch _, e = m.Key(ctx); {
	case e == nil:
		checks["bot"] = "present"
	case errors.Is(e, credential.ErrNoBot):
		checks["bot"] = "absent"
	default:
		record("bot", e)
	}
}
func execute(args []string, out, errOut io.Writer) int {
	if len(args) == 0 || args[0] == "help" || args[0] == "--help" {
		fmt.Fprintln(out, "Usage: ezgh-obs-agent-linux {run|validate|doctor} [--config PATH]\n       ezgh-obs-agent-linux version")
		return 0
	}
	if args[0] == "version" && len(args) == 1 {
		fmt.Fprintf(out, "ezgh-obs-agent-linux %s (%s)\n", version, commit)
		return 0
	}
	command := args[0]
	if command != "run" && command != "validate" && command != "doctor" {
		fmt.Fprintln(errOut, "unknown command (use help)")
		return 2
	}
	path := "/etc/ezgh-obs-agent-linux/config.yaml"
	if len(args) == 3 && args[1] == "--config" {
		path = args[2]
	} else if len(args) != 1 {
		fmt.Fprintln(errOut, "expected --config PATH (credentials belong in a file)")
		return 2
	}
	c, e := config.Load(path)
	if e != nil {
		fmt.Fprintln(errOut, e)
		return 2
	}
	if command == "validate" {
		fmt.Fprintln(out, "Configuration is valid")
		return 0
	}
	if command == "doctor" {
		checks := map[string]string{}
		good := true
		record := func(name string, e error) {
			if e != nil {
				checks[name] = e.Error()
				good = false
			} else {
				checks[name] = "ok"
			}
		}
		if c.Credential == config.CredentialMetadata {
			metadataChecks(context.Background(), &credential.Metadata{}, checks, record)
		} else {
			_, e := c.ReadKey()
			record("credential", e)
		}
		record("state_writable", unix.Access(c.StateDir, unix.W_OK))
		if c.Logs.Journal.Enabled {
			record("journal", journal.Available())
		}
		status, e := diagnostics.Read(context.Background(), filepath.Join(c.StateDir, "status.sock"))
		record("daemon", e)
		json.NewEncoder(out).Encode(struct {
			Checks map[string]string    `json:"checks"`
			Status diagnostics.Snapshot `json:"status"`
		}{checks, status})
		if !good {
			return 1
		}
		return 0
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)
	defer signal.Stop(hup)
	if e = app.Run(ctx, c, hup); e != nil {
		fmt.Fprintln(errOut, e)
		return 1
	}
	return 0
}
