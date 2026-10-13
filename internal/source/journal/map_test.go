package journal

import (
	"github.com/ezgamehost/ezgh-obs-agent-linux/internal/telemetry"
	"testing"
	"time"
)

func TestMapping(t *testing.T) {
	r := Map(map[string]string{"MESSAGE": "hello", "PRIORITY": "3", "_SYSTEMD_UNIT": "app.service", "SECRET": "no"}, time.Unix(1, 0), "id", "host", "prod")
	if r.Record.SeverityNumber != 17 || r.Record.Body.GetStringValue() != "hello" {
		t.Fatal(r)
	}
	for _, a := range r.Record.Attributes {
		if a.Key == "SECRET" {
			t.Fatal("leaked arbitrary field")
		}
	}
}

func TestInvalidIdentifierCannotPoisonQueue(t *testing.T) {
	r := Map(map[string]string{"MESSAGE": "hello", "SYSLOG_IDENTIFIER": "\xff"}, time.Now(), "id", "host", "prod")
	if _, e := telemetry.EncodeLogs([]*telemetry.LogItem{r}, 1<<20); e != nil {
		t.Fatal(e)
	}
}
