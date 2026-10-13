package journal

import (
	"github.com/ezgamehost/ezgh-obs-agent-linux/internal/telemetry"
	logs "go.opentelemetry.io/proto/otlp/logs/v1"
	"strconv"
	"time"
)

var Fields = []string{"MESSAGE", "PRIORITY", "_SYSTEMD_UNIT", "SYSLOG_IDENTIFIER", "_PID", "_UID", "_GID", "_TRANSPORT", "_BOOT_ID"}

func Map(f map[string]string, at time.Time, id, host, env string) *telemetry.LogItem {
	service := f["_SYSTEMD_UNIT"]
	if service == "" {
		service = f["SYSLOG_IDENTIFIER"]
	}
	if service == "" {
		service = "linux"
	}
	attrs := map[string]string{}
	for _, k := range Fields[2:] {
		if v := f[k]; v != "" {
			attrs[k] = v
		}
	}
	severity := logs.SeverityNumber_SEVERITY_NUMBER_UNSPECIFIED
	if p, e := strconv.Atoi(f["PRIORITY"]); e == nil && p >= 0 && p <= 7 {
		severity = []logs.SeverityNumber{21, 21, 21, 17, 13, 9, 9, 5}[p]
	}
	return &telemetry.LogItem{Resource: telemetry.Resource(id, host, env, service), Record: telemetry.Log(f["MESSAGE"], at, severity, attrs)}
}
