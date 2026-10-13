//go:build linux && cgo

package journal

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/coreos/go-systemd/v22/sdjournal"
	"github.com/ezgamehost/ezgh-obs-agent-linux/internal/telemetry"
	"google.golang.org/protobuf/proto"
	"runtime"
	"time"
)

type checkpoint struct {
	Cursor   string
	Realtime uint64
	BootID   string
}

func Available() error {
	j, e := sdjournal.NewJournal()
	if e != nil {
		return e
	}
	return j.Close()
}
func Run(ctx context.Context, start string, excluded []string, id, host, env string, load func(string) ([]byte, error), sink func(context.Context, []*telemetry.LogItem, map[string][]byte) error, gap func()) error {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	j, e := sdjournal.NewJournal()
	if e != nil {
		return errors.New("cannot open system journal")
	}
	defer j.Close()
	if e = j.SetDataThreshold(128 << 10); e != nil {
		return e
	}
	raw, e := load("journal")
	if e != nil {
		return e
	}
	var cp checkpoint
	if len(raw) > 0 {
		if e = json.Unmarshal(raw, &cp); e != nil {
			return errors.New("invalid journal checkpoint")
		}
		e = j.SeekCursor(cp.Cursor)
		if e == nil {
			_, e = j.Next()
		}
		if e == nil {
			e = j.TestCursor(cp.Cursor)
		}
		if e != nil {
			gap()
			if e = j.SeekRealtimeUsec(cp.Realtime); e != nil {
				return e
			}
		}
	} else if start == "beginning" {
		if e = j.SeekHead(); e != nil {
			return e
		}
	} else {
		if e = j.SeekTail(); e != nil {
			return e
		}
		if _, e = j.Previous(); e != nil {
			return e
		}
		if cursor, e := j.GetCursor(); e == nil {
			t, _ := j.GetRealtimeUsec()
			raw, _ := json.Marshal(checkpoint{Cursor: cursor, Realtime: t})
			if e = sink(ctx, nil, map[string][]byte{"journal": raw}); e != nil {
				return e
			}
		}
	}
	exclusions := map[string]bool{}
	for _, u := range excluded {
		exclusions[u] = true
	}
	for ctx.Err() == nil {
		var rows []*telemetry.LogItem
		var last []byte
		batchBytes := 0
		for i := 0; i < 256; i++ {
			n, e := j.Next()
			if e != nil {
				return e
			}
			if n == 0 {
				break
			}
			fields := map[string]string{}
			for _, k := range Fields {
				v, e := j.GetDataValue(k)
				if e == nil {
					fields[k] = v
				}
			}
			t, e := j.GetRealtimeUsec()
			if e != nil {
				return e
			}
			cursor, e := j.GetCursor()
			if e != nil {
				return e
			}
			last, _ = json.Marshal(checkpoint{cursor, t, fields["_BOOT_ID"]})
			if !exclusions[fields["_SYSTEMD_UNIT"]] {
				item := Map(fields, time.UnixMicro(int64(t)), id, host, env)
				rows = append(rows, item)
				batchBytes += proto.Size(item.Record) + proto.Size(item.Resource) + 32
				if batchBytes >= 256<<10 {
					break
				}
			}
		}
		if last != nil {
			if e = sink(ctx, rows, map[string][]byte{"journal": last}); e != nil {
				return e
			}
		} else {
			j.Wait(250 * time.Millisecond)
		}
	}
	return ctx.Err()
}
