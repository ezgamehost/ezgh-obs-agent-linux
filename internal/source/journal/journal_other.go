//go:build !linux || !cgo

package journal

import (
	"context"
	"errors"
	"github.com/ezgamehost/ezgh-obs-agent-linux/internal/telemetry"
)

func Available() error { return errors.New("native journal requires Linux and CGO/libsystemd") }
func Run(context.Context, string, []string, string, string, string, func(string) ([]byte, error), func(context.Context, []*telemetry.LogItem, map[string][]byte) error, func()) error {
	return Available()
}
