// Package identity persists a random host identifier independently of hostname.
package identity

import (
	"errors"
	"github.com/google/uuid"
	"os"
	"path/filepath"
	"strings"
)

func Load(dir, configured string) (string, error) {
	if configured != "" {
		return configured, nil
	}
	p := filepath.Join(dir, "host-id")
	b, e := os.ReadFile(p)
	if e == nil {
		v := strings.TrimSpace(string(b))
		if _, e := uuid.Parse(v); e != nil {
			return "", errors.New("invalid persisted host-id; restore it instead of silently changing identity")
		}
		return v, nil
	}
	if !errors.Is(e, os.ErrNotExist) {
		return "", e
	}
	v := uuid.NewString()
	f, e := os.CreateTemp(dir, ".host-id-")
	if e != nil {
		return "", e
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if _, e = f.WriteString(v + "\n"); e != nil {
		f.Close()
		return "", e
	}
	if e = f.Sync(); e != nil {
		f.Close()
		return "", e
	}
	if e = f.Close(); e != nil {
		return "", e
	}
	if e = os.Rename(tmp, p); e != nil {
		return "", e
	}
	d, e := os.Open(dir)
	if e != nil {
		return "", e
	}
	defer d.Close()
	return v, d.Sync()
}
