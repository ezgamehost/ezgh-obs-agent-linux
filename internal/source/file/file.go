// Package file tails explicit regular files with bounded framing and durable offsets.
package file

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ezgamehost/ezgh-obs-agent-linux/internal/config"
	"github.com/ezgamehost/ezgh-obs-agent-linux/internal/telemetry"
	"golang.org/x/sys/unix"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

type Sink func(context.Context, []*telemetry.LogItem, map[string][]byte) error
type Checkpoint struct {
	Offset    int64
	Prefix    string
	PrefixLen int
	Path      string
}
type openFile struct {
	f                  *os.File
	path, key, service string
	offset, committed  int64
	line               []byte
	truncated          bool
	lastData           time.Time
}
type Source struct {
	configs                       []config.File
	max                           int
	hostID, hostname, environment string
	load                          func(string) ([]byte, error)
	sink                          Sink
	files                         map[string]*openFile
}

func New(c []config.File, max int, id, hostname, env string, load func(string) ([]byte, error), sink Sink) *Source {
	return &Source{c, max, id, hostname, env, load, sink, map[string]*openFile{}}
}
func (s *Source) Close() {
	for k, f := range s.files {
		f.f.Close()
		delete(s.files, k)
	}
}
func keyOf(st os.FileInfo) string {
	v := st.Sys().(*syscall.Stat_t)
	return fmt.Sprintf("file:%d:%d", v.Dev, v.Ino)
}
func prefix(f *os.File, n int) (string, error) {
	b := make([]byte, n)
	got, e := f.ReadAt(b, 0)
	if e != nil && e != io.EOF {
		return "", e
	}
	h := sha256.Sum256(b[:got])
	return hex.EncodeToString(h[:]), nil
}
func (s *Source) discover(ctx context.Context, now time.Time) error {
	matched := map[string]string{}
	for _, c := range s.configs {
		for _, pattern := range c.Paths {
			paths, e := filepath.Glob(pattern)
			if e != nil {
				return e
			}
			for _, p := range paths {
				excluded := false
				for _, x := range c.Exclude {
					if ok, _ := filepath.Match(x, p); ok {
						excluded = true
					}
				}
				if excluded {
					continue
				}
				st, e := os.Lstat(p)
				if e != nil {
					continue
				}
				if !st.Mode().IsRegular() {
					continue
				}
				k := keyOf(st)
				if previous, ok := matched[k]; ok && previous != c.Service {
					return errors.New("file matched by overlapping sources")
				}
				matched[k] = c.Service
				if _, ok := s.files[k]; ok {
					continue
				}
				if len(s.files) >= s.max {
					return errors.New("file source exceeds max_open_files")
				}
				fd, e := unix.Open(p, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
				var f *os.File
				if e == nil {
					f = os.NewFile(uintptr(fd), p)
				}
				if e != nil {
					return errors.New("cannot read configured log file")
				}
				actual, e := f.Stat()
				if e != nil || !actual.Mode().IsRegular() || !os.SameFile(st, actual) {
					f.Close()
					continue
				}
				state := &openFile{f: f, path: p, key: k, service: c.Service, lastData: now}
				raw, e := s.load(k)
				if e != nil {
					f.Close()
					return e
				}
				var cp Checkpoint
				if len(raw) > 0 {
					if e = json.Unmarshal(raw, &cp); e != nil {
						f.Close()
						return errors.New("invalid file checkpoint")
					}
					fingerprint, e := prefix(f, cp.PrefixLen)
					if e != nil {
						f.Close()
						return e
					}
					if cp.Offset <= st.Size() && fingerprint == cp.Prefix {
						state.offset = cp.Offset
					}
				}
				if len(raw) == 0 && c.StartAt != "beginning" {
					state.offset = st.Size()
				}
				state.committed = state.offset
				if _, e = f.Seek(state.offset, io.SeekStart); e != nil {
					f.Close()
					return e
				}
				s.files[k] = state
				// Persist initial positioning, including tail mode on an empty file.
				if e = s.commit(ctx, state, nil); e != nil {
					return e
				}
			}
		}
	}
	return nil
}
func (s *Source) commit(ctx context.Context, f *openFile, rows []*telemetry.LogItem) error {
	n := int(min(f.committed, 256))
	p, e := prefix(f.f, n)
	if e != nil {
		return e
	}
	raw, e := json.Marshal(Checkpoint{f.committed, p, n, f.path})
	if e != nil {
		return e
	}
	return s.sink(ctx, rows, map[string][]byte{f.key: raw})
}
func (s *Source) Poll(ctx context.Context, now time.Time) error {
	if e := s.discover(ctx, now); e != nil {
		return e
	}
	buf := make([]byte, 64<<10)
	for k, f := range s.files {
		st, e := f.f.Stat()
		if e != nil {
			return e
		}
		if st.Size() < f.offset {
			f.offset = 0
			f.committed = 0
			f.line = nil
			f.truncated = false
			if _, e = f.f.Seek(0, io.SeekStart); e != nil {
				return e
			}
		}
		var rows []*telemetry.LogItem
		emit := func() {
			attrs := map[string]string{"log.file.path": f.path}
			if f.truncated {
				attrs["ezgh.log.truncated"] = "true"
			}
			body := strings.TrimSuffix(string(f.line), "\r")
			rows = append(rows, &telemetry.LogItem{Resource: telemetry.Resource(s.hostID, s.hostname, s.environment, f.service), Record: telemetry.Log(body, now, 0, attrs)})
			f.line = nil
			f.truncated = false
			f.committed = f.offset
		}
		for reads := 0; reads < 4; reads++ {
			n, err := f.f.Read(buf)
			if n > 0 {
				f.lastData = now
			}
			for _, b := range buf[:n] {
				f.offset++
				if b == '\n' {
					emit()
					if len(rows) >= 256 {
						if e = s.commit(ctx, f, rows); e != nil {
							return e
						}
						rows = nil
					}
				} else if len(f.line) < 64<<10 {
					f.line = append(f.line, b)
				} else {
					f.truncated = true
				}
			}
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}
		}
		if len(f.line) > 0 && now.Sub(f.lastData) >= 5*time.Second {
			emit()
		}
		if len(rows) > 0 {
			if e = s.commit(ctx, f, rows); e != nil {
				return e
			}
		}
		// A renamed descriptor is retained long enough to drain late writes.
		if now.Sub(f.lastData) >= 5*time.Second {
			current, e := os.Stat(f.path)
			if e != nil || keyOf(current) != k {
				f.f.Close()
				delete(s.files, k)
			}
		}
	}
	return nil
}
