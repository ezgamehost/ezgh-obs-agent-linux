// Package spool owns synchronous, transactional delivery queues and source checkpoints.
package spool

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
	"golang.org/x/sys/unix"
)

var ErrFull = errors.New("durable queue is full")
var ErrEmpty = errors.New("durable queue is empty")
var batches = []byte("batches")
var checkpoints = []byte("checkpoints")
var meta = []byte("meta")

type Batch struct {
	ID      []byte
	Payload []byte
	Created time.Time
}
type Stats struct {
	Batches int       `json:"batches"`
	Bytes   int64     `json:"bytes"`
	Oldest  time.Time `json:"oldest,omitempty"`
}
type Queue struct {
	db      *bolt.DB
	path    string
	limit   int64
	reserve uint64
	mu      sync.Mutex
}

func Open(path, binding string, limit int64, reserve uint64) (*Queue, error) {
	if st, e := os.Lstat(path); e == nil && (!st.Mode().IsRegular() || st.Mode().Perm()&0077 != 0) {
		return nil, errors.New("queue must be a private regular file")
	}
	db, e := bolt.Open(path, 0600, &bolt.Options{Timeout: time.Second, FreelistType: bolt.FreelistMapType})
	if e != nil {
		return nil, fmt.Errorf("open queue (restore corrupt state; do not delete automatically): %w", e)
	}
	db.AllocSize = 64 << 10
	q := &Queue{db: db, path: path, limit: limit, reserve: reserve}
	e = db.Update(func(tx *bolt.Tx) error {
		for _, k := range [][]byte{batches, checkpoints, meta} {
			if _, e := tx.CreateBucketIfNotExists(k); e != nil {
				return e
			}
		}
		m := tx.Bucket(meta)
		v := m.Get([]byte("version"))
		if v != nil && string(v) != "1" {
			return errors.New("unsupported queue format")
		}
		old := m.Get([]byte("binding"))
		k, _ := tx.Bucket(batches).Cursor().First()
		if old != nil && string(old) != binding && k != nil {
			return errors.New("target changed with pending data; drain queue or use new state directory")
		}
		if e := m.Put([]byte("version"), []byte("1")); e != nil {
			return e
		}
		return m.Put([]byte("binding"), []byte(binding))
	})
	if e == nil {
		e = db.View(func(tx *bolt.Tx) error {
			for err := range tx.Check() {
				if err != nil {
					return err
				}
			}
			return nil
		})
	}
	if e == nil {
		if st, err := os.Stat(path); err != nil {
			e = err
		} else if st.Size() > limit {
			e = errors.New("queue exceeds configured budget")
		}
	}
	if e != nil {
		db.Close()
		return nil, e
	}
	return q, nil
}
func (q *Queue) Close() error { return q.db.Close() }

// Admission reserves B-tree copy-on-write, freelist and split overhead in addition to payload.
// Pending pages cannot be reused by this transaction and are intentionally not counted.
func (q *Queue) admit(n int64) error {
	st, e := os.Stat(q.path)
	if e != nil {
		return e
	}
	s := q.db.Stats()
	need := n*4 + (256 << 10) + int64(st.Size()/512)
	free := int64(s.FreePageN) * int64(q.db.Info().PageSize)
	growth := max(int64(0), need-free)
	if st.Size()+growth > q.limit {
		return ErrFull
	}
	var fs unix.Statfs_t
	if e = unix.Statfs(q.path, &fs); e != nil {
		return e
	}
	available := uint64(fs.Bavail) * uint64(fs.Bsize)
	if available < q.reserve+uint64(growth) {
		return ErrFull
	}
	return nil
}
func (q *Queue) Enqueue(payloads [][]byte, cp map[string][]byte) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	var n int64
	for _, p := range payloads {
		n += int64(len(p))
	}
	for k, v := range cp {
		n += int64(len(k) + len(v))
	}
	if e := q.admit(n); e != nil {
		return e
	}
	return q.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(batches)
		for _, p := range payloads {
			id, e := b.NextSequence()
			if e != nil {
				return e
			}
			key := make([]byte, 8)
			binary.BigEndian.PutUint64(key, id)
			if e = b.Put(key, pack(time.Now(), p)); e != nil {
				return e
			}
		}
		for k, v := range cp {
			if e := tx.Bucket(checkpoints).Put([]byte(k), v); e != nil {
				return e
			}
		}
		return nil
	})
}
func pack(t time.Time, p []byte) []byte {
	v := make([]byte, 8+len(p))
	binary.BigEndian.PutUint64(v, uint64(t.UnixNano()))
	copy(v[8:], p)
	return v
}
func (q *Queue) Peek() (Batch, error) {
	var out Batch
	e := q.db.View(func(tx *bolt.Tx) error {
		k, v := tx.Bucket(batches).Cursor().First()
		if k == nil {
			return ErrEmpty
		}
		if len(v) < 8 {
			return errors.New("corrupt queue record")
		}
		out = Batch{ID: append([]byte(nil), k...), Payload: append([]byte(nil), v[8:]...), Created: time.Unix(0, int64(binary.BigEndian.Uint64(v[:8])))}
		return nil
	})
	return out, e
}
func (q *Queue) Ack(id []byte) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.db.Update(func(tx *bolt.Tx) error { return tx.Bucket(batches).Delete(id) })
}
func (q *Queue) Replace(id []byte, parts [][]byte) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(parts) != 2 || len(id) > 64 {
		return errors.New("invalid split")
	}
	return q.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(batches)
		v := b.Get(id)
		if len(v) < 8 {
			return errors.New("missing split parent")
		}
		created := time.Unix(0, int64(binary.BigEndian.Uint64(v[:8])))
		for i, p := range parts {
			k := append(append([]byte(nil), id...), byte(i))
			if e := b.Put(k, pack(created, p)); e != nil {
				return e
			}
		}
		return b.Delete(id)
	})
}
func (q *Queue) Checkpoint(k string) ([]byte, error) {
	var v []byte
	e := q.db.View(func(tx *bolt.Tx) error { v = append([]byte(nil), tx.Bucket(checkpoints).Get([]byte(k))...); return nil })
	return v, e
}
func (q *Queue) Stats() (Stats, error) {
	var s Stats
	e := q.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(batches)
		s.Batches = b.Stats().KeyN
		k, v := b.Cursor().First()
		if k != nil && len(v) >= 8 {
			s.Oldest = time.Unix(0, int64(binary.BigEndian.Uint64(v[:8])))
		}
		return nil
	})
	if e == nil {
		st, err := os.Stat(q.path)
		if err != nil {
			return s, err
		}
		s.Bytes = st.Size()
	}
	return s, e
}
func (q *Queue) CanAccept() bool { q.mu.Lock(); defer q.mu.Unlock(); return q.admit(1<<20) == nil }
