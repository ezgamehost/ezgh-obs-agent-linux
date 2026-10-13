package spool

import (
	"errors"
	"path/filepath"
	"testing"
)

func TestAtomicCheckpointAndReplay(t *testing.T) {
	p := filepath.Join(t.TempDir(), "logs.db")
	q, e := Open(p, "a", 8<<20, 0)
	if e != nil {
		t.Fatal(e)
	}
	if e = q.Enqueue([][]byte{[]byte("one"), []byte("two")}, map[string][]byte{"cursor": []byte("after-two")}); e != nil {
		t.Fatal(e)
	}
	q.Close()
	q, e = Open(p, "a", 8<<20, 0)
	if e != nil {
		t.Fatal(e)
	}
	defer q.Close()
	cp, _ := q.Checkpoint("cursor")
	if string(cp) != "after-two" {
		t.Fatal(string(cp))
	}
	b, _ := q.Peek()
	if string(b.Payload) != "one" {
		t.Fatal(b)
	}
	if e = q.Ack(b.ID); e != nil {
		t.Fatal(e)
	}
	b, _ = q.Peek()
	if string(b.Payload) != "two" {
		t.Fatal(b)
	}
}
func TestBindingAndQuota(t *testing.T) {
	p := filepath.Join(t.TempDir(), "q.db")
	q, e := Open(p, "a", 8<<20, 0)
	if e != nil {
		t.Fatal(e)
	}
	q.Enqueue([][]byte{[]byte("x")}, nil)
	q.Close()
	if q, e = Open(p, "b", 8<<20, 0); e == nil {
		q.Close()
		t.Fatal("retargeted pending queue")
	}
	q, e = Open(p, "a", 8<<20, 0)
	if e != nil {
		t.Fatal(e)
	}
	defer q.Close()
	if e = q.Enqueue([][]byte{make([]byte, 8<<20)}, map[string][]byte{"c": []byte("bad")}); !errors.Is(e, ErrFull) {
		t.Fatalf("%v", e)
	}
	cp, _ := q.Checkpoint("c")
	if len(cp) != 0 {
		t.Fatal("checkpoint advanced on rejected data")
	}
}
func TestSplitPreservesFIFO(t *testing.T) {
	q, e := Open(filepath.Join(t.TempDir(), "q"), "a", 8<<20, 0)
	if e != nil {
		t.Fatal(e)
	}
	defer q.Close()
	q.Enqueue([][]byte{[]byte("ab"), []byte("c")}, nil)
	b, _ := q.Peek()
	if e = q.Replace(b.ID, [][]byte{[]byte("a"), []byte("b")}); e != nil {
		t.Fatal(e)
	}
	for _, s := range []string{"a", "b", "c"} {
		b, e = q.Peek()
		if e != nil || string(b.Payload) != s {
			t.Fatalf("%v %v", b, e)
		}
		q.Ack(b.ID)
	}
}
