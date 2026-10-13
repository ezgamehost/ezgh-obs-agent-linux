package main

import (
	"bytes"
	"testing"
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
