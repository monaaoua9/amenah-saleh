package config

import (
	"flag"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func newFS() (*flag.FlagSet, *int64, *string, *bool, *time.Duration) {
	fs := flag.NewFlagSet("t", flag.ContinueOnError)
	size := fs.Int64("window", 1000, "")
	dir := fs.String("ckdir", "./checkpoints", "")
	shed := fs.Bool("shed", false, "")
	every := fs.Duration("ckevery", 5*time.Second, "")
	return fs, size, dir, shed, every
}

func write(t *testing.T, body string) string {
	p := filepath.Join(t.TempDir(), "c.json")
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestFileFillsUnsetFlags(t *testing.T) {
	fs, size, dir, shed, every := newFS()
	fs.Parse([]string{"-ckdir", "/cli"})
	p := write(t, `{"window": 250, "ckdir": "/file", "shed": true, "ckevery": "2s"}`)
	if err := Load(fs, p); err != nil {
		t.Fatal(err)
	}
	if *size != 250 || *dir != "/cli" || !*shed || *every != 2*time.Second {
		t.Fatalf("size=%d dir=%s shed=%v every=%v", *size, *dir, *shed, *every)
	}
}

func TestLargeNumbersStayIntegers(t *testing.T) {
	fs, size, _, _, _ := newFS()
	fs.Parse(nil)
	if err := Load(fs, write(t, `{"window": 3600000000}`)); err != nil {
		t.Fatal(err)
	}
	if *size != 3600000000 {
		t.Fatalf("got %d", *size)
	}
}

func TestBadFiles(t *testing.T) {
	for name, body := range map[string]string{
		"unknown": `{"windw": 5}`,
		"badtype": `{"window": [1]}`,
		"badval":  `{"window": "abc"}`,
		"notjson": `window=5`,
	} {
		fs, _, _, _, _ := newFS()
		fs.Parse(nil)
		if err := Load(fs, write(t, body)); err == nil {
			t.Errorf("%s: want error", name)
		}
	}
	fs, _, _, _, _ := newFS()
	if err := Load(fs, filepath.Join(t.TempDir(), "missing.json")); err == nil {
		t.Error("missing file accepted")
	}
}
