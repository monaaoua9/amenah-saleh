package checkpoint

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestPutGetPlainAndGzip(t *testing.T) {
	dir := t.TempDir()
	data := bytes.Repeat([]byte("window state "), 2000)

	s := NewStore(dir)
	if err := s.Put(1, "worker-0", data); err != nil {
		t.Fatal(err)
	}
	if err := s.Compress(true).Put(2, "worker-0", data); err != nil {
		t.Fatal(err)
	}

	for _, id := range []uint64{1, 2} {
		got, err := s.Get(id, "worker-0")
		if err != nil || !bytes.Equal(got, data) {
			t.Fatalf("chk %d: err=%v, equal=%v", id, err, bytes.Equal(got, data))
		}
	}
	// a fresh, non-compressing store still reads the gzip one
	if got, err := NewStore(dir).Get(2, "worker-0"); err != nil || !bytes.Equal(got, data) {
		t.Fatalf("plain store on gzip checkpoint: %v", err)
	}

	st, _ := os.Stat(filepath.Join(dir, "chk-00000002", "worker-0.snap.gz"))
	if st == nil || st.Size() >= int64(len(data))/4 {
		t.Fatalf("gzip file not smaller: %v", st)
	}
}

func TestGetMissing(t *testing.T) {
	if _, err := NewStore(t.TempDir()).Get(9, "nope"); err == nil {
		t.Fatal("want error")
	}
}

func TestLatestAndPrune(t *testing.T) {
	s := NewStore(t.TempDir())
	for id := uint64(1); id <= 5; id++ {
		s.Put(id, "t", []byte("x"))
		if id != 4 { // 4 never commits, so it must not count
			s.Commit(id)
		}
	}
	if id, ok := s.Latest(); !ok || id != 5 {
		t.Fatalf("latest=%d ok=%v", id, ok)
	}
	s.Prune(2)
	if _, err := s.Get(1, "t"); err == nil {
		t.Fatal("chk 1 should be gone")
	}
	if _, err := s.Get(3, "t"); err != nil {
		t.Fatal("chk 3 should be kept")
	}
}
