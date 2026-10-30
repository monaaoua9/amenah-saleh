package output

import (
	"bufio"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/monaaoua9/streamdb/internal/event"
)

func readLines(t *testing.T, p string) []event.Result {
	t.Helper()
	fh, err := os.Open(p)
	if err != nil {
		t.Fatal(err)
	}
	defer fh.Close()
	var out []event.Result
	sc := bufio.NewScanner(fh)
	for sc.Scan() {
		var r event.Result
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			t.Fatal(err)
		}
		out = append(out, r)
	}
	return out
}

func TestFileRolls(t *testing.T) {
	p := filepath.Join(t.TempDir(), "out.jsonl")
	f, err := NewFile(p, 300)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		f.Write(event.Result{Key: "k", Start: int64(i), Count: 1})
	}
	f.Close()

	total := len(readLines(t, p))
	for i := 1; ; i++ {
		q := p + "." + string(rune('0'+i))
		if _, err := os.Stat(q); err != nil {
			if i == 1 {
				t.Fatal("never rolled")
			}
			break
		}
		total += len(readLines(t, q))
	}
	if total != 20 {
		t.Fatalf("lost lines: %d of 20", total)
	}
}

func TestFileAppendsAcrossRestart(t *testing.T) {
	p := filepath.Join(t.TempDir(), "o.jsonl")
	for range 2 {
		f, _ := NewFile(p, 0)
		f.Write(event.Result{Key: "a"})
		f.Close()
	}
	if n := len(readLines(t, p)); n != 2 {
		t.Fatalf("got %d lines", n)
	}
}

func TestWebhookBatchesAndRetries(t *testing.T) {
	var calls, got atomic.Int32
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		var b []event.Result
		json.NewDecoder(r.Body).Decode(&b)
		mu.Lock()
		got.Add(int32(len(b)))
		mu.Unlock()
	}))
	defer srv.Close()

	w := NewWebhook(srv.URL, 10, 20*time.Millisecond)
	for i := 0; i < 25; i++ {
		w.Write(event.Result{Key: "k", Start: int64(i)})
	}
	w.Close()
	if got.Load() != 25 {
		t.Fatalf("receiver got %d of 25", got.Load())
	}
	if w.Failed() != 0 {
		t.Fatal("batch dropped")
	}
}

func TestWebhookDropsOn4xx(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()
	w := NewWebhook(srv.URL, 10, time.Hour)
	w.Write(event.Result{Key: "x"})
	w.Close()
	if calls.Load() != 1 || w.Failed() != 1 {
		t.Fatalf("calls=%d failed=%d", calls.Load(), w.Failed())
	}
}

func TestFileFlushesWithoutClose(t *testing.T) {
	p := filepath.Join(t.TempDir(), "o.jsonl")
	f, _ := NewFile(p, 0)
	defer f.Close()
	f.Write(event.Result{Key: "a"})
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if st, err := os.Stat(p); err == nil && st.Size() > 0 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("result never reached the file")
}

func TestWebhookMetrics(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer srv.Close()
	w := NewWebhook(srv.URL, 10, time.Hour)
	for i := 0; i < 4; i++ {
		w.Write(event.Result{Key: "k"})
	}
	w.Close()
	var b strings.Builder
	w.WriteMetrics(&b)
	if !strings.Contains(b.String(), "dspe_webhook_results_sent_total 4") || !strings.Contains(b.String(), "dspe_webhook_queued 0") {
		t.Fatal(b.String())
	}
}
