package api

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/monaaoua9/amenah-saleh/internal/engine"
	"github.com/monaaoua9/amenah-saleh/internal/event"
)

type noEngine struct{}

func (noEngine) Engine() *engine.Engine { return nil }

func TestRecentWrapsAndFilters(t *testing.T) {
	r := NewRecent(3)
	for i, k := range []string{"a", "b", "a", "b", "a"} {
		r.Write(event.Result{Key: k, Start: int64(i)})
	}
	all := r.Query("", 10)
	if len(all) != 3 || all[0].Start != 4 || all[2].Start != 2 {
		t.Fatalf("%+v", all)
	}
	as := r.Query("a", 10)
	if len(as) != 2 || as[0].Start != 4 {
		t.Fatalf("%+v", as)
	}
	if got := r.Query("", 1); len(got) != 1 || got[0].Start != 4 {
		t.Fatalf("limit: %+v", got)
	}
}

func TestResultsEndpoint(t *testing.T) {
	r := NewRecent(10)
	r.Write(event.Result{Key: "k", Count: 4, Sum: 8, SumSq: 20})
	h := Handler(noEngine{}, r)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/results?key=k", nil))
	var body struct {
		Data []resultView `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if len(body.Data) != 1 || body.Data[0].Avg != 2 || body.Data[0].StdDev != 1 {
		t.Fatalf("%s", rec.Body)
	}

	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/results?limit=abc", nil))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("code %d", rec.Code)
	}
}

func TestReadyzWithoutEngine(t *testing.T) {
	h := Handler(noEngine{}, NewRecent(1))
	for path, want := range map[string]int{"/healthz": 200, "/readyz": 503, "/stats": 503} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest("GET", path, nil))
		if rec.Code != want {
			t.Errorf("%s: %d, want %d", path, rec.Code, want)
		}
	}
}

func TestTopUsesNewestWindowPerKey(t *testing.T) {
	r := NewRecent(10)
	r.Write(event.Result{Key: "a", Start: 0, Count: 1, Sum: 100})
	r.Write(event.Result{Key: "b", Start: 0, Count: 1, Sum: 7})
	r.Write(event.Result{Key: "a", Start: 1000, Count: 1, Sum: 3}) // supersedes the 100
	r.Write(event.Result{Key: "c", Start: 0, Count: 1, Sum: 5})

	got, ok := r.Top("sum", 2)
	if !ok || len(got) != 2 || got[0].Result.Key != "b" || got[1].Result.Key != "c" {
		t.Fatalf("%+v", got)
	}
	if _, ok := r.Top("median", 3); ok {
		t.Fatal("unknown metric accepted")
	}
}

func TestTopEndpoint(t *testing.T) {
	r := NewRecent(10)
	r.Write(event.Result{Key: "x", Count: 2, Sum: 10})
	r.Write(event.Result{Key: "y", Count: 1, Sum: 9})
	h := Handler(noEngine{}, r)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/top?by=avg&n=1", nil))
	var body struct {
		Data []struct {
			Key   string
			Value float64
		}
	}
	json.Unmarshal(rec.Body.Bytes(), &body)
	if len(body.Data) != 1 || body.Data[0].Key != "y" || body.Data[0].Value != 9 {
		t.Fatalf("%s", rec.Body)
	}
	for _, u := range []string{"/top?by=nope", "/top?n=0"} {
		rec = httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest("GET", u, nil))
		if rec.Code != 400 {
			t.Errorf("%s -> %d", u, rec.Code)
		}
	}
}

func TestTopByPercentile(t *testing.T) {
	r := NewRecent(5)
	r.Write(event.Result{Key: "a", P99: 10})
	r.Write(event.Result{Key: "b", P99: 40})
	got, ok := r.Top("p99", 1)
	if !ok || got[0].Result.Key != "b" {
		t.Fatalf("%+v", got)
	}
}

func TestMetricsAppendsExtras(t *testing.T) {
	// no engine, so only the extras show up
	h := Handler(noEngine{}, NewRecent(1), func(w io.Writer) { io.WriteString(w, "extra_metric 1\n") })
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	if !strings.Contains(rec.Body.String(), "extra_metric 1") {
		t.Fatalf("%q", rec.Body)
	}
}
