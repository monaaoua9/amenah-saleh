package ctl

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func serve(t *testing.T, routes map[string]string) *Client {
	t.Helper()
	mux := http.NewServeMux()
	for p, body := range routes {
		mux.HandleFunc(p, func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte(body)) })
	}
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "engine not running", http.StatusServiceUnavailable)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return New(srv.URL)
}

func TestTopAndResultsTables(t *testing.T) {
	c := serve(t, map[string]string{
		"/top":     `{"data":[{"key":"a","value":42.5,"start":0,"end":1000}]}`,
		"/results": `{"data":[{"key":"b","start":0,"end":1000,"count":2,"sum":10,"avg":5,"min":4,"max":6}]}`,
	})
	var b strings.Builder
	if err := c.Top(&b, "sum", 5); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(b.String(), "a") || !strings.Contains(b.String(), "42.5") || !strings.Contains(b.String(), "[0,1000)") {
		t.Fatalf("%s", b.String())
	}
	b.Reset()
	if err := c.Results(&b, "b", 5); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(b.String(), "COUNT") || !strings.Contains(b.String(), "b") {
		t.Fatalf("%s", b.String())
	}
}

func TestStatsAndErrors(t *testing.T) {
	c := serve(t, map[string]string{"/stats": `{"ingested":7,"p99_ms":1.5}`})
	var b strings.Builder
	if err := c.Stats(&b); err != nil || !strings.Contains(b.String(), "ingested") {
		t.Fatalf("%v %s", err, b.String())
	}
	if err := c.Ready(); err == nil {
		t.Fatal("503 should be an error")
	}
	if err := c.Top(&b, "sum", 1); err == nil {
		t.Fatal("404 should be an error")
	}
}
