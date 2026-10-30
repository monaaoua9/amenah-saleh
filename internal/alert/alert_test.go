package alert

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/monaaoua9/streamdb/internal/event"
)

func TestParseRule(t *testing.T) {
	good := []string{"avg > 80", "count <= 3 key=cpu-", "stddev != 0", "max >= -5.5"}
	for _, s := range good {
		if _, err := ParseRule(s); err != nil {
			t.Errorf("%q: %v", s, err)
		}
	}
	bad := []string{"", "avg >", "median > 3", "avg => 3", "avg > x", "avg > 3 cpu", "avg > 3 key=", "a b c d e"}
	for _, s := range bad {
		if _, err := ParseRule(s); err == nil {
			t.Errorf("%q parsed", s)
		}
	}
}

func collect(rules []string, cd int64) (*Evaluator, *[]Alert) {
	var rs []Rule
	for _, s := range rules {
		r, err := ParseRule(s)
		if err != nil {
			panic(err)
		}
		rs = append(rs, r)
	}
	var got []Alert
	return NewEvaluator(rs, cd, func(a Alert) { got = append(got, a) }), &got
}

func TestFiresAndFilters(t *testing.T) {
	ev, got := collect([]string{"avg > 10 key=cpu"}, 0)
	ev.Write(event.Result{Key: "cpu-1", Count: 2, Sum: 30}) // avg 15
	ev.Write(event.Result{Key: "cpu-2", Count: 2, Sum: 10}) // avg 5
	ev.Write(event.Result{Key: "mem-1", Count: 1, Sum: 99}) // wrong key
	if len(*got) != 1 || (*got)[0].Result.Key != "cpu-1" || (*got)[0].Value != 15 {
		t.Fatalf("%+v", *got)
	}
}

func TestCooldownPerKey(t *testing.T) {
	ev, got := collect([]string{"max > 1"}, 1000)
	hot := func(k string, end int64) event.Result { return event.Result{Key: k, End: end, Count: 1, Max: 5} }

	ev.Write(hot("a", 1000))
	ev.Write(hot("a", 1500)) // inside cooldown
	ev.Write(hot("b", 1500)) // other key, fires
	ev.Write(hot("a", 2000)) // exactly cooldown later, fires
	if len(*got) != 3 {
		t.Fatalf("want 3 alerts, got %d: %+v", len(*got), *got)
	}
}

func TestStdDevRule(t *testing.T) {
	ev, got := collect([]string{"stddev > 1"}, 0)
	ev.Write(event.Result{Key: "k", Count: 2, Sum: 6, SumSq: 20})  // values 2,4 -> std 1
	ev.Write(event.Result{Key: "k", Count: 2, Sum: 10, SumSq: 52}) // values 4,6 -> std 1, not > 1
	ev.Write(event.Result{Key: "k", Count: 2, Sum: 10, SumSq: 58}) // values 3,7 -> std 2
	if len(*got) != 1 || (*got)[0].Value != 2 {
		t.Fatalf("%+v", *got)
	}
}

func TestHookPosts(t *testing.T) {
	got := make(chan Alert, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		var a Alert
		json.NewDecoder(r.Body).Decode(&a)
		got <- a
	}))
	defer srv.Close()

	h := NewHook(srv.URL)
	h.Notify(Alert{Rule: "max > 1", Value: 5, Result: event.Result{Key: "k"}})
	h.Close()
	select {
	case a := <-got:
		if a.Rule != "max > 1" || a.Result.Key != "k" {
			t.Fatalf("%+v", a)
		}
	default:
		t.Fatal("nothing posted")
	}
}

func TestPercentileRule(t *testing.T) {
	ev, got := collect([]string{"p99 > 250 key=api"}, 0)
	ev.Write(event.Result{Key: "api-1", P99: 300})
	ev.Write(event.Result{Key: "api-2", P99: 120})
	if len(*got) != 1 || (*got)[0].Value != 300 {
		t.Fatalf("%+v", *got)
	}
}

func TestDistinctRule(t *testing.T) {
	ev, got := collect([]string{"distinct < 3"}, 0)
	ev.Write(event.Result{Key: "a", Distinct: 2})
	ev.Write(event.Result{Key: "b", Distinct: 9})
	if len(*got) != 1 || (*got)[0].Result.Key != "a" {
		t.Fatalf("%+v", *got)
	}
}
