package api

import (
	"sort"
	"sync"

	"github.com/monaaoua9/streamdb/internal/event"
)

// Fixed-size ring of the latest emitted results.
type Recent struct {
	mu   sync.RWMutex
	buf  []event.Result
	next int
	full bool
}

func NewRecent(n int) *Recent { return &Recent{buf: make([]event.Result, n)} }

func (r *Recent) Write(x event.Result) {
	r.mu.Lock()
	r.buf[r.next] = x
	r.next++
	if r.next == len(r.buf) {
		r.next, r.full = 0, true
	}
	r.mu.Unlock()
}

// newest first; key == "" matches everything
func (r *Recent) Query(key string, limit int) []event.Result {
	r.mu.RLock()
	defer r.mu.RUnlock()

	n := r.next
	if r.full {
		n = len(r.buf)
	}
	var out []event.Result
	for i := 1; i <= n && len(out) < limit; i++ {
		x := r.buf[(r.next-i+len(r.buf))%len(r.buf)]
		if key == "" || x.Key == key {
			out = append(out, x)
		}
	}
	return out
}

type Ranked struct {
	Result event.Result
	Value  float64
}

// Top ranks keys by the metric of their newest window in the ring. ok is false
// for an unknown metric name.
func (r *Recent) Top(metric string, n int) (out []Ranked, ok bool) {
	if _, ok := (event.Result{}).Metric(metric); !ok {
		return nil, false
	}
	r.mu.RLock()
	size := r.next
	if r.full {
		size = len(r.buf)
	}
	newest := map[string]event.Result{}
	for i := 1; i <= size; i++ { // newest first, so the first sighting of a key wins
		x := r.buf[(r.next-i+len(r.buf))%len(r.buf)]
		if _, seen := newest[x.Key]; !seen {
			newest[x.Key] = x
		}
	}
	r.mu.RUnlock()

	for _, x := range newest {
		v, _ := x.Metric(metric)
		out = append(out, Ranked{x, v})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Value != out[j].Value {
			return out[i].Value > out[j].Value
		}
		return out[i].Result.Key < out[j].Result.Key
	})
	if len(out) > n {
		out = out[:n]
	}
	return out, true
}
