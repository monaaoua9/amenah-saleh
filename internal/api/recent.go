package api

import (
	"sync"

	"github.com/monaaoua9/amenah-saleh/internal/event"
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
