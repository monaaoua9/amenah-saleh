package partition

import "sync/atomic"

type Partitioner interface {
	Pick(key string, n int) int
}

type Hash struct{}

func (Hash) Pick(key string, n int) int {
	return int(Hash64(key) % uint64(n))
}

// RoundRobin ignores the key, so keyed windows end up split across workers.
// Only useful for stateless stages.
type RoundRobin struct{ c atomic.Uint64 }

func (r *RoundRobin) Pick(_ string, n int) int {
	return int(r.c.Add(1) % uint64(n))
}

// fnv-1a, inlined so we don't allocate a hasher per event
func Hash64(s string) uint64 {
	h := uint64(14695981039346656037)
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= 1099511628211
	}
	return h
}
