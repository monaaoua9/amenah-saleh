package partition

import "sync/atomic"

type Partitioner interface {
	Pick(key string, n int) int
}

// Keyed partitioners map a key to a stable owner, which is what lets state be
// re-split when the worker count changes.
type Keyed interface {
	Partitioner
	Owner(key string, n int) int
}

// fixed number of key groups; workers own contiguous ranges of them
const KeyGroups = 128

func KeyGroup(key string) int { return int(Hash64(key) % KeyGroups) }

type Hash struct{}

func (h Hash) Pick(key string, n int) int { return h.Owner(key, n) }

func (Hash) Owner(key string, n int) int {
	return KeyGroup(key) * n / KeyGroups
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
