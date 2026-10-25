package metrics

import (
	"math"
	"math/bits"
	"sync/atomic"
	"time"
)

// log2 buckets with 4 sub-buckets each, ~25% error on quantiles. lock-free.
type Histogram struct {
	b [256]atomic.Uint64
	n atomic.Uint64
}

func (h *Histogram) Observe(d time.Duration) {
	ns := uint64(max(d, 0))
	var idx int
	if ns < 4 {
		idx = int(ns)
	} else {
		l := bits.Len64(ns) - 1
		idx = l*4 + int((ns>>(l-2))&3)
	}
	h.b[idx].Add(1)
	h.n.Add(1)
}

func (h *Histogram) Count() uint64 { return h.n.Load() }

func (h *Histogram) Quantile(q float64) time.Duration {
	total := h.n.Load()
	if total == 0 {
		return 0
	}
	target := uint64(math.Ceil(q * float64(total)))
	var cum uint64
	for i := range h.b {
		cum += h.b[i].Load()
		if cum >= target {
			if i < 4 {
				return time.Duration(i)
			}
			l, sub := i/4, uint64(i%4)
			return time.Duration((4 + sub) << (l - 2))
		}
	}
	return 0
}
