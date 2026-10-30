package ingest

import (
	"sync"
	"time"
)

// Token bucket in events/sec. Tokens may go negative: a batch bigger than the
// burst is let through and the debt is paid by delaying whoever comes next.
type bucket struct {
	mu     sync.Mutex
	rate   float64
	burst  float64
	tokens float64
	last   time.Time
	now    func() time.Time
}

func newBucket(rate float64, burst int) *bucket {
	b := &bucket{rate: rate, burst: float64(burst), tokens: float64(burst), now: time.Now}
	b.last = b.now()
	return b
}

// take removes n tokens and says how long the caller should wait before using them.
func (b *bucket) take(n int) time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()
	t := b.now()
	b.tokens = min(b.burst, b.tokens+t.Sub(b.last).Seconds()*b.rate)
	b.last = t
	b.tokens -= float64(n)
	if b.tokens >= 0 {
		return 0
	}
	return time.Duration(-b.tokens / b.rate * float64(time.Second))
}

// refund puts tokens back, for a batch that was rejected after being counted
func (b *bucket) refund(n int) {
	b.mu.Lock()
	b.tokens = min(b.burst, b.tokens+float64(n))
	b.mu.Unlock()
}

type limiter struct {
	rate  float64
	burst int
	m     sync.Map // producer -> *bucket
}

func (l *limiter) bucket(producer string) *bucket {
	if b, ok := l.m.Load(producer); ok {
		return b.(*bucket)
	}
	b, _ := l.m.LoadOrStore(producer, newBucket(l.rate, l.burst))
	return b.(*bucket)
}
