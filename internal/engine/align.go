package engine

// Barrier alignment over a shared channel. Once a sender's barrier shows up,
// everything else from that sender is parked until the rest catch up, so the
// snapshot never mixes pre- and post-barrier records.
type aligner[M any] struct {
	n      int
	active bool
	seen   []bool
	count  int
	held   []M
}

func newAligner[M any](n int) *aligner[M] {
	return &aligner[M]{n: n, seen: make([]bool, n)}
}

func (a *aligner[M]) blocked(from int) bool { return a.active && a.seen[from] }

func (a *aligner[M]) hold(m M) { a.held = append(a.held, m) }

func (a *aligner[M]) arrive(from int) (complete bool) {
	a.active = true
	a.seen[from] = true
	a.count++
	return a.count == a.n
}

func (a *aligner[M]) release() []M {
	h := a.held
	a.held = nil
	a.active = false
	a.count = 0
	clear(a.seen)
	return h
}
