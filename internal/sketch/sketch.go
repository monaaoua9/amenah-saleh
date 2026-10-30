package sketch

import (
	"math"
	"sort"
)

// DDSketch-style quantile sketch: values land in log-spaced buckets so any
// reported quantile is within Alpha (relative) of the true one. Mergeable,
// which is what window sessions and rescale need. Fields are exported only so
// gob can snapshot it.
type Sketch struct {
	Pos, Neg map[int32]uint32
	Zero     uint32
	N        uint64
}

const (
	Alpha = 0.02
	// anything with |x| below this is counted as zero
	minAbs = 1e-9
)

var (
	gamma    = (1 + Alpha) / (1 - Alpha)
	logGamma = math.Log(gamma)
)

func New() *Sketch { return &Sketch{} }

func (s *Sketch) Add(x float64) {
	if math.IsNaN(x) || math.IsInf(x, 0) {
		return
	}
	s.N++
	switch {
	case x > minAbs:
		s.Pos = bump(s.Pos, index(x))
	case x < -minAbs:
		s.Neg = bump(s.Neg, index(-x))
	default:
		s.Zero++
	}
}

func bump(m map[int32]uint32, i int32) map[int32]uint32 {
	if m == nil {
		m = make(map[int32]uint32)
	}
	m[i]++
	return m
}

func index(x float64) int32 { return int32(math.Ceil(math.Log(x) / logGamma)) }

// midpoint of bucket i, the point with the smallest worst-case relative error
func value(i int32) float64 { return 2 * math.Pow(gamma, float64(i)) / (gamma + 1) }

func (s *Sketch) Merge(o *Sketch) {
	if o == nil {
		return
	}
	for i, c := range o.Pos {
		s.Pos = addTo(s.Pos, i, c)
	}
	for i, c := range o.Neg {
		s.Neg = addTo(s.Neg, i, c)
	}
	s.Zero += o.Zero
	s.N += o.N
}

func addTo(m map[int32]uint32, i int32, c uint32) map[int32]uint32 {
	if m == nil {
		m = make(map[int32]uint32)
	}
	m[i] += c
	return m
}

// Quantile for q in [0,1]; 0 when empty.
func (s *Sketch) Quantile(q float64) float64 {
	if s == nil || s.N == 0 {
		return 0
	}
	q = math.Min(math.Max(q, 0), 1)
	rank := uint64(q * float64(s.N-1))

	// ascending order: most negative first (largest Neg index), zeros, then Pos
	neg := keys(s.Neg)
	sort.Slice(neg, func(a, b int) bool { return neg[a] > neg[b] })
	var seen uint64
	for _, i := range neg {
		seen += uint64(s.Neg[i])
		if seen > rank {
			return -value(i)
		}
	}
	seen += uint64(s.Zero)
	if seen > rank {
		return 0
	}
	pos := keys(s.Pos)
	sort.Slice(pos, func(a, b int) bool { return pos[a] < pos[b] })
	for _, i := range pos {
		seen += uint64(s.Pos[i])
		if seen > rank {
			return value(i)
		}
	}
	return 0
}

func keys(m map[int32]uint32) []int32 {
	out := make([]int32, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
