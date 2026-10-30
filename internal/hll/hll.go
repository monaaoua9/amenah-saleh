package hll

import (
	"math"
	"math/bits"
)

// HyperLogLog with 2^P one-byte registers (1 KiB at P=10, ~3.3% standard
// error). Mergeable by register-wise max. R is exported for gob.
type Sketch struct {
	R []uint8
}

const (
	P = 10
	m = 1 << P
)

func New() *Sketch { return &Sketch{R: make([]uint8, m)} }

// fnv-style hashes cluster badly in the low bits; run a splitmix finalizer first
func mix(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}

func (s *Sketch) AddHash(h uint64) {
	h = mix(h)
	i := h >> (64 - P)
	rest := h<<P | 1<<(P-1) // sentinel bit caps the run length
	rho := uint8(bits.LeadingZeros64(rest)) + 1
	if rho > s.R[i] {
		s.R[i] = rho
	}
}

// AddFloat counts distinct float64 values by bit pattern (so -0 and 0 differ, NaNs collapse by payload).
func (s *Sketch) AddFloat(v float64) { s.AddHash(math.Float64bits(v)) }

func (s *Sketch) Merge(o *Sketch) {
	if o == nil || len(o.R) != len(s.R) {
		return
	}
	for i, r := range o.R {
		if r > s.R[i] {
			s.R[i] = r
		}
	}
}

func (s *Sketch) Count() uint64 {
	if s == nil || len(s.R) != m {
		return 0
	}
	var sum float64
	zeros := 0
	for _, r := range s.R {
		sum += math.Ldexp(1, -int(r))
		if r == 0 {
			zeros++
		}
	}
	alpha := 0.7213 / (1 + 1.079/float64(m))
	est := alpha * m * m / sum
	if est <= 2.5*m && zeros > 0 {
		est = m * math.Log(float64(m)/float64(zeros)) // linear counting
	}
	return uint64(est + 0.5)
}
