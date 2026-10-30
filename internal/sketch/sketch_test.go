package sketch

import (
	"bytes"
	"encoding/gob"
	"math"
	"math/rand"
	"sort"
	"testing"
)

func exact(xs []float64, q float64) float64 {
	s := append([]float64(nil), xs...)
	sort.Float64s(s)
	return s[int(q*float64(len(s)-1))]
}

func withinRelErr(got, want float64) bool {
	if want == 0 {
		return math.Abs(got) < 1e-6
	}
	return math.Abs(got-want) <= Alpha*math.Abs(want)*1.01
}

func TestQuantileAccuracy(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	var xs []float64
	s := New()
	for i := 0; i < 50000; i++ {
		x := math.Exp(rng.NormFloat64()*2) + 0.001 // heavy right tail
		xs = append(xs, x)
		s.Add(x)
	}
	for _, q := range []float64{0, 0.1, 0.5, 0.9, 0.95, 0.99, 1} {
		if g, w := s.Quantile(q), exact(xs, q); !withinRelErr(g, w) {
			t.Errorf("q=%v got %v want %v", q, g, w)
		}
	}
}

func TestNegativesAndZeros(t *testing.T) {
	s := New()
	xs := []float64{-100, -10, -1, 0, 0, 1, 10, 100}
	for _, x := range xs {
		s.Add(x)
	}
	for _, q := range []float64{0, 0.25, 0.5, 0.75, 1} {
		if g, w := s.Quantile(q), exact(xs, q); !withinRelErr(g, w) {
			t.Errorf("q=%v got %v want %v", q, g, w)
		}
	}
}

func TestMergeEqualsSingle(t *testing.T) {
	rng := rand.New(rand.NewSource(2))
	a, b, all := New(), New(), New()
	for i := 0; i < 10000; i++ {
		x := rng.Float64()*200 - 50
		all.Add(x)
		if i%3 == 0 {
			a.Add(x)
		} else {
			b.Add(x)
		}
	}
	a.Merge(b)
	for _, q := range []float64{0.01, 0.5, 0.99} {
		if a.Quantile(q) != all.Quantile(q) {
			t.Errorf("q=%v merged %v single %v", q, a.Quantile(q), all.Quantile(q))
		}
	}
	a.Merge(nil)
}

func TestEmptyAndJunk(t *testing.T) {
	s := New()
	if s.Quantile(0.5) != 0 {
		t.Fatal("empty sketch")
	}
	s.Add(math.NaN())
	s.Add(math.Inf(1))
	if s.N != 0 {
		t.Fatalf("junk counted: %d", s.N)
	}
	var nilS *Sketch
	if nilS.Quantile(0.9) != 0 {
		t.Fatal("nil sketch")
	}
}

func TestGobRoundTrip(t *testing.T) {
	s := New()
	for i := 1; i <= 1000; i++ {
		s.Add(float64(i))
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(s); err != nil {
		t.Fatal(err)
	}
	var back Sketch
	if err := gob.NewDecoder(&buf).Decode(&back); err != nil {
		t.Fatal(err)
	}
	if back.Quantile(0.5) != s.Quantile(0.5) || back.N != 1000 {
		t.Fatalf("%v vs %v", back.Quantile(0.5), s.Quantile(0.5))
	}
}

func BenchmarkAdd(b *testing.B) {
	s := New()
	for i := 0; i < b.N; i++ {
		s.Add(float64(i%5000) + 0.5)
	}
}
