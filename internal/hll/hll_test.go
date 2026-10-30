package hll

import (
	"bytes"
	"encoding/gob"
	"math"
	"testing"
)

func fill(s *Sketch, from, to int) {
	for i := from; i < to; i++ {
		s.AddFloat(float64(i))
	}
}

func TestAccuracy(t *testing.T) {
	for _, n := range []int{0, 1, 10, 100, 1000, 10000, 200000} {
		s := New()
		fill(s, 0, n)
		got := float64(s.Count())
		// ~3.3% std error; allow 4 sigma, and a little absolute slack when tiny
		if math.Abs(got-float64(n)) > 0.14*float64(n)+2 {
			t.Errorf("n=%d got %v", n, got)
		}
	}
}

func TestDuplicatesDontCount(t *testing.T) {
	s := New()
	for range 50 {
		fill(s, 0, 100)
	}
	if c := s.Count(); c < 90 || c > 110 {
		t.Fatalf("got %d", c)
	}
}

func TestMergeOverlap(t *testing.T) {
	a, b := New(), New()
	fill(a, 0, 6000)
	fill(b, 3000, 9000)
	a.Merge(b)
	if c := float64(a.Count()); math.Abs(c-9000) > 0.14*9000 {
		t.Fatalf("got %v want ~9000", c)
	}
	a.Merge(nil)
}

func TestGob(t *testing.T) {
	s := New()
	fill(s, 0, 5000)
	var buf bytes.Buffer
	gob.NewEncoder(&buf).Encode(s)
	var back Sketch
	if err := gob.NewDecoder(&buf).Decode(&back); err != nil {
		t.Fatal(err)
	}
	if back.Count() != s.Count() {
		t.Fatal("changed in transit")
	}
	var nilS *Sketch
	if nilS.Count() != 0 {
		t.Fatal("nil")
	}
}
