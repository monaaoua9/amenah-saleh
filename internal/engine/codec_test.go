package engine

import (
	"bytes"
	"encoding/gob"
	"math"
	"math/rand"
	"reflect"
	"testing"

	"github.com/monaaoua9/streamdb/internal/event"
)

func sample(rng *rand.Rand, n int) []event.Result {
	out := make([]event.Result, n)
	for i := range out {
		r := event.Result{
			Key: string(rune('a'+rng.Intn(26))) + "-key", Start: rng.Int63n(1<<40) - 1<<39, Count: rng.Int63n(1000),
			Sum: rng.NormFloat64(), SumSq: rng.Float64(), Min: -rng.Float64(), Max: rng.Float64(),
			Late: rng.Intn(4) == 0,
		}
		r.End = r.Start + 1000
		if rng.Intn(2) == 0 {
			r.P50, r.P95, r.P99 = rng.Float64(), rng.Float64(), rng.Float64()
		}
		if rng.Intn(2) == 0 {
			r.Distinct = uint64(rng.Intn(5000))
		}
		out[i] = r
	}
	return out
}

func TestResultsRoundTrip(t *testing.T) {
	rng := rand.New(rand.NewSource(3))
	chunks := [][]event.Result{sample(rng, 40), nil, sample(rng, 1), sample(rng, 300)}
	b, err := encodeResults(chunks)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeResults(b)
	if err != nil {
		t.Fatal(err)
	}
	var want []event.Result
	for _, c := range chunks {
		want = append(want, c...)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatal("round trip changed the results")
	}
}

func TestSpecialFloatsSurvive(t *testing.T) {
	in := [][]event.Result{{{Key: "k", Sum: math.Inf(1), Min: math.Inf(-1), Max: math.MaxFloat64, SumSq: math.SmallestNonzeroFloat64}}}
	b, _ := encodeResults(in)
	got, err := decodeResults(b)
	if err != nil || got[0] != in[0][0] {
		t.Fatalf("%v %+v", err, got)
	}
}

func TestEmptyAndOldGobFormat(t *testing.T) {
	b, _ := encodeResults(nil)
	if got, err := decodeResults(b); err != nil || len(got) != 0 {
		t.Fatalf("%v %v", err, got)
	}

	// a staged sink written by the previous version
	old := [][]event.Result{{{Key: "x", Start: 5, End: 1005, Count: 3, Sum: 9}}}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(old); err != nil {
		t.Fatal(err)
	}
	got, err := decodeResults(buf.Bytes())
	if err != nil || len(got) != 1 || got[0] != old[0][0] {
		t.Fatalf("%v %+v", err, got)
	}
}

func TestCorruptInputIsAnErrorNotAPanic(t *testing.T) {
	b, _ := encodeResults([][]event.Result{sample(rand.New(rand.NewSource(1)), 20)})
	for cut := 0; cut < len(b); cut += 7 {
		decodeResults(b[:cut]) // must not panic
	}
	if _, err := decodeResults(append([]byte("DSR1"), 0xff, 0xff, 0xff)); err == nil {
		t.Fatal("garbage accepted")
	}
	huge := append([]byte("DSR1"), 1, 0xff, 0xff, 0xff, 0xff, 0x0f) // claims 4 billion results
	if _, err := decodeResults(huge); err == nil {
		t.Fatal("absurd count accepted")
	}
}

func BenchmarkEncodeResults(b *testing.B) {
	chunks := [][]event.Result{sample(rand.New(rand.NewSource(1)), 10000)}
	b.Run("binary", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			encodeResults(chunks)
		}
	})
	b.Run("gob", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			var buf bytes.Buffer
			gob.NewEncoder(&buf).Encode(chunks)
		}
	})
}

func FuzzDecodeResults(f *testing.F) {
	good, _ := encodeResults([][]event.Result{sample(rand.New(rand.NewSource(5)), 5)})
	f.Add(good)
	f.Add([]byte("DSR1"))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, b []byte) {
		decodeResults(b)
	})
}
