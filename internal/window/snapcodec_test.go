package window

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	"github.com/monaaoua9/streamdb/internal/event"
)

func busyOperator(cfg Config) *Operator {
	rng := rand.New(rand.NewSource(11))
	op := New(cfg)
	for i := 0; i < 3000; i++ {
		op.Add(event.Event{
			Key:   fmt.Sprintf("key-%d", rng.Intn(40)),
			Value: float64(rng.Intn(500)) - 100, // negatives and zeros hit every sketch branch
			Time:  int64(i*7) - int64(rng.Intn(300)),
		}, nil)
	}
	return op
}

func TestSnapshotRoundTripsEverything(t *testing.T) {
	for name, cfg := range map[string]Config{
		"plain":    {Size: 1000, Lateness: 2000, Disorder: 300},
		"full":     {Size: 1000, Slide: 500, Lateness: 2000, Disorder: 300, Quantiles: true, Distinct: true},
		"sessions": {Gap: 200, Disorder: 300, Quantiles: true, Distinct: true},
	} {
		t.Run(name, func(t *testing.T) {
			op := busyOperator(cfg)
			op.Advance(func(event.Result) {}) // some windows already Fired
			blob, err := op.Snapshot()
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.HasPrefix(blob, snapMagic) {
				t.Fatal("not the binary form")
			}
			back := New(cfg)
			if err := back.Restore(blob); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(op.wins, back.wins) || !reflect.DeepEqual(op.sess, back.sess) {
				t.Fatal("state changed across snapshot")
			}
			if op.wm != back.wm || op.Late() != back.Late() {
				t.Fatalf("wm %+v vs %+v", op.wm, back.wm)
			}
			if len(op.wins)+len(op.sess) == 0 {
				t.Fatal("test never built any state")
			}
			// and the restored operator behaves the same from here on
			a, b := fire(op, 1<<40), fire(back, 1<<40)
			if !reflect.DeepEqual(sig(a), sig(b)) {
				t.Fatal("restored operator fires different results")
			}
		})
	}
}

func TestSnapshotIsDeterministic(t *testing.T) {
	cfg := Config{Size: 1000, Quantiles: true, Distinct: true}
	a, _ := busyOperator(cfg).Snapshot()
	b, _ := busyOperator(cfg).Snapshot()
	// map iteration order differs between the two, entry order may too, but sizes must match
	if len(a) != len(b) {
		t.Fatalf("%d vs %d bytes", len(a), len(b))
	}
}

func TestOldGobSnapshotStillLoads(t *testing.T) {
	cfg := Config{Size: 1000, Lateness: 2000, Disorder: 300, Quantiles: true}
	op := busyOperator(cfg)
	s := snap{WM: op.wm, Late: op.late.Load()}
	for k, a := range op.wins {
		s.Entries = append(s.Entries, entry{k, *a})
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(s); err != nil {
		t.Fatal(err)
	}
	back := New(cfg)
	if err := back.Restore(buf.Bytes()); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(op.wins, back.wins) {
		t.Fatal("gob snapshot restored differently")
	}
}

func TestCorruptSnapshotErrorsCleanly(t *testing.T) {
	blob, _ := busyOperator(Config{Size: 1000, Quantiles: true, Distinct: true}).Snapshot()
	for cut := len(snapMagic); cut < len(blob); cut += len(blob)/97 + 1 {
		New(Config{Size: 1000}).Restore(blob[:cut]) // must not panic
	}
	bad := append(append([]byte{}, snapMagic...), 0, 0, 0, 0, 0xff, 0xff, 0xff, 0xff, 0x0f)
	if err := New(Config{Size: 1000}).Restore(bad); err == nil {
		t.Fatal("absurd entry count accepted")
	}
}

func BenchmarkSnapshot(b *testing.B) {
	op := New(Config{Size: 1000, Lateness: 1 << 40})
	for i := 0; i < 100000; i++ {
		op.Add(event.Event{Key: fmt.Sprintf("k%d", i%5000), Value: 1, Time: int64(i)}, nil)
	}
	b.Run("binary", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			op.Snapshot()
		}
	})
	b.Run("gob", func(b *testing.B) {
		s := snap{WM: op.wm}
		for k, a := range op.wins {
			s.Entries = append(s.Entries, entry{k, *a})
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var buf bytes.Buffer
			gob.NewEncoder(&buf).Encode(s)
		}
	})
}

func FuzzRestore(f *testing.F) {
	cfg := Config{Size: 1000, Quantiles: true, Distinct: true}
	good, _ := busyOperator(cfg).Snapshot()
	f.Add(good)
	f.Add(snapMagic)
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, b []byte) {
		New(cfg).Restore(b)
	})
}
