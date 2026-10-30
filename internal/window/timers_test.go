package window

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/monaaoua9/streamdb/internal/event"
)

// the pre-heap Advance, kept as the oracle
func (o *Operator) naiveAdvance(emit func(event.Result)) {
	wm := o.wm.Current()
	if o.cfg.Gap > 0 {
		for k, list := range o.sess {
			keep := list[:0]
			for _, s := range list {
				if s.End > wm {
					keep = append(keep, s)
					continue
				}
				emit(event.Result{Key: k, Start: s.Start, End: s.End, Count: s.A.Count, Sum: s.A.Sum})
			}
			if len(keep) == 0 {
				delete(o.sess, k)
			} else {
				o.sess[k] = keep
			}
		}
		return
	}
	for k, a := range o.wins {
		end := k.Start + o.cfg.Size
		if !a.Fired && end <= wm {
			emit(toResult(k, a, o.cfg.Size))
			a.Fired = true
		}
		if a.Fired && end+o.cfg.Lateness <= wm {
			delete(o.wins, k)
		}
	}
}

func sig(rs []event.Result) []string {
	out := make([]string, len(rs))
	for i, r := range rs {
		out[i] = fmt.Sprintf("%s|%d|%d|%d|%g|%v", r.Key, r.Start, r.End, r.Count, r.Sum, r.Late)
	}
	sort.Strings(out)
	return out
}

func TestHeapAdvanceMatchesFullScan(t *testing.T) {
	cfgs := map[string]Config{
		"tumbling":         {Size: 1000, Disorder: 300},
		"tumbling+latency": {Size: 1000, Disorder: 300, Lateness: 1500},
		"sliding":          {Size: 2000, Slide: 500, Disorder: 200, Lateness: 700},
		"session":          {Gap: 400, Disorder: 300},
	}
	for name, cfg := range cfgs {
		t.Run(name, func(t *testing.T) {
			rng := rand.New(rand.NewSource(7))
			fast, slow := New(cfg), New(cfg)
			var gotF, gotS []event.Result
			now := int64(0)
			for step := 0; step < 400; step++ {
				for i := 0; i < 1+rng.Intn(20); i++ {
					now += int64(rng.Intn(60))
					e := event.Event{
						Key:   fmt.Sprintf("k%d", rng.Intn(30)),
						Value: float64(rng.Intn(10)),
						Time:  now - int64(rng.Intn(800)), // out of order
					}
					fast.Add(e, func(r event.Result) { gotF = append(gotF, r) })
					slow.Add(e, func(r event.Result) { gotS = append(gotS, r) })
				}
				if step%50 == 49 { // punctuation, like an idle partition catching up
					fast.AdvanceTo(now + 3000)
					slow.AdvanceTo(now + 3000)
				}
				fast.Advance(func(r event.Result) { gotF = append(gotF, r) })
				slow.naiveAdvance(func(r event.Result) { gotS = append(gotS, r) })

				if fast.Open() != slow.Open() {
					t.Fatalf("step %d: open %d vs %d", step, fast.Open(), slow.Open())
				}
			}
			a, b := sig(gotF), sig(gotS)
			if len(a) != len(b) || len(a) == 0 {
				t.Fatalf("emitted %d vs %d", len(a), len(b))
			}
			for i := range a {
				if a[i] != b[i] {
					t.Fatalf("result %d: %s vs %s", i, a[i], b[i])
				}
			}
		})
	}
}

func TestTimersRebuiltAfterRestore(t *testing.T) {
	for _, cfg := range []Config{{Size: 1000, Lateness: 500}, {Gap: 300, Disorder: 100}} {
		op := New(cfg)
		for i := 0; i < 50; i++ {
			op.Add(event.Event{Key: fmt.Sprintf("k%d", i%5), Value: 1, Time: int64(i * 40)}, nil)
		}
		b, err := op.Snapshot()
		if err != nil {
			t.Fatal(err)
		}
		op2 := New(cfg)
		if err := op2.Restore(b); err != nil {
			t.Fatal(err)
		}
		want := fire(op, 100000)
		got := fire(op2, 100000)
		if len(want) == 0 || len(sig(got)) != len(sig(want)) {
			t.Fatalf("cfg %+v: restored fired %d, original %d", cfg, len(got), len(want))
		}
		if op2.Open() != 0 {
			t.Fatalf("cfg %+v: %d still open", cfg, op2.Open())
		}
	}
}

func TestBusyKeyKeepsHeapSmall(t *testing.T) {
	op := New(Config{Gap: 1000})
	for i := 0; i < 10000; i++ {
		op.Add(event.Event{Key: "hot", Value: 1, Time: int64(i)}, nil)
	}
	if n := len(op.sessTimers); n > 5 {
		t.Fatalf("%d heap entries for one key", n)
	}
}

func benchAdvance(b *testing.B, adv func(*Operator, func(event.Result))) {
	op := New(Config{Size: 1000, Lateness: 100000})
	for i := 0; i < 200000; i++ { // 200k open windows, all far from firing
		op.Add(event.Event{Key: fmt.Sprintf("k%d", i), Value: 1, Time: 1 << 40}, nil)
	}
	emit := func(event.Result) {}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		adv(op, emit)
	}
}

func BenchmarkAdvanceHeap200kOpen(b *testing.B) {
	benchAdvance(b, func(o *Operator, e func(event.Result)) { o.Advance(e) })
}

func BenchmarkAdvanceFullScan200kOpen(b *testing.B) {
	benchAdvance(b, func(o *Operator, e func(event.Result)) { o.naiveAdvance(e) })
}
