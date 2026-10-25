package window

import (
	"testing"

	"github.com/monaaoua9/amenah-saleh/internal/event"
)

func run(o *Operator, evs ...event.Event) (out []event.Result) {
	emit := func(r event.Result) { out = append(out, r) }
	for _, e := range evs {
		o.Add(e, emit)
	}
	o.Advance(emit)
	return
}

func TestFiresOnWatermark(t *testing.T) {
	o := New(Config{Size: 1000, Disorder: 100})
	got := run(o, event.Event{Key: "a", Value: 2, Time: 100}, event.Event{Key: "a", Value: 3, Time: 900})
	if len(got) != 0 {
		t.Fatalf("fired early: %+v", got)
	}
	// 1100 - 100 disorder = 1000 >= window end
	got = run(o, event.Event{Key: "a", Value: 1, Time: 1100})
	if len(got) != 1 || got[0].Count != 2 || got[0].Sum != 5 {
		t.Fatalf("got %+v", got)
	}
}

func TestLateDataDropped(t *testing.T) {
	o := New(Config{Size: 1000})
	run(o, event.Event{Key: "a", Value: 1, Time: 5000})
	got := run(o, event.Event{Key: "a", Value: 1, Time: 100})
	if len(got) != 0 || o.Late() != 1 {
		t.Fatalf("late=%d got=%+v", o.Late(), got)
	}
}

func TestLatenessRefires(t *testing.T) {
	o := New(Config{Size: 1000, Lateness: 5000})
	run(o, event.Event{Key: "a", Value: 1, Time: 100}, event.Event{Key: "a", Value: 1, Time: 2000})
	got := run(o, event.Event{Key: "a", Value: 4, Time: 200})
	if len(got) != 1 || !got[0].Late || got[0].Count != 2 {
		t.Fatalf("got %+v", got)
	}
}

func TestSnapshotRoundTrip(t *testing.T) {
	o := New(Config{Size: 1000})
	run(o, event.Event{Key: "a", Value: 7, Time: 10})
	b, err := o.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	o2 := New(Config{Size: 1000})
	if err := o2.Restore(b); err != nil {
		t.Fatal(err)
	}
	got := run(o2, event.Event{Key: "a", Value: 1, Time: 5000})
	if len(got) != 1 || got[0].Sum != 7 {
		t.Fatalf("got %+v", got)
	}
}

func BenchmarkAdd(b *testing.B) {
	o := New(Config{Size: 1000, Disorder: 100})
	emit := func(event.Result) {}
	e := event.Event{Key: "k", Value: 1}
	for i := 0; i < b.N; i++ {
		e.Time = int64(i)
		o.Add(e, emit)
		if i%512 == 0 {
			o.Advance(emit)
		}
	}
}

func TestSlidingAssignsToOverlappingWindows(t *testing.T) {
	o := New(Config{Size: 1000, Slide: 500})
	run(o, event.Event{Key: "a", Value: 1, Time: 700})
	got := run(o, event.Event{Key: "a", Value: 1, Time: 5000})
	// 700 sits in [0,1000) and [500,1500)
	var n int
	for _, r := range got {
		if r.Start == 0 || r.Start == 500 {
			if r.Count != 1 {
				t.Fatalf("window %d count %d", r.Start, r.Count)
			}
			n++
		}
	}
	if n != 2 {
		t.Fatalf("got %+v", got)
	}
}
