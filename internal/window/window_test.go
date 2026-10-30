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

func TestSessionMergesAndFires(t *testing.T) {
	o := New(Config{Gap: 1000, Disorder: 10000})
	run(o,
		event.Event{Key: "a", Value: 1, Time: 0},
		event.Event{Key: "a", Value: 2, Time: 2000}, // separate: 0+1000 < 2000
	)
	if o.Open() != 2 {
		t.Fatalf("open = %d, want 2", o.Open())
	}
	run(o, event.Event{Key: "a", Value: 4, Time: 1000}) // bridges both
	if o.Open() != 1 {
		t.Fatalf("open = %d after bridge, want 1", o.Open())
	}

	o.AdvanceTo(100000)
	var got []event.Result
	o.Advance(func(r event.Result) { got = append(got, r) })
	if len(got) != 1 || got[0].Count != 3 || got[0].Sum != 7 || got[0].Start != 0 || got[0].End != 3000 {
		t.Fatalf("got %+v", got)
	}
}

func TestSessionSnapshot(t *testing.T) {
	o := New(Config{Gap: 1000})
	run(o, event.Event{Key: "a", Value: 5, Time: 100})
	b, err := o.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	o2 := New(Config{Gap: 1000})
	if err := o2.Restore(b); err != nil {
		t.Fatal(err)
	}
	o2.AdvanceTo(50000)
	var got []event.Result
	o2.Advance(func(r event.Result) { got = append(got, r) })
	if len(got) != 1 || got[0].Sum != 5 {
		t.Fatalf("got %+v", got)
	}
}

func TestPoisonTimestampDoesNotKillWatermark(t *testing.T) {
	o := New(Config{Size: 1000})
	run(o, event.Event{Key: "a", Value: 1, Time: 100})
	run(o, event.Event{Key: "a", Value: 1, Time: 1<<63 - 1})
	run(o, event.Event{Key: "a", Value: 1, Time: 200})
	if o.Late() != 1 {
		t.Fatalf("late = %d, want only the poison event", o.Late())
	}
	o.AdvanceTo(5000)
	var got []event.Result
	o.Advance(func(r event.Result) { got = append(got, r) })
	if len(got) != 1 || got[0].Count != 2 {
		t.Fatalf("got %+v", got)
	}
}

func TestMaxJumpDropsFarFutureEvents(t *testing.T) {
	o := New(Config{Size: 1000, MaxJump: 10000})
	run(o, event.Event{Key: "a", Value: 1, Time: 100})
	run(o, event.Event{Key: "a", Value: 1, Time: 1_000_000})
	if o.Late() != 1 {
		t.Fatalf("late = %d", o.Late())
	}
}

func TestValidate(t *testing.T) {
	for _, c := range []Config{{}, {Size: -1}, {Size: 10, Slide: -5}, {Size: 10, Lateness: -1}} {
		if c.Validate() == nil {
			t.Fatalf("%+v should be invalid", c)
		}
	}
	for _, c := range []Config{{Size: 10}, {Size: 10, Slide: 5}, {Gap: 100}} {
		if err := c.Validate(); err != nil {
			t.Fatalf("%+v: %v", c, err)
		}
	}
}

func TestAvgAndStdDev(t *testing.T) {
	op := New(Config{Size: 1000})
	var got []event.Result
	for i, v := range []float64{2, 4, 4, 4, 5, 5, 7, 9} {
		op.Add(event.Event{Key: "k", Value: v, Time: int64(i)}, nil)
	}
	op.AdvanceTo(5000)
	op.Advance(func(r event.Result) { got = append(got, r) })
	if len(got) != 1 {
		t.Fatalf("want 1 result, got %d", len(got))
	}
	if a, s := got[0].Avg(), got[0].StdDev(); a != 5 || s != 2 {
		t.Fatalf("avg=%v std=%v", a, s)
	}
}

func TestSessionMergeKeepsSumSq(t *testing.T) {
	op := New(Config{Gap: 100, Disorder: 500})
	for _, e := range []event.Event{{Key: "k", Value: 3, Time: 0}, {Key: "k", Value: 4, Time: 200}, {Key: "k", Value: 5, Time: 100}} {
		op.Add(e, nil)
	}
	op.AdvanceTo(10000)
	var got []event.Result
	op.Advance(func(r event.Result) { got = append(got, r) })
	if len(got) != 1 || got[0].SumSq != 50 {
		t.Fatalf("%+v", got)
	}
}
