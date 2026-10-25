package window

import (
	"bytes"
	"encoding/gob"
	"sync/atomic"

	"github.com/monaaoua9/amenah-saleh/internal/event"
)

type Config struct {
	Size     int64 // ms
	Slide    int64 // ms, 0 means tumbling (slide == size)
	Lateness int64 // ms a fired window is kept around for stragglers
	Disorder int64 // ms, feeds the watermark
}

type wkey struct {
	Key   string
	Start int64
}

type acc struct {
	Count    int64
	Sum      float64
	Min, Max float64
	Fired    bool
}

// Operator is single-threaded, one per worker.
type Operator struct {
	cfg  Config
	wins map[wkey]*acc
	wm   Watermark
	late atomic.Uint64
}

func New(cfg Config) *Operator {
	return &Operator{
		cfg:  cfg,
		wins: make(map[wkey]*acc),
		wm:   Watermark{Disorder: cfg.Disorder},
	}
}

func (o *Operator) Add(e event.Event, emit func(event.Result)) {
	size := o.cfg.Size
	slide := o.cfg.Slide
	if slide <= 0 {
		slide = size
	}
	wm := o.wm.Current()

	// newest window containing e.Time, then walk back while it still covers it
	start := e.Time - ((e.Time%slide)+slide)%slide
	taken := false
	for ; start+size > e.Time; start -= slide {
		// older windows only end earlier, so the rest are late too
		if start+size+o.cfg.Lateness <= wm {
			break
		}
		o.addTo(wkey{e.Key, start}, e, emit)
		taken = true
	}
	if !taken {
		o.late.Add(1)
	}
	o.wm.Observe(e.Time)
}

func (o *Operator) addTo(k wkey, e event.Event, emit func(event.Result)) {
	a := o.wins[k]
	if a == nil {
		a = &acc{Min: e.Value, Max: e.Value}
		o.wins[k] = a
	}
	a.Count++
	a.Sum += e.Value
	if e.Value < a.Min {
		a.Min = e.Value
	}
	if e.Value > a.Max {
		a.Max = e.Value
	}
	if a.Fired {
		r := toResult(k, a, o.cfg.Size)
		r.Late = true
		emit(r)
	}
}

// call once per batch, not per event
func (o *Operator) Advance(emit func(event.Result)) {
	wm := o.wm.Current()
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

// punctuation from outside (idle partitions, end of input)
func (o *Operator) AdvanceTo(ts int64) {
	o.wm.Observe(ts + o.cfg.Disorder)
}

func (o *Operator) Late() uint64 { return o.late.Load() }

func (o *Operator) Open() int { return len(o.wins) }

func toResult(k wkey, a *acc, size int64) event.Result {
	return event.Result{
		Key: k.Key, Start: k.Start, End: k.Start + size,
		Count: a.Count, Sum: a.Sum, Min: a.Min, Max: a.Max,
	}
}

type entry struct {
	K wkey
	A acc
}

type snap struct {
	Entries []entry
	WM      Watermark
	Late    uint64
}

func (o *Operator) Snapshot() ([]byte, error) {
	s := snap{WM: o.wm, Late: o.late.Load(), Entries: make([]entry, 0, len(o.wins))}
	for k, a := range o.wins {
		s.Entries = append(s.Entries, entry{k, *a})
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(s); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (o *Operator) Restore(b []byte) error {
	var s snap
	if err := gob.NewDecoder(bytes.NewReader(b)).Decode(&s); err != nil {
		return err
	}
	o.wins = make(map[wkey]*acc, len(s.Entries))
	for _, e := range s.Entries {
		a := e.A
		o.wins[e.K] = &a
	}
	o.wm = s.WM
	o.wm.Disorder = o.cfg.Disorder
	o.late.Store(s.Late)
	return nil
}
