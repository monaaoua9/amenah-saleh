package window

import (
	"bytes"
	"encoding/gob"
	"errors"
	"sync/atomic"

	"github.com/monaaoua9/amenah-saleh/internal/event"
	"github.com/monaaoua9/amenah-saleh/internal/sketch"
)

type Config struct {
	Size      int64 // ms
	Slide     int64 // ms, 0 means tumbling (slide == size)
	Gap       int64 // ms of silence that closes a session; > 0 switches to session windows
	Lateness  int64 // ms a fired window is kept around for stragglers
	Disorder  int64 // ms, feeds the watermark
	MaxJump   int64 // ms, an event further ahead of the watermark base than this is dropped; 0 = unlimited
	Quantiles bool  // keep a sketch per window and report p50/p95/p99 (costs memory and some throughput)
}

// anything outside this is a bad clock or a hostile producer; it would also
// overflow the window arithmetic
const maxEventTime = int64(1) << 60

func (c Config) Validate() error {
	switch {
	case c.Gap < 0 || c.Slide < 0 || c.Lateness < 0 || c.Disorder < 0 || c.MaxJump < 0:
		return errors.New("window: negative duration")
	case c.Gap == 0 && c.Size <= 0:
		return errors.New("window: size must be > 0")
	}
	return nil
}

func (o *Operator) implausible(t int64) bool {
	if t > maxEventTime || t < -maxEventTime {
		return true
	}
	return o.cfg.MaxJump > 0 && o.wm.Init && t > o.wm.Max+o.cfg.MaxJump
}

type wkey struct {
	Key   string
	Start int64
}

type acc struct {
	Count    int64
	Sum      float64
	SumSq    float64
	Min, Max float64
	Fired    bool
	Q        *sketch.Sketch
}

// Operator is single-threaded, one per worker.
type Operator struct {
	cfg  Config
	wins map[wkey]*acc
	sess map[string][]*session
	wm   Watermark
	late atomic.Uint64

	// sees every event counted as late, including implausible timestamps.
	// Runs on the worker goroutine; set before the first Add.
	OnLate func(event.Event)
}

func (o *Operator) drop(e event.Event) {
	o.late.Add(1)
	if o.OnLate != nil {
		o.OnLate(e)
	}
}

func New(cfg Config) *Operator {
	return &Operator{
		cfg:  cfg,
		wins: make(map[wkey]*acc),
		sess: make(map[string][]*session),
		wm:   Watermark{Disorder: cfg.Disorder},
	}
}

func (o *Operator) Add(e event.Event, emit func(event.Result)) {
	if o.implausible(e.Time) {
		o.drop(e)
		return
	}
	if o.cfg.Gap > 0 {
		o.addSession(e)
		return
	}
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
		o.drop(e)
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
	a.SumSq += e.Value * e.Value
	if o.cfg.Quantiles {
		if a.Q == nil {
			a.Q = sketch.New()
		}
		a.Q.Add(e.Value)
	}
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
	if o.cfg.Gap > 0 {
		o.advanceSessions(emit)
		return
	}
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

func (o *Operator) Open() int {
	n := len(o.wins)
	for _, l := range o.sess {
		n += len(l)
	}
	return n
}

func toResult(k wkey, a *acc, size int64) event.Result {
	r := event.Result{
		Key: k.Key, Start: k.Start, End: k.Start + size,
		Count: a.Count, Sum: a.Sum, SumSq: a.SumSq, Min: a.Min, Max: a.Max,
	}
	r.P50, r.P95, r.P99 = quantiles(a)
	return r
}

type entry struct {
	K wkey
	A acc
}

type snap struct {
	Entries []entry
	Sess    []sessEntry
	WM      Watermark
	Late    uint64
}

func (o *Operator) Snapshot() ([]byte, error) {
	s := snap{WM: o.wm, Late: o.late.Load(), Entries: make([]entry, 0, len(o.wins))}
	for k, a := range o.wins {
		s.Entries = append(s.Entries, entry{k, *a})
	}
	for k, l := range o.sess {
		for _, x := range l {
			s.Sess = append(s.Sess, sessEntry{k, *x})
		}
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(s); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (o *Operator) Restore(b []byte) error {
	return o.RestoreMerge([][]byte{b}, nil)
}

// RestoreMerge rebuilds state from several snapshots, keeping only keys that
// keep() accepts (nil keeps all). This is the rescale path: a new worker pulls
// its share out of every old worker's snapshot.
// The watermark comes back as the slowest snapshot's, so it never runs ahead
// of data we might still receive. Costs: after a rescale a very late event
// can be accepted that an unscaled run would have dropped.
func (o *Operator) RestoreMerge(blobs [][]byte, keep func(key string) bool) error {
	o.wins = make(map[wkey]*acc)
	o.sess = make(map[string][]*session)
	o.late.Store(0)

	var wm Watermark
	for _, b := range blobs {
		var s snap
		if err := gob.NewDecoder(bytes.NewReader(b)).Decode(&s); err != nil {
			return err
		}
		for _, e := range s.Entries {
			if keep == nil || keep(e.K.Key) {
				a := e.A
				o.wins[e.K] = &a
			}
		}
		for _, e := range s.Sess {
			if keep == nil || keep(e.Key) {
				x := e.S
				o.sess[e.Key] = append(o.sess[e.Key], &x)
			}
		}
		if keep == nil {
			o.late.Add(s.Late)
		}
		if s.WM.Init && (!wm.Init || s.WM.Max < wm.Max) {
			wm.Max, wm.Init = s.WM.Max, true
		}
	}
	wm.Disorder = o.cfg.Disorder
	o.wm = wm
	return nil
}

func quantiles(a *acc) (p50, p95, p99 float64) {
	if a.Q == nil {
		return
	}
	return a.Q.Quantile(0.5), a.Q.Quantile(0.95), a.Q.Quantile(0.99)
}
