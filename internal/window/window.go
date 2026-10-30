package window

import (
	"container/heap"
	"errors"
	"sync/atomic"

	"github.com/monaaoua9/streamdb/internal/event"
	"github.com/monaaoua9/streamdb/internal/hll"
	"github.com/monaaoua9/streamdb/internal/sketch"
)

type Config struct {
	Size      int64 // ms
	Slide     int64 // ms, 0 means tumbling (slide == size)
	Gap       int64 // ms of silence that closes a session; > 0 switches to session windows
	Lateness  int64 // ms a fired window is kept around for stragglers
	Disorder  int64 // ms, feeds the watermark
	MaxJump   int64 // ms, an event further ahead of the watermark base than this is dropped; 0 = unlimited
	Quantiles bool  // keep a sketch per window and report p50/p95/p99 (costs memory and some throughput)
	Distinct  bool  // count distinct values per window with a HyperLogLog (1 KiB per open window)
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
	D        *hll.Sketch
}

// Operator is single-threaded, one per worker.
type Operator struct {
	cfg   Config
	keys  map[string]*keyWins
	nwins int
	sess  map[string][]*session
	wm    Watermark
	late  atomic.Uint64

	// sees every event counted as late, including implausible timestamps.
	// Runs on the worker goroutine; set before the first Add.
	OnLate func(event.Event)

	due        map[int64][]ref // when -> windows needing attention then
	dueTimes   int64Heap       // each key of due once
	sessTimers sessHeap
	sched      map[string]int64 // session key -> at of its live heap entry
}

func (o *Operator) drop(e event.Event) {
	o.late.Add(1)
	if o.OnLate != nil {
		o.OnLate(e)
	}
}

func New(cfg Config) *Operator {
	return &Operator{
		cfg:   cfg,
		keys:  make(map[string]*keyWins),
		sess:  make(map[string][]*session),
		wm:    Watermark{Disorder: cfg.Disorder},
		sched: make(map[string]int64),
		due:   make(map[int64][]ref),
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
	rem := e.Time % slide // one division; the old double-mod form paid for two
	if rem < 0 {
		rem += slide
	}
	start := e.Time - rem
	ks := o.keys[e.Key] // one hash per event, however many windows it lands in
	taken := false
	for ; start+size > e.Time; start -= slide {
		// older windows only end earlier, so the rest are late too
		if start+size+o.cfg.Lateness <= wm {
			break
		}
		ks = o.addTo(ks, e, start, emit)
		taken = true
	}
	if !taken {
		o.drop(e)
	}
	o.wm.Observe(e.Time)
}

// ks is the key's window set, nil if it has none yet; the (possibly new) set is returned
func (o *Operator) addTo(ks *keyWins, e event.Event, start int64, emit func(event.Result)) *keyWins {
	var a *acc
	if ks != nil {
		a = ks.find(start)
	}
	if a == nil {
		if ks == nil {
			ks = o.keyFor(e.Key)
		}
		a = &acc{Min: e.Value, Max: e.Value}
		o.put(ks, start, a)
		o.schedule(start+o.cfg.Size, ref{ks, start})
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
	if o.cfg.Distinct {
		if a.D == nil {
			a.D = hll.New()
		}
		a.D.AddFloat(e.Value)
	}
	if e.Value < a.Min {
		a.Min = e.Value
	}
	if e.Value > a.Max {
		a.Max = e.Value
	}
	if a.Fired {
		r := toResult(wkey{e.Key, start}, a, o.cfg.Size)
		r.Late = true
		emit(r)
	}
	return ks
}

// call once per batch, not per event
func (o *Operator) Advance(emit func(event.Result)) {
	if o.cfg.Gap > 0 {
		o.advanceSessions(emit)
		return
	}
	wm := o.wm.Current()
	for len(o.dueTimes) > 0 && o.dueTimes[0] <= wm {
		at := heap.Pop(&o.dueTimes).(int64)
		ks := o.due[at]
		delete(o.due, at)
		var wait []ref
		for _, r := range ks {
			a := r.ks.find(r.start)
			if a == nil {
				continue
			}
			end := r.start + o.cfg.Size
			if !a.Fired {
				emit(toResult(wkey{r.ks.key, r.start}, a, o.cfg.Size))
				a.Fired = true
			}
			if end+o.cfg.Lateness <= wm {
				o.drop1(r.ks, r.start)
			} else {
				wait = append(wait, r)
			}
		}
		// same lateness for everyone, so they all expire together
		if len(wait) > 0 {
			o.schedule(at+o.cfg.Lateness, wait...)
		}
	}
}

// punctuation from outside (idle partitions, end of input)
func (o *Operator) AdvanceTo(ts int64) {
	o.wm.Observe(ts + o.cfg.Disorder)
}

func (o *Operator) Late() uint64 { return o.late.Load() }

func (o *Operator) Open() int {
	n := o.nwins
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
	r.Distinct = a.D.Count()
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
	s := snap{WM: o.wm, Late: o.late.Load(), Entries: make([]entry, 0, o.nwins)}
	o.each(func(key string, start int64, a *acc) {
		s.Entries = append(s.Entries, entry{wkey{key, start}, *a})
	})
	for k, l := range o.sess {
		for _, x := range l {
			s.Sess = append(s.Sess, sessEntry{k, *x})
		}
	}
	return encodeSnap(&s), nil
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
	o.keys = make(map[string]*keyWins)
	o.nwins = 0
	o.sess = make(map[string][]*session)
	o.late.Store(0)

	var wm Watermark
	for _, b := range blobs {
		s, err := decodeSnap(b)
		if err != nil {
			return err
		}
		for _, e := range s.Entries {
			if keep == nil || keep(e.K.Key) {
				a := e.A
				o.put(o.keyFor(e.K.Key), e.K.Start, &a)
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
	o.rebuildTimers()
	return nil
}

func quantiles(a *acc) (p50, p95, p99 float64) {
	if a.Q == nil {
		return
	}
	return a.Q.Quantile(0.5), a.Q.Quantile(0.95), a.Q.Quantile(0.99)
}
