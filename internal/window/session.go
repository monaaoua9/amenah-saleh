package window

import (
	"container/heap"

	"github.com/monaaoua9/streamdb/internal/event"
	"github.com/monaaoua9/streamdb/internal/hll"
	"github.com/monaaoua9/streamdb/internal/sketch"
)

type session struct {
	Start, End int64 // End = last event + gap
	A          acc
}

func (o *Operator) addSession(e event.Event) {
	gap := o.cfg.Gap
	if e.Time+gap <= o.wm.Current() {
		o.drop(e)
		return
	}

	ns := &session{Start: e.Time, End: e.Time + gap, A: acc{Count: 1, Sum: e.Value, SumSq: e.Value * e.Value, Min: e.Value, Max: e.Value}}
	if o.cfg.Quantiles {
		ns.A.Q = sketch.New()
		ns.A.Q.Add(e.Value)
	}
	if o.cfg.Distinct {
		ns.A.D = hll.New()
		ns.A.D.AddFloat(e.Value)
	}
	var keep []*session
	for _, s := range o.sess[e.Key] {
		if s.Start > ns.End || ns.Start > s.End {
			keep = append(keep, s)
			continue
		}
		// overlaps: this event bridges (or extends) an existing session
		ns.Start = min(ns.Start, s.Start)
		ns.End = max(ns.End, s.End)
		ns.A.Count += s.A.Count
		ns.A.Sum += s.A.Sum
		ns.A.SumSq += s.A.SumSq
		ns.A.Min = min(ns.A.Min, s.A.Min)
		ns.A.Max = max(ns.A.Max, s.A.Max)
		if s.A.Q != nil {
			if ns.A.Q == nil {
				ns.A.Q = sketch.New()
			}
			ns.A.Q.Merge(s.A.Q)
		}
		if s.A.D != nil {
			if ns.A.D == nil {
				ns.A.D = hll.New()
			}
			ns.A.D.Merge(s.A.D)
		}
	}
	o.sess[e.Key] = append(keep, ns)
	o.scheduleSession(e.Key)
	o.wm.Observe(e.Time)
}

func (o *Operator) advanceSessions(emit func(event.Result)) {
	wm := o.wm.Current()
	for len(o.sessTimers) > 0 && o.sessTimers[0].at <= wm {
		t := heap.Pop(&o.sessTimers).(sessTimer)
		if cur, ok := o.sched[t.key]; !ok || cur != t.at {
			continue // superseded by an earlier entry
		}
		delete(o.sched, t.key)
		k, list := t.key, o.sess[t.key]
		keep := list[:0]
		for _, s := range list {
			if s.End > wm {
				keep = append(keep, s)
				continue
			}
			r := event.Result{
				Key: k, Start: s.Start, End: s.End,
				Count: s.A.Count, Sum: s.A.Sum, SumSq: s.A.SumSq, Min: s.A.Min, Max: s.A.Max,
			}
			r.P50, r.P95, r.P99 = quantiles(&s.A)
			r.Distinct = s.A.D.Count()
			emit(r)
		}
		if len(keep) == 0 {
			delete(o.sess, k)
		} else {
			o.sess[k] = keep
		}
		o.scheduleSession(k)
	}
}

type sessEntry struct {
	Key string
	S   session
}
