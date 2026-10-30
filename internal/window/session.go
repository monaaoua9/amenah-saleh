package window

import (
	"github.com/monaaoua9/amenah-saleh/internal/event"
	"github.com/monaaoua9/amenah-saleh/internal/sketch"
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
	}
	o.sess[e.Key] = append(keep, ns)
	o.wm.Observe(e.Time)
}

func (o *Operator) advanceSessions(emit func(event.Result)) {
	wm := o.wm.Current()
	for k, list := range o.sess {
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
			emit(r)
		}
		if len(keep) == 0 {
			delete(o.sess, k)
		} else {
			o.sess[k] = keep
		}
	}
}

type sessEntry struct {
	Key string
	S   session
}
