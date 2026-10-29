package window

import "github.com/monaaoua9/streamdb/internal/event"

type session struct {
	Start, End int64 // End = last event + gap
	A          acc
}

func (o *Operator) addSession(e event.Event) {
	gap := o.cfg.Gap
	if e.Time+gap <= o.wm.Current() {
		o.late.Add(1)
		return
	}

	ns := &session{Start: e.Time, End: e.Time + gap, A: acc{Count: 1, Sum: e.Value, Min: e.Value, Max: e.Value}}
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
		ns.A.Min = min(ns.A.Min, s.A.Min)
		ns.A.Max = max(ns.A.Max, s.A.Max)
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
			emit(event.Result{
				Key: k, Start: s.Start, End: s.End,
				Count: s.A.Count, Sum: s.A.Sum, Min: s.A.Min, Max: s.A.Max,
			})
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
