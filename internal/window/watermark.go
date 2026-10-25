package window

import "math"

// Bounded out-of-orderness: watermark trails the max seen event time by Disorder ms.
type Watermark struct {
	Max      int64
	Init     bool
	Disorder int64
}

func (w *Watermark) Observe(t int64) {
	if !w.Init || t > w.Max {
		w.Max = t
		w.Init = true
	}
}

func (w *Watermark) Current() int64 {
	if !w.Init {
		return math.MinInt64
	}
	return w.Max - w.Disorder
}
