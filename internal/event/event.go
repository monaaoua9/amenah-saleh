package event

import "math"

type Event struct {
	Key   string  `json:"key"`
	Value float64 `json:"value"`
	Time  int64   `json:"time"` // event time, unix ms
	Seq   uint64  `json:"seq"`  // per-producer, starts at 1; 0 opts out of dedup
}

type Result struct {
	Key   string  `json:"key"`
	Start int64   `json:"start"`
	End   int64   `json:"end"`
	Count int64   `json:"count"`
	Sum   float64 `json:"sum"`
	Min   float64 `json:"min"`
	Max   float64 `json:"max"`
	Late  bool    `json:"late,omitempty"` // refire caused by late data inside allowed lateness
	SumSq float64 `json:"sum_sq"`
}

func (r Result) Avg() float64 {
	if r.Count == 0 {
		return 0
	}
	return r.Sum / float64(r.Count)
}

// population stddev from the running sums; clamped because the subtraction can
// go slightly negative on near-constant data
func (r Result) StdDev() float64 {
	if r.Count == 0 {
		return 0
	}
	n := float64(r.Count)
	v := r.SumSq/n - (r.Sum/n)*(r.Sum/n)
	if v <= 0 {
		return 0
	}
	return math.Sqrt(v)
}
