package event

import "math"

type Event struct {
	Key   string  `json:"key"`
	Value float64 `json:"value"`
	Time  int64   `json:"time"` // event time, unix ms
	Seq   uint64  `json:"seq"`  // per-producer, starts at 1; 0 opts out of dedup
}

type Result struct {
	Key      string  `json:"key"`
	Start    int64   `json:"start"`
	End      int64   `json:"end"`
	Count    int64   `json:"count"`
	Sum      float64 `json:"sum"`
	Min      float64 `json:"min"`
	Max      float64 `json:"max"`
	Late     bool    `json:"late,omitempty"` // refire caused by late data inside allowed lateness
	SumSq    float64 `json:"sum_sq"`
	P50      float64 `json:"p50,omitempty"` // only when window.Config.Quantiles is on
	P95      float64 `json:"p95,omitempty"`
	P99      float64 `json:"p99,omitempty"`
	Distinct uint64  `json:"distinct,omitempty"` // distinct values, only with window.Config.Distinct
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

// Metric picks one of count, sum, min, max, avg, stddev, p50, p95, p99, distinct by name.
// The percentiles are zero unless the engine runs with quantiles on.
func (r Result) Metric(name string) (float64, bool) {
	switch name {
	case "count":
		return float64(r.Count), true
	case "sum":
		return r.Sum, true
	case "min":
		return r.Min, true
	case "max":
		return r.Max, true
	case "avg":
		return r.Avg(), true
	case "stddev":
		return r.StdDev(), true
	case "p50":
		return r.P50, true
	case "p95":
		return r.P95, true
	case "p99":
		return r.P99, true
	case "distinct":
		return float64(r.Distinct), true
	}
	return 0, false
}
