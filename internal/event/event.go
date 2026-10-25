package event

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
}
