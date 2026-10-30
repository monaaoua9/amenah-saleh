package alert

import (
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/monaaoua9/amenah-saleh/internal/event"
)

// Rule text: "<metric> <op> <number> [key=<substring>]"
// metric: count sum min max avg stddev    op: > >= < <= == !=
// e.g. "avg > 80 key=cpu-"
type Rule struct {
	Text   string
	metric string
	op     string
	limit  float64
	key    string
}

type Alert struct {
	Rule   string       `json:"rule"`
	Value  float64      `json:"value"`
	Result event.Result `json:"result"`
}

func ParseRule(s string) (Rule, error) {
	f := strings.Fields(s)
	if len(f) < 3 || len(f) > 4 {
		return Rule{}, fmt.Errorf("alert: want \"<metric> <op> <number> [key=<substr>]\", got %q", s)
	}
	r := Rule{Text: s, metric: f[0], op: f[1]}
	if _, ok := (event.Result{}).Metric(r.metric); !ok {
		return Rule{}, fmt.Errorf("alert: unknown metric %q", r.metric)
	}
	switch r.op {
	case ">", ">=", "<", "<=", "==", "!=":
	default:
		return Rule{}, fmt.Errorf("alert: bad operator %q", r.op)
	}
	n, err := strconv.ParseFloat(f[2], 64)
	if err != nil {
		return Rule{}, fmt.Errorf("alert: bad number %q", f[2])
	}
	r.limit = n
	if len(f) == 4 {
		k, ok := strings.CutPrefix(f[3], "key=")
		if !ok || k == "" {
			return Rule{}, fmt.Errorf("alert: expected key=<substr>, got %q", f[3])
		}
		r.key = k
	}
	return r, nil
}

func (r Rule) match(res event.Result) (float64, bool) {
	if r.key != "" && !strings.Contains(res.Key, r.key) {
		return 0, false
	}
	v, _ := res.Metric(r.metric)
	var hit bool
	switch r.op {
	case ">":
		hit = v > r.limit
	case ">=":
		hit = v >= r.limit
	case "<":
		hit = v < r.limit
	case "<=":
		hit = v <= r.limit
	case "==":
		hit = v == r.limit
	case "!=":
		hit = v != r.limit
	}
	return v, hit
}

type cdKey struct {
	rule int
	key  string
}

// Evaluator is an output sink: feed it results, it calls notify for each rule
// that trips. Cooldown is in event time (result end), so replays behave the
// same as live runs; the same rule on the same key stays quiet for that long.
type Evaluator struct {
	rules    []Rule
	cooldown int64
	notify   func(Alert)

	mu   sync.Mutex
	last map[cdKey]int64
}

func NewEvaluator(rules []Rule, cooldownMs int64, notify func(Alert)) *Evaluator {
	return &Evaluator{rules: rules, cooldown: cooldownMs, notify: notify, last: map[cdKey]int64{}}
}

func (e *Evaluator) Write(res event.Result) {
	for i, r := range e.rules {
		v, hit := r.match(res)
		if !hit {
			continue
		}
		k := cdKey{i, res.Key}
		e.mu.Lock()
		prev, seen := e.last[k]
		quiet := seen && res.End-prev < e.cooldown
		if !quiet {
			e.last[k] = res.End
			if len(e.last) > 100000 {
				e.prune(res.End)
			}
		}
		e.mu.Unlock()
		if !quiet {
			e.notify(Alert{Rule: r.Text, Value: v, Result: res})
		}
	}
}

// caller holds mu
func (e *Evaluator) prune(now int64) {
	for k, t := range e.last {
		if now-t >= e.cooldown {
			delete(e.last, k)
		}
	}
}
