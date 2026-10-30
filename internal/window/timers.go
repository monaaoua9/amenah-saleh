package window

import "container/heap"

// Advance used to walk every open window per batch. These heaps make it cost
// proportional to what actually fires. They are derived state: never
// snapshotted, rebuilt by rebuildTimers after a restore.

type timer struct {
	at int64
	k  wkey
}

type timerHeap []timer

func (h timerHeap) Len() int           { return len(h) }
func (h timerHeap) Less(i, j int) bool { return h[i].at < h[j].at }
func (h timerHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *timerHeap) Push(x any)        { *h = append(*h, x.(timer)) }
func (h *timerHeap) Pop() any {
	old := *h
	n := len(old)
	t := old[n-1]
	*h = old[:n-1]
	return t
}

type sessTimer struct {
	at  int64
	key string
}

type sessHeap []sessTimer

func (h sessHeap) Len() int           { return len(h) }
func (h sessHeap) Less(i, j int) bool { return h[i].at < h[j].at }
func (h sessHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *sessHeap) Push(x any)        { *h = append(*h, x.(sessTimer)) }
func (h *sessHeap) Pop() any {
	old := *h
	n := len(old)
	t := old[n-1]
	*h = old[:n-1]
	return t
}

func (o *Operator) rebuildTimers() {
	o.timers = o.timers[:0]
	o.sessTimers = o.sessTimers[:0]
	o.sched = make(map[string]int64)
	for k, a := range o.wins {
		end := k.Start + o.cfg.Size
		if a.Fired {
			o.timers = append(o.timers, timer{end + o.cfg.Lateness, k})
		} else {
			o.timers = append(o.timers, timer{end, k})
		}
	}
	heap.Init(&o.timers)

	for key := range o.sess {
		o.scheduleSession(key)
	}
}

// Keeps one live heap entry per key: the earliest end among its sessions.
// A later pop re-schedules whatever is left, so a busy key doesn't pile up
// an entry per event.
func (o *Operator) scheduleSession(key string) {
	list := o.sess[key]
	if len(list) == 0 {
		delete(o.sched, key)
		return
	}
	at := list[0].End
	for _, s := range list[1:] {
		at = min(at, s.End)
	}
	if cur, ok := o.sched[key]; ok && cur <= at {
		return
	}
	o.sched[key] = at
	heap.Push(&o.sessTimers, sessTimer{at, key})
}
