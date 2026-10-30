package window

import "container/heap"

// Advance used to walk every open window per batch. These heaps make it cost
// proportional to what actually fires. They are derived state: never
// snapshotted, rebuilt by rebuildTimers after a restore.

// Windows are grouped by the instant they next need attention, so the heap
// holds one entry per distinct time instead of one per window: every tumbling
// window of a given Start shares an end, across all keys.
type int64Heap []int64

func (h int64Heap) Len() int           { return len(h) }
func (h int64Heap) Less(i, j int) bool { return h[i] < h[j] }
func (h int64Heap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *int64Heap) Push(x any)        { *h = append(*h, x.(int64)) }
func (h *int64Heap) Pop() any {
	old := *h
	n := len(old)
	t := old[n-1]
	*h = old[:n-1]
	return t
}

func (o *Operator) schedule(at int64, ks ...ref) {
	if _, ok := o.due[at]; !ok {
		heap.Push(&o.dueTimes, at)
	}
	o.due[at] = append(o.due[at], ks...)
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
	o.due = make(map[int64][]ref)
	o.dueTimes = o.dueTimes[:0]
	o.sessTimers = o.sessTimers[:0]
	o.sched = make(map[string]int64)
	for _, ks := range o.keys {
		for _, s := range ks.w {
			end := s.start + o.cfg.Size
			if s.a.Fired {
				o.schedule(end+o.cfg.Lateness, ref{ks, s.start})
			} else {
				o.schedule(end, ref{ks, s.start})
			}
		}
	}

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
