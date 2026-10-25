package engine

import (
	"bytes"
	"context"
	"encoding/gob"
	"sync"
	"time"

	"github.com/monaaoua9/amenah-saleh/internal/event"
)

type laneMsg struct {
	producer string
	events   []event.Event
	at       time.Time
	barrier  uint64
	punct    bool
	wm       int64
}

var batchPool = sync.Pool{New: func() any {
	b := make([]event.Event, 0, 512)
	return &b
}}

func getBatch() *[]event.Event {
	b := batchPool.Get().(*[]event.Event)
	*b = (*b)[:0]
	return b
}

// lane = one upstream "source" as far as the workers are concerned. A producer
// always lands on the same lane, which is what makes seq dedup work.
type lane struct {
	id   int
	in   chan laneMsg
	e    *Engine
	last map[string]uint64
}

func (l *lane) run(ctx context.Context) {
	parts := make([]*[]event.Event, len(l.e.workers))
	for {
		select {
		case <-ctx.Done():
			return
		case m := <-l.in:
			switch {
			case m.barrier != 0:
				l.onBarrier(ctx, m.barrier)
			case m.punct:
				l.broadcast(ctx, workerMsg{from: l.id, punct: true, wm: m.wm})
			default:
				l.route(ctx, m, parts)
			}
		}
	}
}

func (l *lane) route(ctx context.Context, m laneMsg, parts []*[]event.Event) {
	nw := len(parts)
	dups := 0
	for i := range m.events {
		ev := &m.events[i]
		if ev.Seq != 0 {
			if ev.Seq <= l.last[m.producer] {
				dups++
				continue
			}
			l.last[m.producer] = ev.Seq
		}
		w := l.e.cfg.Partitioner.Pick(ev.Key, nw)
		if parts[w] == nil {
			parts[w] = getBatch()
		}
		*parts[w] = append(*parts[w], *ev)
	}
	if dups > 0 {
		l.e.dups.Add(uint64(dups))
		l.e.release(int64(dups))
	}
	for w, p := range parts {
		if p == nil {
			continue
		}
		parts[w] = nil
		select {
		case l.e.workers[w].in <- workerMsg{from: l.id, batch: p, at: m.at}:
		case <-ctx.Done():
			return
		}
	}
}

func (l *lane) onBarrier(ctx context.Context, id uint64) {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(l.last)
	l.e.coord.Ack(id, laneTask(l.id), buf.Bytes(), err)
	l.broadcast(ctx, workerMsg{from: l.id, barrier: id})
}

func (l *lane) broadcast(ctx context.Context, m workerMsg) {
	for _, w := range l.e.workers {
		select {
		case w.in <- m:
		case <-ctx.Done():
			return
		}
	}
}

func (l *lane) restore(b []byte) error {
	l.last = map[string]uint64{}
	return gob.NewDecoder(bytes.NewReader(b)).Decode(&l.last)
}
