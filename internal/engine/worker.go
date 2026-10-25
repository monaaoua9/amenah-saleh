package engine

import (
	"context"
	"math"
	"slices"
	"time"

	"github.com/monaaoua9/amenah-saleh/internal/event"
	"github.com/monaaoua9/amenah-saleh/internal/window"
)

type workerMsg struct {
	from    int
	batch   *[]event.Event
	at      time.Time
	barrier uint64
	punct   bool
	wm      int64
}

type worker struct {
	id int
	in chan workerMsg
	e  *Engine
	op *window.Operator
	al *aligner[workerMsg]

	// last punctuation seen per lane; effective watermark is the min
	laneWM []int64
}

func (w *worker) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case m := <-w.in:
			w.handle(ctx, m)
		}
	}
}

func (w *worker) handle(ctx context.Context, m workerMsg) {
	if w.al.blocked(m.from) {
		w.al.hold(m)
		return
	}

	switch {
	case m.barrier != 0:
		if !w.al.arrive(m.from) {
			return
		}
		snap, err := w.op.Snapshot()
		w.e.coord.Ack(m.barrier, workerTask(w.id), snap, err)
		w.send(ctx, sinkMsg{from: w.id, barrier: m.barrier})
		for _, h := range w.al.release() {
			w.handle(ctx, h)
		}

	case m.punct:
		w.laneWM[m.from] = max(w.laneWM[m.from], m.wm)
		low := slices.Min(w.laneWM)
		if low == math.MinInt64 {
			return
		}
		w.op.AdvanceTo(low)
		var out []event.Result
		w.op.Advance(func(r event.Result) { out = append(out, r) })
		if len(out) > 0 {
			w.send(ctx, sinkMsg{from: w.id, results: out})
		}

	default:
		w.process(ctx, m)
	}
}

func (w *worker) process(ctx context.Context, m workerMsg) {
	b := *m.batch
	var out []event.Result
	emit := func(r event.Result) { out = append(out, r) }

	for i := range b {
		w.op.Add(b[i], emit)
	}
	w.op.Advance(emit)
	n := len(b)
	batchPool.Put(m.batch)

	if len(out) > 0 {
		w.send(ctx, sinkMsg{from: w.id, results: out})
	}
	// credits go back only after the sink took the output, so a slow sink throttles ingest
	w.e.credits.Release(int64(n))
	w.e.lat.Observe(time.Since(m.at))
}

func (w *worker) send(ctx context.Context, m sinkMsg) {
	select {
	case w.e.sink.in <- m:
	case <-ctx.Done():
	}
}
