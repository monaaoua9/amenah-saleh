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

// What a worker needs from its surroundings. In-process it's the engine; on a
// worker node it's the gRPC stream back to the master.
type workerEnv interface {
	sink(ctx context.Context, m sinkMsg)
	ack(id uint64, task string, data []byte, err error)
	processed(n int, at time.Time)
}

type worker struct {
	id  int
	in  chan workerMsg
	env workerEnv
	op  *window.Operator
	al  *aligner[workerMsg]

	// last punctuation seen per lane; effective watermark is the min
	laneWM []int64
}

func newWorker(id, lanes int, cfg window.Config, env workerEnv) *worker {
	return &worker{
		id: id, env: env,
		op:     window.New(cfg),
		al:     newAligner[workerMsg](lanes),
		laneWM: slices.Repeat([]int64{math.MinInt64}, lanes),
	}
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
		w.env.ack(m.barrier, workerTask(w.id), snap, err)
		w.env.sink(ctx, sinkMsg{from: w.id, barrier: m.barrier})
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
			w.env.sink(ctx, sinkMsg{from: w.id, results: out})
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
		w.env.sink(ctx, sinkMsg{from: w.id, results: out})
	}
	// credits go back only after the sink took the output, so a slow sink throttles ingest
	w.env.processed(n, m.at)
}

type localEnv struct{ e *Engine }

func (l localEnv) sink(ctx context.Context, m sinkMsg) {
	select {
	case l.e.sink.in <- m:
	case <-ctx.Done():
	}
}

func (l localEnv) ack(id uint64, task string, data []byte, err error) {
	l.e.coord.Ack(id, task, data, err)
}

func (l localEnv) processed(n int, at time.Time) {
	l.e.release(int64(n))
	l.e.lat.Observe(time.Since(at))
}
