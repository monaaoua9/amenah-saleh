package engine

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/semaphore"

	"github.com/monaaoua9/amenah-saleh/internal/checkpoint"
	"github.com/monaaoua9/amenah-saleh/internal/event"
	"github.com/monaaoua9/amenah-saleh/internal/metrics"
	"github.com/monaaoua9/amenah-saleh/internal/partition"
	"github.com/monaaoua9/amenah-saleh/internal/window"
)

var (
	ErrOverloaded = errors.New("engine overloaded")
	ErrStopped    = errors.New("engine stopped")
)

type Config struct {
	Lanes, Workers  int
	QueueDepth      int   // batches per channel
	MaxInflight     int64 // events admitted but not yet fully processed; the real memory bound
	Window          window.Config
	Partitioner     partition.Partitioner
	CheckpointDir   string
	CheckpointEvery time.Duration // 0 = manual only
	OnResult        func(event.Result)
}

type Stats struct {
	Ingested, Dups, Late uint64
	P50, P99             time.Duration
}

type Engine struct {
	cfg     Config
	lanes   []*lane
	workers []*worker
	sink    *sink

	credits *semaphore.Weighted
	store   *checkpoint.Store
	coord   *checkpoint.Coordinator
	ckMu    sync.Mutex

	lat      metrics.Histogram
	ingested atomic.Uint64
	dups     atomic.Uint64

	done   chan struct{}
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func New(cfg Config) *Engine {
	if cfg.Partitioner == nil {
		cfg.Partitioner = partition.Hash{}
	}
	if cfg.QueueDepth == 0 {
		cfg.QueueDepth = 64
	}
	if cfg.MaxInflight == 0 {
		cfg.MaxInflight = 1 << 16
	}
	return &Engine{cfg: cfg, done: make(chan struct{})}
}

func laneTask(i int) string   { return fmt.Sprintf("lane-%d", i) }
func workerTask(i int) string { return fmt.Sprintf("worker-%d", i) }

// Start restores from the latest complete checkpoint if there is one.
func (e *Engine) Start(ctx context.Context) error {
	cfg := e.cfg
	e.credits = semaphore.NewWeighted(cfg.MaxInflight)
	e.store = checkpoint.NewStore(cfg.CheckpointDir)

	e.sink = &sink{
		in:  make(chan sinkMsg, cfg.QueueDepth),
		e:   e,
		al:  newAligner[sinkMsg](cfg.Workers),
		out: cfg.OnResult,
	}
	for i := 0; i < cfg.Lanes; i++ {
		e.lanes = append(e.lanes, &lane{id: i, in: make(chan laneMsg, cfg.QueueDepth), e: e, last: map[string]uint64{}})
	}
	for i := 0; i < cfg.Workers; i++ {
		e.workers = append(e.workers, &worker{
			id: i, in: make(chan workerMsg, cfg.QueueDepth), e: e,
			op: window.New(cfg.Window), al: newAligner[workerMsg](cfg.Lanes),
			laneWM: slices.Repeat([]int64{math.MinInt64}, cfg.Lanes),
		})
	}

	last, ok := e.store.Latest()
	if ok {
		if err := e.restore(last); err != nil {
			return fmt.Errorf("restore chk %d: %w", last, err)
		}
		log.Printf("restored from checkpoint %d", last)
		if err := e.replayStaged(last); err != nil {
			return fmt.Errorf("replay sink chk %d: %w", last, err)
		}
	}
	e.coord = checkpoint.NewCoordinator(e.store, cfg.Lanes+cfg.Workers+1, last)

	ctx, e.cancel = context.WithCancel(ctx)
	spawn := func(fn func(context.Context)) {
		e.wg.Add(1)
		go func() { defer e.wg.Done(); fn(ctx) }()
	}
	spawn(e.sink.run)
	for _, w := range e.workers {
		spawn(w.run)
	}
	for _, l := range e.lanes {
		spawn(l.run)
	}
	if cfg.CheckpointEvery > 0 {
		spawn(e.checkpointLoop)
	}
	return nil
}

// Crashed after the checkpoint was complete but before its output went out:
// the staged results are in the snapshot, send them now. If the crash was
// between emit and the marker they go out twice, (key,start) is the dedup id.
func (e *Engine) replayStaged(id uint64) error {
	if e.store.Emitted(id) {
		return nil
	}
	b, err := e.store.Get(id, "sink")
	if err != nil {
		return err
	}
	res, err := decodeResults(b)
	if err != nil {
		return err
	}
	for _, r := range res {
		e.cfg.OnResult(r)
	}
	return e.store.MarkEmitted(id)
}

func (e *Engine) restore(id uint64) error {
	for _, l := range e.lanes {
		b, err := e.store.Get(id, laneTask(l.id))
		if err != nil {
			return err
		}
		if err := l.restore(b); err != nil {
			return err
		}
	}
	for _, w := range e.workers {
		b, err := e.store.Get(id, workerTask(w.id))
		if err != nil {
			return err
		}
		if err := w.op.Restore(b); err != nil {
			return err
		}
	}
	return nil
}

// Stop is a hard stop, no final checkpoint. Same as a crash as far as state goes.
func (e *Engine) Stop() {
	close(e.done)
	e.cancel()
	e.wg.Wait()
}

func (e *Engine) Submit(ctx context.Context, producer string, evs []event.Event) error {
	return e.submit(ctx, producer, evs, true)
}

// TrySubmit fails fast with ErrOverloaded instead of waiting for credits.
func (e *Engine) TrySubmit(ctx context.Context, producer string, evs []event.Event) error {
	return e.submit(ctx, producer, evs, false)
}

func (e *Engine) submit(ctx context.Context, producer string, evs []event.Event, block bool) error {
	n := int64(len(evs))
	if n == 0 {
		return nil
	}
	if n > e.cfg.MaxInflight {
		return fmt.Errorf("batch of %d exceeds max inflight %d", n, e.cfg.MaxInflight)
	}
	if block {
		if err := e.credits.Acquire(ctx, n); err != nil {
			return err
		}
	} else if !e.credits.TryAcquire(n) {
		return ErrOverloaded
	}

	l := e.lanes[partition.Hash64(producer)%uint64(len(e.lanes))]
	select {
	case l.in <- laneMsg{producer: producer, events: evs, at: time.Now()}:
		e.ingested.Add(uint64(n))
		return nil
	case <-ctx.Done():
		e.credits.Release(n)
		return ctx.Err()
	case <-e.done:
		e.credits.Release(n)
		return ErrStopped
	}
}

// AdvanceWatermark pushes event time forward on every worker, for idle keys
// or end of input. Workers take the min across lanes before applying it.
func (e *Engine) AdvanceWatermark(ctx context.Context, ts int64) error {
	for _, l := range e.lanes {
		select {
		case l.in <- laneMsg{punct: true, wm: ts}:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (e *Engine) Checkpoint(ctx context.Context) (uint64, error) {
	e.ckMu.Lock()
	defer e.ckMu.Unlock()

	id := e.coord.Begin()
	for _, l := range e.lanes {
		select {
		case l.in <- laneMsg{barrier: id}:
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	if err := e.coord.Wait(ctx, id); err != nil {
		return 0, err
	}
	if err := e.store.Commit(id); err != nil {
		return 0, err
	}
	e.sink.commit(id)
	if err := e.store.MarkEmitted(id); err != nil {
		return 0, err
	}
	e.store.Prune(3)
	return id, nil
}

func (e *Engine) checkpointLoop(ctx context.Context) {
	t := time.NewTicker(e.cfg.CheckpointEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if _, err := e.Checkpoint(ctx); err != nil && ctx.Err() == nil {
				log.Printf("checkpoint failed: %v", err)
			}
		}
	}
}

func (e *Engine) Stats() Stats {
	s := Stats{
		Ingested: e.ingested.Load(),
		Dups:     e.dups.Load(),
		P50:      e.lat.Quantile(0.50),
		P99:      e.lat.Quantile(0.99),
	}
	for _, w := range e.workers {
		s.Late += w.op.Late()
	}
	return s
}
