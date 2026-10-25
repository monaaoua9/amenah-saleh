package engine

import (
	"context"
	"encoding/json"
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
	// Staged-but-uncommitted results above this count make the sink ask for an
	// early checkpoint. Soft: what arrives before the barrier lands still piles
	// on top. 0 = no limit.
	MaxPending int
	OnResult   func(event.Result)
}

type Stats struct {
	Ingested, Dups, Late, Emitted, Checkpoints uint64
	Inflight                                   int64
	LastCheckpoint                             time.Duration
	P50, P99                                   time.Duration
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
	ckNow   chan struct{}

	lat      metrics.Histogram
	ingested atomic.Uint64
	dups     atomic.Uint64
	emitted  atomic.Uint64
	inflight atomic.Int64
	ckCount  atomic.Uint64
	ckLastNs atomic.Int64

	done   chan struct{}
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func New(cfg Config) *Engine {
	if cfg.Workers > partition.KeyGroups {
		panic("workers exceed key groups")
	}
	if cfg.Partitioner == nil {
		cfg.Partitioner = partition.Hash{}
	}
	if cfg.QueueDepth == 0 {
		cfg.QueueDepth = 64
	}
	if cfg.MaxInflight == 0 {
		cfg.MaxInflight = 1 << 16
	}
	return &Engine{cfg: cfg, done: make(chan struct{}), ckNow: make(chan struct{}, 1)}
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
	if cfg.CheckpointEvery > 0 || cfg.MaxPending > 0 {
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

type meta struct{ Lanes, Workers int }

func (e *Engine) laneFor(producer string) *lane {
	return e.lanes[partition.Hash64(producer)%uint64(len(e.lanes))]
}

// Restore works across a change in lane/worker count: producer offsets are
// re-homed by producer hash, window state by key group.
func (e *Engine) restore(id uint64) error {
	old := meta{e.cfg.Lanes, e.cfg.Workers} // checkpoints from before meta existed
	if b, err := e.store.Get(id, "meta"); err == nil {
		if err := json.Unmarshal(b, &old); err != nil {
			return err
		}
	}

	for i := 0; i < old.Lanes; i++ {
		b, err := e.store.Get(id, laneTask(i))
		if err != nil {
			return err
		}
		m, err := decodeLast(b)
		if err != nil {
			return err
		}
		for p, seq := range m {
			l := e.laneFor(p)
			l.last[p] = max(l.last[p], seq)
		}
	}

	blobs := make([][]byte, old.Workers)
	for i := range blobs {
		b, err := e.store.Get(id, workerTask(i))
		if err != nil {
			return err
		}
		blobs[i] = b
	}

	if old.Workers == e.cfg.Workers {
		for i, w := range e.workers {
			if err := w.op.Restore(blobs[i]); err != nil {
				return err
			}
		}
		return nil
	}

	keyed, ok := e.cfg.Partitioner.(partition.Keyed)
	if !ok {
		return fmt.Errorf("worker count changed %d -> %d but partitioner is not keyed", old.Workers, e.cfg.Workers)
	}
	for _, w := range e.workers {
		w := w
		keep := func(k string) bool { return keyed.Owner(k, e.cfg.Workers) == w.id }
		if err := w.op.RestoreMerge(blobs, keep); err != nil {
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

func (e *Engine) release(n int64) {
	e.inflight.Add(-n)
	e.credits.Release(n)
}

// Shutdown takes a last checkpoint so nothing since the previous one is lost,
// then stops. Stop alone is the crash path.
func (e *Engine) Shutdown(ctx context.Context) error {
	_, err := e.Checkpoint(ctx)
	e.Stop()
	return err
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
	e.inflight.Add(n)

	l := e.lanes[partition.Hash64(producer)%uint64(len(e.lanes))]
	select {
	case l.in <- laneMsg{producer: producer, events: evs, at: time.Now()}:
		e.ingested.Add(uint64(n))
		return nil
	case <-ctx.Done():
		e.release(n)
		return ctx.Err()
	case <-e.done:
		e.release(n)
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
	t0 := time.Now()

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
	mb, _ := json.Marshal(meta{e.cfg.Lanes, e.cfg.Workers})
	if err := e.store.Put(id, "meta", mb); err != nil {
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
	e.ckCount.Add(1)
	e.ckLastNs.Store(int64(time.Since(t0)))
	return id, nil
}

func (e *Engine) checkpointLoop(ctx context.Context) {
	var tick <-chan time.Time
	if e.cfg.CheckpointEvery > 0 {
		t := time.NewTicker(e.cfg.CheckpointEvery)
		defer t.Stop()
		tick = t.C
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick:
		case <-e.ckNow:
		}
		if _, err := e.Checkpoint(ctx); err != nil && ctx.Err() == nil {
			log.Printf("checkpoint failed: %v", err)
		}
	}
}

func (e *Engine) Stats() Stats {
	s := Stats{
		Ingested:       e.ingested.Load(),
		Dups:           e.dups.Load(),
		Emitted:        e.emitted.Load(),
		Inflight:       e.inflight.Load(),
		Checkpoints:    e.ckCount.Load(),
		LastCheckpoint: time.Duration(e.ckLastNs.Load()),
		P50:            e.lat.Quantile(0.50),
		P99:            e.lat.Quantile(0.99),
	}
	for _, w := range e.workers {
		s.Late += w.op.Late()
	}
	return s
}
