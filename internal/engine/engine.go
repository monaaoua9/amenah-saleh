package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/semaphore"

	"github.com/monaaoua9/streamdb/internal/checkpoint"
	cpb "github.com/monaaoua9/streamdb/internal/cluster/pb"
	"github.com/monaaoua9/streamdb/internal/event"
	"github.com/monaaoua9/streamdb/internal/metrics"
	"github.com/monaaoua9/streamdb/internal/partition"
	"github.com/monaaoua9/streamdb/internal/window"
)

var (
	ErrOverloaded = errors.New("engine overloaded")
	ErrStopped    = errors.New("engine stopped")
	// permanent: retrying the same batch can never work
	ErrBatchTooLarge = errors.New("batch too large")
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

	// Connected worker nodes. When set, Workers is taken from its length and the
	// windows run on those nodes instead of in-process. Needs the Hash partitioner.
	Remote []WorkerConn
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
	remotes []*remoteWorker
	sink    *sink

	credits *semaphore.Weighted
	store   *checkpoint.Store
	coord   *checkpoint.Coordinator
	ckMu    sync.Mutex
	ckNow   chan struct{}

	committed sync.Map // producer -> highest seq inside a completed checkpoint

	// what restore() found, for remote workers whose Init goes out later
	restored bool
	rblobs   [][]byte
	rsame    bool

	lat      metrics.Histogram
	ingested atomic.Uint64
	dups     atomic.Uint64
	emitted  atomic.Uint64
	inflight atomic.Int64
	ckCount  atomic.Uint64
	ckLastNs atomic.Int64

	done     chan struct{}
	runCtx   context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	failCh   chan error
	failOnce sync.Once
}

func New(cfg Config) *Engine {
	if cfg.Remote != nil {
		cfg.Workers = len(cfg.Remote)
	}
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
	return &Engine{
		cfg: cfg, done: make(chan struct{}),
		ckNow: make(chan struct{}, 1), failCh: make(chan error, 1),
	}
}

func laneTask(i int) string   { return fmt.Sprintf("lane-%d", i) }
func workerTask(i int) string { return fmt.Sprintf("worker-%d", i) }

// Start restores from the latest complete checkpoint if there is one.
func (e *Engine) Start(ctx context.Context) error {
	ctx, e.cancel = context.WithCancel(ctx)
	e.runCtx = ctx
	if err := e.start(ctx); err != nil {
		e.cancel()
		for _, c := range e.cfg.Remote {
			c.Close()
		}
		return err
	}
	return nil
}

func (e *Engine) start(ctx context.Context) error {
	cfg := e.cfg
	if err := cfg.Window.Validate(); err != nil {
		return err
	}
	if cfg.Remote != nil {
		if _, ok := cfg.Partitioner.(partition.Hash); !ok {
			return errors.New("remote workers require the Hash partitioner")
		}
	}
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
		in := make(chan workerMsg, cfg.QueueDepth)
		if cfg.Remote != nil {
			e.workers = append(e.workers, &worker{id: i, in: in}) // routing slot only
			e.remotes = append(e.remotes, &remoteWorker{id: i, conn: cfg.Remote[i], e: e, in: in})
			continue
		}
		w := newWorker(i, cfg.Lanes, cfg.Window, localEnv{e})
		w.in = in
		e.workers = append(e.workers, w)
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

	for _, r := range e.remotes {
		if err := r.hello(); err != nil {
			return fmt.Errorf("init worker %d: %w", r.id, err)
		}
	}

	spawn := func(fn func(context.Context)) {
		e.wg.Add(1)
		go func() { defer e.wg.Done(); fn(ctx) }()
	}
	spawn(e.sink.run)
	for _, w := range e.workers {
		if w.op != nil {
			spawn(w.run)
		}
	}
	for _, r := range e.remotes {
		spawn(r.sendLoop)
		spawn(r.recvLoop)
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
			e.committed.Store(p, l.last[p])
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
	same := old.Workers == e.cfg.Workers
	e.restored, e.rblobs, e.rsame = true, blobs, same

	if same {
		for i, w := range e.workers {
			if w.op == nil {
				continue
			}
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
		if w.op == nil {
			continue
		}
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
	for _, r := range e.remotes {
		r.conn.Close() // unblocks its Recv
	}
	e.wg.Wait()
}

// Failed delivers the first fatal error from a remote worker. The engine
// cancels itself when that happens; the caller should Stop it and rebuild.
func (e *Engine) Failed() <-chan error { return e.failCh }

func (e *Engine) fail(err error) {
	e.failOnce.Do(func() {
		e.failCh <- err
		e.cancel()
	})
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
		return fmt.Errorf("%w: %d events, max inflight %d", ErrBatchTooLarge, n, e.cfg.MaxInflight)
	}
	if !e.credits.TryAcquire(n) {
		if !block {
			return ErrOverloaded
		}
		// waiting on credits that a dead engine will never return would hang
		actx, cancel := context.WithCancel(ctx)
		stop := context.AfterFunc(e.runCtx, cancel)
		err := e.credits.Acquire(actx, n)
		stop()
		cancel()
		if err != nil {
			if e.runCtx.Err() != nil {
				return ErrStopped
			}
			return err
		}
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
	case <-e.runCtx.Done():
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
	e.noteCommitted(id)
	e.sink.commit(id)
	if err := e.store.MarkEmitted(id); err != nil {
		return 0, err
	}
	e.store.Prune(3)
	e.ckCount.Add(1)
	e.ckLastNs.Store(int64(time.Since(t0)))
	return id, nil
}

// Committed is the highest sequence number of this producer that a completed
// checkpoint covers. Events above it are lost if the engine dies right now.
func (e *Engine) Committed(producer string) uint64 {
	v, _ := e.committed.Load(producer)
	seq, _ := v.(uint64)
	return seq
}

func (e *Engine) noteCommitted(id uint64) {
	for i := range e.lanes {
		b, err := e.store.Get(id, laneTask(i))
		if err != nil {
			continue
		}
		m, err := decodeLast(b)
		if err != nil {
			continue
		}
		for p, seq := range m {
			if seq > e.Committed(p) {
				e.committed.Store(p, seq)
			}
		}
	}
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
		if w.op != nil {
			s.Late += w.op.Late()
		}
	}
	for _, r := range e.remotes {
		s.Late += r.late.Load()
	}
	return s
}

func windowToWire(c window.Config) *cpb.WindowConfig {
	return &cpb.WindowConfig{Size: c.Size, Slide: c.Slide, Gap: c.Gap, Lateness: c.Lateness, Disorder: c.Disorder, MaxJump: c.MaxJump}
}

func windowFromWire(c *cpb.WindowConfig) window.Config {
	return window.Config{Size: c.Size, Slide: c.Slide, Gap: c.Gap, Lateness: c.Lateness, Disorder: c.Disorder, MaxJump: c.MaxJump}
}
