package engine

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/monaaoua9/amenah-saleh/internal/event"
	"github.com/monaaoua9/amenah-saleh/internal/partition"
	"github.com/monaaoua9/amenah-saleh/internal/window"
)

type collector struct {
	mu  sync.Mutex
	res map[string]event.Result
	n   int
}

func newCollector() *collector { return &collector{res: map[string]event.Result{}} }

func (c *collector) add(r event.Result) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.res[fmt.Sprintf("%s@%d", r.Key, r.Start)] = r
	c.n++
}

func gen(n int) []event.Event {
	evs := make([]event.Event, n)
	for i := range evs {
		evs[i] = event.Event{Key: fmt.Sprintf("k%d", i%17), Value: float64(i % 5), Time: int64(i) * 10, Seq: uint64(i + 1)}
	}
	return evs
}

func testCfg(dir string, c *collector) Config {
	return Config{
		Lanes: 2, Workers: 4, QueueDepth: 8, MaxInflight: 4096,
		Window:        window.Config{Size: 1000},
		CheckpointDir: dir,
		OnResult:      c.add,
	}
}

func feed(t *testing.T, e *Engine, evs []event.Event) {
	t.Helper()
	ctx := context.Background()
	for i := 0; i < len(evs); i += 100 {
		end := min(i+100, len(evs))
		if err := e.Submit(ctx, "p", evs[i:end]); err != nil {
			t.Fatal(err)
		}
	}
}

func finish(t *testing.T, e *Engine) {
	t.Helper()
	ctx := context.Background()
	if err := e.AdvanceWatermark(ctx, 1<<40); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Checkpoint(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestExactlyOnceAcrossCrash(t *testing.T) {
	evs := gen(20000)

	base := newCollector()
	e := New(testCfg(t.TempDir(), base))
	if err := e.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	feed(t, e, evs)
	finish(t, e)
	e.Stop()

	dir := t.TempDir()
	got := newCollector()

	e1 := New(testCfg(dir, got))
	if err := e1.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	feed(t, e1, evs[:12000])
	if _, err := e1.Checkpoint(context.Background()); err != nil {
		t.Fatal(err)
	}
	feed(t, e1, evs[12000:15000]) // lost with the crash
	time.Sleep(50 * time.Millisecond)
	e1.Stop()

	e2 := New(testCfg(dir, got))
	if err := e2.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	feed(t, e2, evs) // producer replays everything, dedup eats the prefix
	finish(t, e2)
	e2.Stop()

	if got.n != base.n {
		t.Fatalf("emitted %d results, want %d (dup or loss)", got.n, base.n)
	}
	for k, w := range base.res {
		g, ok := got.res[k]
		if !ok || g.Count != w.Count || g.Sum != w.Sum {
			t.Fatalf("window %s: got %+v want %+v", k, g, w)
		}
	}
	if e2.Stats().Dups == 0 {
		t.Fatal("expected replay to hit dedup")
	}
}

func TestTrySubmitOverload(t *testing.T) {
	c := newCollector()
	cfg := testCfg(t.TempDir(), c)
	cfg.MaxInflight = 100
	e := New(cfg)
	if err := e.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer e.Stop()

	// reserve everything, nothing can drain a batch we never sent
	if !e.credits.TryAcquire(100) {
		t.Fatal("couldn't take credits")
	}
	err := e.TrySubmit(context.Background(), "p", gen(10))
	if err != ErrOverloaded {
		t.Fatalf("got %v, want ErrOverloaded", err)
	}
}

func TestStagedOutputSurvivesCrashBeforeEmit(t *testing.T) {
	dir := t.TempDir()
	first := newCollector()
	e1 := New(testCfg(dir, first))
	if err := e1.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	feed(t, e1, gen(5000))
	finish(t, e1)
	e1.Stop()

	// pretend we died between _COMPLETE and the emit
	id, _ := e1.store.Latest()
	if err := os.Remove(filepath.Join(dir, fmt.Sprintf("chk-%08d", id), "_EMITTED")); err != nil {
		t.Fatal(err)
	}

	second := newCollector()
	e2 := New(testCfg(dir, second))
	if err := e2.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	e2.Stop()

	if second.n != first.n || second.n == 0 {
		t.Fatalf("replayed %d results, want %d", second.n, first.n)
	}
}

func TestShutdownCheckpointsTail(t *testing.T) {
	dir := t.TempDir()
	evs := gen(3000)

	e1 := New(testCfg(dir, newCollector()))
	if err := e1.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	feed(t, e1, evs)
	if err := e1.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}

	e2 := New(testCfg(dir, newCollector()))
	if err := e2.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer e2.Stop()
	feed(t, e2, evs)
	finish(t, e2)

	// the whole replay should have been recognised as already processed
	if got := e2.Stats().Dups; got != uint64(len(evs)) {
		t.Fatalf("dups = %d, want %d", got, len(evs))
	}
}

func TestMetricsOutput(t *testing.T) {
	c := newCollector()
	e := New(testCfg(t.TempDir(), c))
	if err := e.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer e.Stop()
	feed(t, e, gen(2000))
	finish(t, e)

	var buf strings.Builder
	e.WriteMetrics(&buf)
	out := buf.String()
	for _, want := range []string{
		"dspe_events_ingested_total 2000",
		"dspe_inflight_events 0",
		"dspe_checkpoints_total 1",
		`dspe_batch_latency_seconds{quantile="0.99"}`,
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("missing %q in output: %s", want, out)
		}
	}
	if e.Stats().Emitted == 0 {
		t.Fatal("no results counted as emitted")
	}
}

func TestSubmitBlocksUntilCreditsFree(t *testing.T) {
	cfg := testCfg(t.TempDir(), newCollector())
	cfg.MaxInflight = 100
	e := New(cfg)
	if err := e.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer e.Stop()

	e.credits.TryAcquire(100) // pretend the pipeline is full

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := e.Submit(ctx, "p", gen(10)); err != context.DeadlineExceeded {
		t.Fatalf("got %v, want deadline exceeded while full", err)
	}
	if e.Stats().Inflight != 0 {
		t.Fatalf("failed submit leaked inflight: %d", e.Stats().Inflight)
	}

	done := make(chan error, 1)
	go func() { done <- e.Submit(context.Background(), "p", gen(10)) }()
	select {
	case <-done:
		t.Fatal("submit returned while pipeline was full")
	case <-time.After(50 * time.Millisecond):
	}

	e.credits.Release(100)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("submit still blocked after credits were freed")
	}
}

func TestRescaleOnRestore(t *testing.T) {
	evs := gen(20000)

	base := newCollector()
	eb := New(testCfg(t.TempDir(), base))
	if err := eb.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	feed(t, eb, evs)
	finish(t, eb)
	eb.Stop()

	for _, tc := range []struct{ lanes, workers int }{{3, 7}, {1, 2}, {2, 4}} {
		dir := t.TempDir()
		got := newCollector()

		e1 := New(testCfg(dir, got)) // 2 lanes, 4 workers
		if err := e1.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		feed(t, e1, evs[:12000])
		if _, err := e1.Checkpoint(context.Background()); err != nil {
			t.Fatal(err)
		}
		e1.Stop()

		cfg := testCfg(dir, got)
		cfg.Lanes, cfg.Workers = tc.lanes, tc.workers
		e2 := New(cfg)
		if err := e2.Start(context.Background()); err != nil {
			t.Fatalf("%+v: %v", tc, err)
		}
		feed(t, e2, evs)
		finish(t, e2)
		e2.Stop()

		if got.n != base.n {
			t.Fatalf("%+v: emitted %d results, want %d", tc, got.n, base.n)
		}
		for k, w := range base.res {
			g, ok := got.res[k]
			if !ok || g.Count != w.Count || g.Sum != w.Sum {
				t.Fatalf("%+v: window %s: got %+v want %+v", tc, k, g, w)
			}
		}
	}
}

func TestRescaleNeedsKeyedPartitioner(t *testing.T) {
	dir := t.TempDir()
	e1 := New(testCfg(dir, newCollector()))
	if err := e1.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	feed(t, e1, gen(1000))
	if _, err := e1.Checkpoint(context.Background()); err != nil {
		t.Fatal(err)
	}
	e1.Stop()

	cfg := testCfg(dir, newCollector())
	cfg.Workers = 3
	cfg.Partitioner = &partition.RoundRobin{}
	if err := New(cfg).Start(context.Background()); err == nil {
		t.Fatal("expected error rescaling with a non-keyed partitioner")
	}
}

func TestPendingLimitForcesCheckpoint(t *testing.T) {
	cfg := testCfg(t.TempDir(), newCollector())
	cfg.MaxPending = 50
	cfg.CheckpointEvery = time.Hour // only the limit can trigger one
	e := New(cfg)
	if err := e.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer e.Stop()

	feed(t, e, gen(20000))
	if err := e.AdvanceWatermark(context.Background(), 1<<40); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for e.Stats().Checkpoints == 0 {
		if time.Now().After(deadline) {
			t.Fatal("no automatic checkpoint despite pending limit")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestStopAfterFailedStartDoesNotPanic(t *testing.T) {
	dir := t.TempDir()
	e1 := New(testCfg(dir, newCollector()))
	if err := e1.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	feed(t, e1, gen(500))
	if _, err := e1.Checkpoint(context.Background()); err != nil {
		t.Fatal(err)
	}
	e1.Stop()

	cfg := testCfg(dir, newCollector())
	cfg.Workers = 2
	cfg.Partitioner = &partition.RoundRobin{} // rescale refuses this, so Start fails
	e2 := New(cfg)
	if err := e2.Start(context.Background()); err == nil {
		t.Fatal("expected Start to fail")
	}
	e2.Stop()

	bad := testCfg(t.TempDir(), newCollector())
	bad.Window.Size = 0
	e3 := New(bad)
	if err := e3.Start(context.Background()); err == nil {
		t.Fatal("expected invalid window config to be rejected")
	}
	e3.Stop()
}

func TestOversizedBatchIsPermanentError(t *testing.T) {
	cfg := testCfg(t.TempDir(), newCollector())
	cfg.MaxInflight = 50
	e := New(cfg)
	if err := e.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer e.Stop()
	err := e.Submit(context.Background(), "p", gen(60))
	if !errors.Is(err, ErrBatchTooLarge) {
		t.Fatalf("got %v", err)
	}
}

// A checkpoint that fails after the sink staged its output must not make that
// output unreachable: the next snapshot has to carry it. Output e1 emits dies
// with it (separate collector), so only the restore path can supply it.
func TestFailedCheckpointOutputSurvivesNextOne(t *testing.T) {
	dir := t.TempDir()
	evs := gen(6000)

	base := newCollector()
	eb := New(testCfg(t.TempDir(), base))
	if err := eb.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	feed(t, eb, evs)
	finish(t, eb)
	eb.Stop()

	e1 := New(testCfg(dir, newCollector()))
	if err := e1.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	feed(t, e1, evs)
	if err := e1.AdvanceWatermark(context.Background(), 1<<40); err != nil {
		t.Fatal(err)
	}

	// fails after the barrier, when the sink has already staged this epoch
	e1.store.FailCommitForTest(true)
	if _, err := e1.Checkpoint(context.Background()); err == nil {
		t.Fatal("expected the injected failure")
	}
	e1.store.FailCommitForTest(false)
	if _, err := e1.Checkpoint(context.Background()); err != nil {
		t.Fatal(err)
	}
	id, _ := e1.store.Latest()
	e1.Stop()
	os.Remove(filepath.Join(dir, fmt.Sprintf("chk-%08d", id), "_EMITTED"))

	got := newCollector()
	e2 := New(testCfg(dir, got))
	if err := e2.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	e2.Stop()

	if got.n != base.n {
		t.Fatalf("recovered %d results, want %d", got.n, base.n)
	}
	for k, w := range base.res {
		if g, ok := got.res[k]; !ok || g.Count != w.Count {
			t.Fatalf("window %s lost or wrong: %+v want %+v", k, g, w)
		}
	}
}
