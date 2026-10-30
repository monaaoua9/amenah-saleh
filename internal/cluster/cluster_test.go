package cluster

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/monaaoua9/amenah-saleh/internal/cluster/pb"
	"github.com/monaaoua9/amenah-saleh/internal/engine"
	"github.com/monaaoua9/amenah-saleh/internal/event"
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
		evs[i] = event.Event{Key: fmt.Sprintf("k%d", i%23), Value: float64(i % 7), Time: int64(i) * 10, Seq: uint64(i + 1)}
	}
	return evs
}

func cfg(dir string, c *collector) engine.Config {
	return engine.Config{
		Lanes: 2, Workers: 3, QueueDepth: 8, MaxInflight: 4096,
		Window:        window.Config{Size: 1000},
		CheckpointDir: dir,
		OnResult:      c.add,
	}
}

func feed(t *testing.T, e *engine.Engine, evs []event.Event) {
	t.Helper()
	for i := 0; i < len(evs); i += 100 {
		if err := e.Submit(context.Background(), "p", evs[i:min(i+100, len(evs))]); err != nil {
			t.Fatal(err)
		}
	}
}

func finish(t *testing.T, e *engine.Engine) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := e.AdvanceWatermark(ctx, 1<<40); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Checkpoint(ctx); err != nil {
		t.Fatal(err)
	}
}

func startMaster(t *testing.T) (*Hub, string) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	hub := NewHub()
	g := grpc.NewServer(grpc.MaxRecvMsgSize(engine.MaxMsgSize), grpc.MaxSendMsgSize(engine.MaxMsgSize))
	pb.RegisterClusterServer(g, hub)
	go g.Serve(lis)
	t.Cleanup(g.Stop)
	return hub, lis.Addr().String()
}

// like the real worker process: reconnect until told to stop
func startNode(addr string) context.CancelFunc {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		for ctx.Err() == nil {
			engine.RunWorkerNode(ctx, addr)
			time.Sleep(30 * time.Millisecond)
		}
	}()
	return cancel
}

func remoteEngine(t *testing.T, hub *Hub, n int, dir string, c *collector) *engine.Engine {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conns, err := hub.Wait(ctx, n)
	if err != nil {
		t.Fatal(err)
	}
	cf := cfg(dir, c)
	cf.Remote = conns
	e := engine.New(cf)
	if err := e.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	return e
}

func baseline(t *testing.T, evs []event.Event) *collector {
	t.Helper()
	c := newCollector()
	e := engine.New(cfg(t.TempDir(), c))
	if err := e.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	feed(t, e, evs)
	finish(t, e)
	e.Stop()
	return c
}

func same(t *testing.T, got, want *collector) {
	t.Helper()
	if got.n != want.n {
		t.Fatalf("emitted %d results, want %d", got.n, want.n)
	}
	for k, w := range want.res {
		g, ok := got.res[k]
		if !ok || g.Count != w.Count || g.Sum != w.Sum {
			t.Fatalf("window %s: got %+v want %+v", k, g, w)
		}
	}
}

func TestRemoteWorkersMatchLocal(t *testing.T) {
	evs := gen(15000)
	want := baseline(t, evs)

	hub, addr := startMaster(t)
	for i := 0; i < 3; i++ {
		defer startNode(addr)()
	}
	got := newCollector()
	e := remoteEngine(t, hub, 3, t.TempDir(), got)
	feed(t, e, evs)
	finish(t, e)
	e.Stop()

	same(t, got, want)
}

func TestWorkerLossThenRecovery(t *testing.T) {
	evs := gen(20000)
	want := baseline(t, evs)

	hub, addr := startMaster(t)
	dir := t.TempDir()
	got := newCollector()

	kill := make([]context.CancelFunc, 3)
	for i := range kill {
		kill[i] = startNode(addr)
		defer kill[i]()
	}
	e1 := remoteEngine(t, hub, 3, dir, got)
	feed(t, e1, evs[:12000])
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if _, err := e1.Checkpoint(ctx); err != nil {
		t.Fatal(err)
	}
	feed(t, e1, evs[12000:15000])

	kill[1]() // a worker node dies
	select {
	case err := <-e1.Failed():
		t.Logf("detected: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("master never noticed the lost worker")
	}
	e1.Stop()

	// the two survivors reconnect on their own; add a replacement for the dead one
	defer startNode(addr)()
	e2 := remoteEngine(t, hub, 3, dir, got)
	feed(t, e2, evs) // producer replays, dedup skips what the checkpoint already covers
	finish(t, e2)
	e2.Stop()

	same(t, got, want)
}

func TestSubmitAfterFailureDoesNotHang(t *testing.T) {
	hub, addr := startMaster(t)
	kill := make([]context.CancelFunc, 3)
	for i := range kill {
		kill[i] = startNode(addr)
		defer kill[i]()
	}
	cf := cfg(t.TempDir(), newCollector())
	cf.MaxInflight = 200
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conns, err := hub.Wait(ctx, 3)
	if err != nil {
		t.Fatal(err)
	}
	cf.Remote = conns
	e := engine.New(cf)
	if err := e.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer e.Stop()

	kill[0]()
	<-e.Failed()

	done := make(chan error, 1)
	go func() {
		var err error
		for i := 0; i < 50 && err == nil; i++ {
			err = e.Submit(context.Background(), "p", gen(150))
		}
		done <- err
	}()
	select {
	case err := <-done:
		if err != engine.ErrStopped {
			t.Fatalf("got %v, want ErrStopped", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Submit hung on a failed engine")
	}
}

func TestRemoteRescale(t *testing.T) {
	evs := gen(20000)
	want := baseline(t, evs)

	hub, addr := startMaster(t)
	dir := t.TempDir()
	got := newCollector()

	kill := make([]context.CancelFunc, 3)
	for i := range kill {
		kill[i] = startNode(addr)
		defer kill[i]()
	}
	e1 := remoteEngine(t, hub, 3, dir, got)
	feed(t, e1, evs[:12000])
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if _, err := e1.Checkpoint(ctx); err != nil {
		t.Fatal(err)
	}
	e1.Stop()

	kill[2]() // come back with two nodes instead of three
	e2 := remoteEngine(t, hub, 2, dir, got)
	feed(t, e2, evs)
	finish(t, e2)
	e2.Stop()

	same(t, got, want)
}

// quantiles, sum of squares and window config all have to cross the wire
func TestRemoteCarriesQuantilesAndSumSq(t *testing.T) {
	evs := gen(3000)
	withQ := func(c *engine.Config) { c.Window.Quantiles = true }

	local := newCollector()
	lc := cfg(t.TempDir(), local)
	withQ(&lc)
	le := engine.New(lc)
	if err := le.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	feed(t, le, evs)
	finish(t, le)
	le.Stop()

	hub, addr := startMaster(t)
	for range 3 {
		defer startNode(addr)()
	}
	got := newCollector()
	rc := cfg(t.TempDir(), got)
	withQ(&rc)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conns, err := hub.Wait(ctx, 3)
	if err != nil {
		t.Fatal(err)
	}
	rc.Remote = conns
	re := engine.New(rc)
	if err := re.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	feed(t, re, evs)
	finish(t, re)
	re.Stop()

	if got.n != local.n || got.n == 0 {
		t.Fatalf("remote %d results, local %d", got.n, local.n)
	}
	for k, w := range local.res {
		g := got.res[k]
		if g.SumSq != w.SumSq || g.P50 != w.P50 || g.P95 != w.P95 || g.P99 != w.P99 {
			t.Fatalf("window %s: remote %+v local %+v", k, g, w)
		}
	}
	for _, r := range got.res {
		if r.P99 == 0 && r.Max > 0 {
			t.Fatalf("quantiles missing: %+v", r)
		}
	}
}
