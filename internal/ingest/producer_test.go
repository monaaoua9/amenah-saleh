package ingest

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/monaaoua9/amenah-saleh/internal/engine"
	"github.com/monaaoua9/amenah-saleh/internal/event"
	"github.com/monaaoua9/amenah-saleh/internal/window"
)

type collector struct {
	mu  sync.Mutex
	res map[string]event.Result
	n   int
}

func (c *collector) add(r event.Result) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.res[fmt.Sprintf("%s@%d", r.Key, r.Start)] = r
	c.n++
}

func engCfg(dir string, c *collector) engine.Config {
	return engine.Config{
		Lanes: 2, Workers: 3, QueueDepth: 8, MaxInflight: 4096,
		Window:        window.Config{Size: 1000},
		CheckpointDir: dir, CheckpointEvery: 20 * time.Millisecond,
		OnResult: c.add,
	}
}

type srv struct {
	eng *engine.Engine
	g   *grpc.Server
}

func (s *srv) crash() {
	s.g.Stop()
	s.eng.Stop()
}

func startSrv(addr, dir string, c *collector) (*srv, string, error) {
	var lis net.Listener
	var err error
	for i := 0; i < 40; i++ { // the old listener may take a moment to release the port
		if lis, err = net.Listen("tcp", addr); err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		return nil, "", err
	}
	eng := engine.New(engCfg(dir, c))
	if err := eng.Start(context.Background()); err != nil {
		return nil, "", err
	}
	g := grpc.NewServer()
	NewServer(eng, false).Register(g)
	go g.Serve(lis)
	return &srv{eng, g}, lis.Addr().String(), nil
}

func TestProducerReplaysAfterServerCrash(t *testing.T) {
	const total = 9000
	evs := make([]event.Event, total)
	for i := range evs {
		evs[i] = event.Event{Key: fmt.Sprintf("k%d", i%19), Value: float64(i % 6), Time: int64(i) * 10}
	}

	// reference run: same events, same seqs, no failure
	want := &collector{res: map[string]event.Result{}}
	ref := engine.New(engCfg(t.TempDir(), want))
	if err := ref.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < total; i += 100 {
		b := append([]event.Event(nil), evs[i:i+100]...)
		for j := range b {
			b[j].Seq = uint64(i + j + 1)
		}
		if err := ref.Submit(context.Background(), "p", b); err != nil {
			t.Fatal(err)
		}
	}
	ref.AdvanceWatermark(context.Background(), 1<<40)
	if _, err := ref.Checkpoint(context.Background()); err != nil {
		t.Fatal(err)
	}
	ref.Stop()

	dir := t.TempDir()
	got := &collector{res: map[string]event.Result{}}
	s1, addr, err := startSrv("127.0.0.1:0", dir, got)
	if err != nil {
		t.Fatal(err)
	}
	prod := NewProducer(addr, "p", grpc.WithTransportCredentials(insecure.NewCredentials()))
	defer prod.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	send := func(from, to int) {
		t.Helper()
		for i := from; i < to; i += 100 {
			if err := prod.Send(ctx, append([]event.Event(nil), evs[i:i+100]...)); err != nil {
				t.Fatal(err)
			}
		}
	}

	send(0, 5000)
	deadline := time.Now().Add(5 * time.Second)
	for s1.eng.Committed("p") == 0 {
		if time.Now().After(deadline) {
			t.Fatal("no checkpoint covered the producer")
		}
		time.Sleep(10 * time.Millisecond)
	}
	send(5000, 6000)
	if prod.Unconfirmed() == 0 {
		t.Fatal("expected some uncheckpointed batches before the crash")
	}

	s1.crash()
	s2c := make(chan *srv, 1)
	errc := make(chan error, 1)
	go func() {
		time.Sleep(200 * time.Millisecond)
		s, _, err := startSrv(addr, dir, got)
		s2c <- s
		errc <- err
	}()
	send(6000, total) // blocks, retrying, until the restarted server is up
	s2 := <-s2c
	if err := <-errc; err != nil {
		t.Fatal(err)
	}
	defer s2.crash()

	s2.eng.AdvanceWatermark(ctx, 1<<40)
	if _, err := s2.eng.Checkpoint(ctx); err != nil {
		t.Fatal(err)
	}

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
