package main

import (
	"context"
	"flag"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/monaaoua9/amenah-saleh/internal/cluster"
	"github.com/monaaoua9/amenah-saleh/internal/cluster/pb"
	"github.com/monaaoua9/amenah-saleh/internal/engine"
	"github.com/monaaoua9/amenah-saleh/internal/event"
	"github.com/monaaoua9/amenah-saleh/internal/ingest"
	"github.com/monaaoua9/amenah-saleh/internal/window"
)

// The engine gets rebuilt after a worker loss, so ingest and metrics look it
// up here instead of holding a pointer.
type current struct{ p atomic.Pointer[engine.Engine] }

func (c *current) Submit(ctx context.Context, producer string, evs []event.Event) error {
	e := c.p.Load()
	if e == nil {
		return engine.ErrStopped
	}
	return e.Submit(ctx, producer, evs)
}

func (c *current) TrySubmit(ctx context.Context, producer string, evs []event.Event) error {
	e := c.p.Load()
	if e == nil {
		return engine.ErrStopped
	}
	return e.TrySubmit(ctx, producer, evs)
}

func (c *current) Committed(producer string) uint64 {
	if e := c.p.Load(); e != nil {
		return e.Committed(producer)
	}
	return 0
}

func main() {
	role := flag.String("role", "standalone", "standalone | master | worker")
	addr := flag.String("addr", ":7070", "grpc listen addr (standalone, master)")
	masterAddr := flag.String("master", "localhost:7070", "master address (worker)")
	expect := flag.Int("expect", 2, "worker nodes the master waits for")
	lanes := flag.Int("lanes", 2, "ingest lanes")
	workers := flag.Int("workers", 4, "in-process window workers (standalone)")
	inflight := flag.Int64("inflight", 1<<17, "max in-flight events")
	size := flag.Int64("window", 1000, "window size, ms")
	slide := flag.Int64("slide", 0, "slide for sliding windows, ms (0 = tumbling)")
	gap := flag.Int64("gap", 0, "session gap, ms (> 0 switches to session windows)")
	lateness := flag.Int64("lateness", 2000, "allowed lateness, ms")
	maxJump := flag.Int64("maxjump", 0, "drop events further than this many ms ahead of the newest seen, 0 = off")
	disorder := flag.Int64("disorder", 500, "max out-of-orderness, ms")
	dir := flag.String("ckdir", "./checkpoints", "checkpoint dir")
	every := flag.Duration("ckevery", 5*time.Second, "checkpoint interval")
	maxPending := flag.Int("maxpending", 100000, "staged results that trigger an early checkpoint, 0 = off")
	shed := flag.Bool("shed", false, "reject instead of stalling when overloaded")
	metricsAddr := flag.String("metrics", "", "prometheus /metrics listen addr, off when empty")
	pprofAddr := flag.String("pprof", "", "pprof listen addr, off when empty")
	flag.Parse()

	if *pprofAddr != "" {
		go http.ListenAndServe(*pprofAddr, nil)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if *role == "worker" {
		runWorker(ctx, *masterAddr)
		return
	}

	var out atomic.Uint64
	base := engine.Config{
		Lanes: *lanes, Workers: *workers, MaxInflight: *inflight,
		Window:        window.Config{Size: *size, Slide: *slide, Gap: *gap, Lateness: *lateness, Disorder: *disorder, MaxJump: *maxJump},
		CheckpointDir: *dir, CheckpointEvery: *every, MaxPending: *maxPending,
		OnResult: func(r event.Result) { out.Add(1) },
	}

	cur := &current{}
	g := grpc.NewServer(grpc.MaxRecvMsgSize(engine.MaxMsgSize), grpc.MaxSendMsgSize(engine.MaxMsgSize))
	ingest.NewServer(cur, *shed).Register(g)
	var hub *cluster.Hub
	if *role == "master" {
		hub = cluster.NewHub()
		pb.RegisterClusterServer(g, hub)
	}

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatal(err)
	}
	go g.Serve(lis)
	log.Printf("%s listening on %s", *role, *addr)

	if *metricsAddr != "" {
		mux := http.NewServeMux()
		mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
			if e := cur.p.Load(); e != nil {
				e.WriteMetrics(w)
			}
		})
		go http.ListenAndServe(*metricsAddr, mux)
	}

	for ctx.Err() == nil {
		cfg := base
		if hub != nil {
			log.Printf("waiting for %d worker nodes", *expect)
			conns, err := hub.Wait(ctx, *expect)
			if err != nil {
				break
			}
			cfg.Remote = conns
		}

		eng := engine.New(cfg)
		if err := eng.Start(ctx); err != nil {
			log.Printf("start failed: %v", err)
			eng.Stop()
			time.Sleep(time.Second)
			continue
		}
		cur.p.Store(eng)

		if !serve(ctx, eng, &out) {
			break
		}
		// a worker died: drop the engine, nodes reconnect, restore from the last checkpoint
		cur.p.Store(nil)
		eng.Stop()
	}
	g.GracefulStop()
}

// returns true when the engine failed and should be rebuilt, false on shutdown
func serve(ctx context.Context, eng *engine.Engine, out *atomic.Uint64) bool {
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := eng.Shutdown(sctx); err != nil {
				log.Printf("final checkpoint failed: %v", err)
			}
			return false
		case err := <-eng.Failed():
			log.Printf("engine failed: %v; recovering", err)
			return true
		case <-t.C:
			s := eng.Stats()
			log.Printf("in=%d dup=%d late=%d out=%d p99=%v", s.Ingested, s.Dups, s.Late, out.Load(), s.P99)
		}
	}
}

func runWorker(ctx context.Context, master string) {
	for ctx.Err() == nil {
		log.Printf("connecting to master %s", master)
		if err := engine.RunWorkerNode(ctx, master); err != nil {
			log.Printf("worker stream ended: %v", err)
		}
		select {
		case <-ctx.Done():
		case <-time.After(time.Second):
		}
	}
}
