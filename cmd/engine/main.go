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

	"github.com/monaaoua9/amenah-saleh/internal/engine"
	"github.com/monaaoua9/amenah-saleh/internal/event"
	"github.com/monaaoua9/amenah-saleh/internal/ingest"
	"github.com/monaaoua9/amenah-saleh/internal/window"
)

func main() {
	addr := flag.String("addr", ":7070", "grpc listen addr")
	lanes := flag.Int("lanes", 2, "ingest lanes")
	workers := flag.Int("workers", 4, "window workers")
	inflight := flag.Int64("inflight", 1<<17, "max in-flight events")
	size := flag.Int64("window", 1000, "window size, ms")
	slide := flag.Int64("slide", 0, "slide for sliding windows, ms (0 = tumbling)")
	gap := flag.Int64("gap", 0, "session gap, ms (> 0 switches to session windows)")
	lateness := flag.Int64("lateness", 2000, "allowed lateness, ms")
	disorder := flag.Int64("disorder", 500, "max out-of-orderness, ms")
	dir := flag.String("ckdir", "./checkpoints", "checkpoint dir")
	every := flag.Duration("ckevery", 5*time.Second, "checkpoint interval")
	shed := flag.Bool("shed", false, "reject instead of stalling when overloaded")
	metricsAddr := flag.String("metrics", "", "prometheus /metrics listen addr, off when empty")
	pprofAddr := flag.String("pprof", "", "pprof listen addr, off when empty")
	flag.Parse()

	if *pprofAddr != "" {
		go http.ListenAndServe(*pprofAddr, nil)
	}

	var out atomic.Uint64
	eng := engine.New(engine.Config{
		Lanes: *lanes, Workers: *workers, MaxInflight: *inflight,
		Window:        window.Config{Size: *size, Slide: *slide, Gap: *gap, Lateness: *lateness, Disorder: *disorder},
		CheckpointDir: *dir, CheckpointEvery: *every,
		OnResult: func(r event.Result) { out.Add(1) },
	})

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := eng.Start(ctx); err != nil {
		log.Fatal(err)
	}

	if *metricsAddr != "" {
		mux := http.NewServeMux()
		mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) { eng.WriteMetrics(w) })
		go http.ListenAndServe(*metricsAddr, mux)
	}

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatal(err)
	}
	g := grpc.NewServer()
	ingest.NewServer(eng, *shed).Register(g)
	go g.Serve(lis)
	log.Printf("listening on %s", *addr)

	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			g.GracefulStop()
			sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := eng.Shutdown(sctx); err != nil {
				log.Printf("final checkpoint failed: %v", err)
			}
			return
		case <-t.C:
			s := eng.Stats()
			log.Printf("in=%d dup=%d late=%d out=%d p99=%v", s.Ingested, s.Dups, s.Late, out.Load(), s.P99)
		}
	}
}
