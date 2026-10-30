package main

import (
	"context"
	"flag"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/monaaoua9/amenah-saleh/internal/alert"
	"github.com/monaaoua9/amenah-saleh/internal/api"
	"github.com/monaaoua9/amenah-saleh/internal/cluster"
	"github.com/monaaoua9/amenah-saleh/internal/cluster/pb"
	"github.com/monaaoua9/amenah-saleh/internal/config"
	"github.com/monaaoua9/amenah-saleh/internal/engine"
	"github.com/monaaoua9/amenah-saleh/internal/event"
	"github.com/monaaoua9/amenah-saleh/internal/filter"
	"github.com/monaaoua9/amenah-saleh/internal/ingest"
	"github.com/monaaoua9/amenah-saleh/internal/output"
	"github.com/monaaoua9/amenah-saleh/internal/security"
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

func (c *current) Engine() *engine.Engine { return c.p.Load() }

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
	outPath := flag.String("out", "", "append results as JSON lines to this file")
	outMax := flag.Int64("outmax", 64<<20, "roll the -out file after this many bytes, 0 = never")
	webhook := flag.String("webhook", "", "POST result batches to this url")
	httpAddr := flag.String("http", "", "http api (/results, /stats, /healthz, /readyz, /metrics), off when empty")
	rate := flag.Float64("rate", 0, "per-producer limit, events/sec, 0 = off")
	token := flag.String("token", "", "require this bearer token on every rpc; workers must pass the same")
	tlsCert := flag.String("tls-cert", "", "serve grpc over TLS (with -tls-key); workers: trust this cert")
	tlsKey := flag.String("tls-key", "", "TLS private key")
	filterSrc := flag.String("filter", "", "drop events that do not match, e.g. value >= 0 && key ~ 'sensor-'")
	alertRules := flag.String("alert", "", "alert rules separated by ;, e.g. 'avg > 80 key=cpu-; count < 3'")
	alertCool := flag.Int64("alertcooldown", 60000, "ms of event time before the same rule fires again for a key")
	alertHook := flag.String("alerthook", "", "POST alerts as JSON to this url (they are always logged)")
	ckGzip := flag.Bool("ckgzip", false, "gzip checkpoint snapshots")
	shed := flag.Bool("shed", false, "reject instead of stalling when overloaded")
	metricsAddr := flag.String("metrics", "", "prometheus /metrics listen addr, off when empty")
	pprofAddr := flag.String("pprof", "", "pprof listen addr, off when empty")
	cfgPath := flag.String("config", "", "json file of flag values; command line flags win")
	flag.Parse()
	if *cfgPath != "" {
		if err := config.Load(flag.CommandLine, *cfgPath); err != nil {
			log.Fatal(err)
		}
	}

	if *pprofAddr != "" {
		go http.ListenAndServe(*pprofAddr, nil)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if *role == "worker" {
		dopts, err := security.DialOptions(*token, *tlsCert != "", *tlsCert)
		if err != nil {
			log.Fatal(err)
		}
		runWorker(ctx, *masterAddr, dopts)
		return
	}

	var out atomic.Uint64
	sinks := []output.Sink{output.Func(func(event.Result) { out.Add(1) })}
	recent := api.NewRecent(10000)
	sinks = append(sinks, recent)
	if *alertRules != "" {
		var rules []alert.Rule
		for _, s := range strings.Split(*alertRules, ";") {
			r, err := alert.ParseRule(strings.TrimSpace(s))
			if err != nil {
				log.Fatal(err)
			}
			rules = append(rules, r)
		}
		notify := func(a alert.Alert) {
			log.Printf("ALERT %s: %g on %s [%d,%d)", a.Rule, a.Value, a.Result.Key, a.Result.Start, a.Result.End)
		}
		if *alertHook != "" {
			h := alert.NewHook(*alertHook)
			defer h.Close()
			notify = func(a alert.Alert) {
				log.Printf("ALERT %s: %g on %s", a.Rule, a.Value, a.Result.Key)
				h.Notify(a)
			}
		}
		sinks = append(sinks, alert.NewEvaluator(rules, *alertCool, notify))
	}
	if *outPath != "" {
		f, err := output.NewFile(*outPath, *outMax)
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		sinks = append(sinks, f)
	}
	if *webhook != "" {
		wh := output.NewWebhook(*webhook, 500, time.Second)
		defer wh.Close()
		sinks = append(sinks, wh)
	}
	base := engine.Config{
		Lanes: *lanes, Workers: *workers, MaxInflight: *inflight,
		Window:        window.Config{Size: *size, Slide: *slide, Gap: *gap, Lateness: *lateness, Disorder: *disorder, MaxJump: *maxJump},
		CheckpointDir: *dir, CheckpointEvery: *every, CheckpointCompress: *ckGzip, MaxPending: *maxPending,
		OnResult: output.Fanout(sinks...),
	}

	cur := &current{}
	secOpts, err := security.ServerOptions(*token, *tlsCert, *tlsKey)
	if err != nil {
		log.Fatal(err)
	}
	g := grpc.NewServer(append(secOpts, grpc.MaxRecvMsgSize(engine.MaxMsgSize), grpc.MaxSendMsgSize(engine.MaxMsgSize))...)
	is := ingest.NewServer(cur, *shed)
	if *filterSrc != "" {
		f, err := filter.Parse(*filterSrc)
		if err != nil {
			log.Fatal(err)
		}
		is.WithFilter(f)
	}
	if *rate > 0 {
		is.WithRate(*rate, int(*rate)) // one second of burst
	}
	is.Register(g)
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

	if *httpAddr != "" {
		go http.ListenAndServe(*httpAddr, api.Handler(cur, recent))
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

func runWorker(ctx context.Context, master string, dopts []grpc.DialOption) {
	for ctx.Err() == nil {
		log.Printf("connecting to master %s", master)
		if err := engine.RunWorkerNode(ctx, master, dopts...); err != nil {
			log.Printf("worker stream ended: %v", err)
		}
		select {
		case <-ctx.Done():
		case <-time.After(time.Second):
		}
	}
}
