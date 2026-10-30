package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/monaaoua9/amenah-saleh/internal/event"
	"github.com/monaaoua9/amenah-saleh/internal/ingest"
	"github.com/monaaoua9/amenah-saleh/internal/metrics"
	"github.com/monaaoua9/amenah-saleh/internal/security"
)

func main() {
	addr := flag.String("addr", "localhost:7070", "")
	conns := flag.Int("c", 4, "parallel producers")
	batch := flag.Int("batch", 500, "events per batch")
	dur := flag.Duration("d", 10*time.Second, "run time")
	keys := flag.Int("keys", 1000, "distinct keys")
	jitter := flag.Int64("jitter", 300, "event time jitter, ms")
	token := flag.String("token", "", "bearer token the server expects")
	tlsOn := flag.Bool("tls", false, "connect with TLS")
	ca := flag.String("ca", "", "CA or self-signed cert to trust (implies -tls)")
	flag.Parse()

	dialOpts, err := security.DialOptions(*token, *tlsOn, *ca)
	if err != nil {
		log.Fatal(err)
	}

	var sent atomic.Uint64
	var rtt metrics.Histogram
	ctx, cancel := context.WithTimeout(context.Background(), *dur)
	defer cancel()

	var wg sync.WaitGroup
	start := time.Now()
	for p := 0; p < *conns; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			prod := ingest.NewProducer(*addr, fmt.Sprintf("p%d", p), dialOpts...)
			defer prod.Close()

			rng := rand.New(rand.NewSource(int64(p)))
			for ctx.Err() == nil {
				now := time.Now().UnixMilli()
				evs := make([]event.Event, *batch)
				for i := range evs {
					evs[i] = event.Event{
						Key:   fmt.Sprintf("k%d", rng.Intn(*keys)),
						Value: rng.Float64(),
						Time:  now - rng.Int63n(*jitter+1),
					}
				}
				t0 := time.Now()
				if err := prod.Send(ctx, evs); err != nil {
					if ctx.Err() == nil {
						log.Print(err)
					}
					return
				}
				rtt.Observe(time.Since(t0))
				sent.Add(uint64(len(evs)))
			}
		}(p)
	}
	wg.Wait()

	el := time.Since(start).Seconds()
	fmt.Printf("events=%d  %.0f ev/s  ack p50=%v p99=%v\n",
		sent.Load(), float64(sent.Load())/el, rtt.Quantile(0.5), rtt.Quantile(0.99))
}
