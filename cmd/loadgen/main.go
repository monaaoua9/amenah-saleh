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
)

func main() {
	addr := flag.String("addr", "localhost:7070", "")
	conns := flag.Int("c", 4, "parallel producers")
	batch := flag.Int("batch", 500, "events per batch")
	dur := flag.Duration("d", 10*time.Second, "run time")
	keys := flag.Int("keys", 1000, "distinct keys")
	jitter := flag.Int64("jitter", 300, "event time jitter, ms")
	flag.Parse()

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
			c, err := ingest.Dial(context.Background(), *addr, grpcInsecure())
			if err != nil {
				log.Print(err)
				return
			}
			defer c.Close()

			name := fmt.Sprintf("p%d", p)
			rng := rand.New(rand.NewSource(int64(p)))
			var seq uint64
			for ctx.Err() == nil {
				now := time.Now().UnixMilli()
				evs := make([]event.Event, *batch)
				for i := range evs {
					seq++
					evs[i] = event.Event{
						Key:   fmt.Sprintf("k%d", rng.Intn(*keys)),
						Value: rng.Float64(),
						Time:  now - rng.Int63n(*jitter+1),
						Seq:   seq,
					}
				}
				t0 := time.Now()
				if err := c.Send(name, evs); err != nil {
					log.Print(err)
					return
				}
				if _, err := c.Recv(); err != nil {
					log.Print(err)
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
