package engine

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/monaaoua9/amenah-saleh/internal/event"
	"github.com/monaaoua9/amenah-saleh/internal/window"
)

func BenchmarkPipeline(b *testing.B) {
	for _, workers := range []int{1, 4, 8} {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			e := New(Config{
				Lanes: 2, Workers: workers, QueueDepth: 64, MaxInflight: 1 << 17,
				Window:        window.Config{Size: 1000, Disorder: 200},
				CheckpointDir: b.TempDir(),
				OnResult:      func(event.Result) {},
			})
			if err := e.Start(context.Background()); err != nil {
				b.Fatal(err)
			}
			defer e.Stop()

			const bs = 512
			keys := make([]string, 256)
			for i := range keys {
				keys[i] = "k" + fmt.Sprint(i)
			}
			ctx := context.Background()
			b.ResetTimer()
			start := time.Now()
			var ts int64
			for sent := 0; sent < b.N; sent += bs {
				evs := make([]event.Event, bs)
				for i := range evs {
					ts++
					evs[i] = event.Event{Key: keys[i%256], Value: 1, Time: ts}
				}
				if err := e.Submit(ctx, "bench", evs); err != nil {
					b.Fatal(err)
				}
			}
			// drain: checkpoint barrier only completes after everything before it
			if _, err := e.Checkpoint(ctx); err != nil {
				b.Fatal(err)
			}
			b.ReportMetric(float64(b.N)/time.Since(start).Seconds(), "events/s")
			b.ReportMetric(float64(e.Stats().P99.Microseconds()), "p99-batch-µs")
		})
	}
}
