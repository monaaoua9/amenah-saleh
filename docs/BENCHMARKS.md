# streamdb: benchmarks

Measured 2026-10-29.

> **Stale for the in-process numbers.** These were taken at commit `5ff7cb2`,
> before sum of squares, quantile and distinct state, and the window timer heap
> were added. A back-to-back comparison on 2026-10-30 (old and new binaries
> alternating on the same laptop) showed the current code about 10-25% slower
> on the pipeline benchmark (~200 B/event instead of ~150) and about 10% slower
> on `Add`. The laptop then started throttling, so there is no clean rerun yet.
> The gRPC and cluster figures were not re-compared like this.

| | |
|---|---|
| CPU | Intel i5-8365U, 4 cores / 8 threads, 1.6 GHz base |
| RAM / disk | 8 GB, SSD |
| OS | Windows 11 |
| Toolchain | Go 1.27.0 |

## In-process pipeline

```
go test -run xxx -bench . -benchmem -benchtime 3s -count 3 ./internal/engine ./internal/window
```

Events go through ingest, lanes, workers and the window operator into the sink, with no network. About 150 bytes per event, 0 allocs/op. Three runs; median with min–max in brackets.

| workers | events/s | batch p99 |
|---:|---:|---:|
| 1 | 3,510,519 (3,255,349 – 3,591,613) | 25 ms (25 – 29) |
| 4 | 4,474,929 (4,436,718 – 4,612,752) | 12.6 ms (10.5 – 14.7) |
| 8 | 5,111,711 (5,056,836 – 5,158,730) | 14.7 ms (10.5 – 16.8) |

The window operator's `Add` alone: 80 ns/event (79 – 82), 0 allocations.

## gRPC ingestion

```
go build -o engine ./cmd/engine && go build -o loadgen ./cmd/loadgen
./engine &            # defaults, listens on :7070
./loadgen -d 10s -c 4 # 4 producers, 500 events per batch
```

Engine and load generator on the same laptop. Three 10 s runs:

| run | events/s | ack p50 | ack p99 |
|---:|---:|---:|---:|
| 1 | 1,296,184 | 1.05 ms | 3.67 ms |
| 2 | 1,333,329 | 1.05 ms | 2.62 ms |
| 3 | 1,246,938 | 1.05 ms | 2.62 ms |

Median 1,296,184 events/s (1.25M – 1.33M).

## Reading these numbers

- Latency quantiles come from a log-bucketed histogram, so they're accurate to about 25%; that's why p50 reads exactly 1.048576 ms in every run (a bucket edge). Don't read differences between runs under that as real.
- The in-process number is a ceiling for the pipeline code. It ignores the network, serialization and checkpoint I/O. The gRPC run is the closer one to real use, and it's about 3.4× lower than the 4-worker in-process figure.
- Both load generator and engine share four cores here, so the gRPC figure underestimates what a separate client machine would allow.
- Scaling from 1 to 8 workers gives about 1.5×, not 8×. That fits a laptop with 4 physical cores, but I haven't profiled where the rest goes.
- The README's window `Add` figure (130–150 ns) is from an earlier run; this one is lower (80 ns). I haven't investigated why.

## Not rerun today

The cluster-mode figure in the README (master plus three worker processes on one machine, about 0.8M events/s) and checkpoint/recovery timings. This file doesn't compare streamdb with Flink or anything else.
