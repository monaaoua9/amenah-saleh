# streamdb: benchmarks

Measured 2026-10-31, one terminal open, nothing else running.

| | |
|---|---|
| CPU | Intel i5-8365U, 4 cores / 8 threads, 1.6 GHz base |
| RAM / disk | 8 GB, SSD |
| OS | Windows 11 |
| Toolchain | Go 1.27.0 |

## In-process pipeline

```
go test -run xxx -bench . -benchmem -benchtime 2s -count 3 ./internal/engine ./internal/window
```

Events go through ingest, lanes, workers and the window operator into the sink, with no network. About 150 bytes per event, 0 allocs/op. Three runs; median with min–max in brackets.

| workers | events/s | batch p99 |
|---:|---:|---:|
| 1 | 5,901,996 (5,260,461 – 6,401,806) | 16.8 ms (16.8 – 21) |
| 4 | 7,743,390 (7,279,996 – 7,749,008) | 14.7 ms (8.4 – 16.8) |
| 8 | 6,577,876 (5,898,007 – 7,008,678) | 14.7 ms (12.6 – 16.8) |

The window operator's `Add` alone: 44 ns/event (43 – 44), 0 allocations.

## gRPC ingestion

```
go build -o engine ./cmd/engine && go build -o loadgen ./cmd/loadgen
./engine &            # defaults, listens on :7070
./loadgen -d 10s -c 4 # 4 producers, 500 events per batch
```

Engine and load generator on the same laptop. Three 10 s runs:

| run | events/s | ack p50 | ack p99 |
|---:|---:|---:|---:|
| 1 | 1,761,953 | 0.66 ms | 2.10 ms |
| 2 | 1,480,669 | 1.05 ms | 2.10 ms |
| 3 | 1,419,187 | 1.05 ms | 2.10 ms |

Median 1,480,669 events/s (1.42M – 1.76M).

## Reading these numbers

- Latency quantiles come from a log-bucketed histogram, so they're accurate to about 25%; that's why p50 reads exactly 1.048576 ms in every run (a bucket edge). Don't read differences between runs under that as real.
- The in-process number is a ceiling for the pipeline code. It ignores the network, serialization and checkpoint I/O. The gRPC run is the closer one to real use, and it's about 5× lower than the 4-worker in-process figure.
- Both load generator and engine share four cores here, so the gRPC figure underestimates what a separate client machine would allow.
- Scaling from 1 to 4 workers gives about 1.3×, and 8 workers is no better than 4. That fits a laptop with 4 physical cores, but I haven't profiled where the rest goes.

## Not rerun today

The cluster-mode figure in the README (master plus three worker processes on one machine, about 0.8M events/s) and checkpoint/recovery timings. This file doesn't compare streamdb with Flink or anything else.

## Operator and checkpoint micro-benchmarks, after the optimization pass

Measured 2026-10-30 on the same laptop, which was busy (six terminals open), so
read these as ratios, not absolute figures. Old and new binaries ran alternately,
five rounds; the numbers are the best round of each. The old side is the commit
before the change being measured.

| benchmark | before | after |
|---|---:|---:|
| `BenchmarkAddAdvance` (tumbling, 256 keys, Advance every 512 events) | 127 ns/event | 77 ns/event |
| `BenchmarkAddAdvanceSliding` (4 windows per event) | 299 ns/event | 119 ns/event |
| `BenchmarkSnapshot` (~100k open windows) | 374 ms (gob) | 28 ms (binary) |
| `BenchmarkEncodeResults` (10k staged results) | 64 ms (gob) | 10 ms (binary) |

What changed: open windows are grouped by key (the key is hashed once per event
instead of once per window), timers are bucketed by due time, and the snapshot
and the staged sink output use a flat binary format instead of gob. Commands:

```
go test -run xxx -bench 'AddAdvance|Snapshot|EncodeResults' ./internal/window ./internal/engine
```

The figures at the top were measured after these changes.
