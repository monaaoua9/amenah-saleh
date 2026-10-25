# AMENAH SALEH

[![ci](https://github.com/monaaoua9/amenah-saleh/actions/workflows/ci.yml/badge.svg)](https://github.com/monaaoua9/amenah-saleh/actions/workflows/ci.yml)

High-throughput distributed stream processing engine. Author: AMENAH SALEH.

A small distributed-style stream processing engine in Go: gRPC ingestion,
event-time windows with watermarks, credit-based backpressure, and
Chandy-Lamport style checkpoints with exactly-once results across a crash.

Single process, goroutines instead of machines. The design (lanes, workers,
sink, barriers) maps onto separate nodes, but the transport between them is
channels today.

Design and the reasoning behind it: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Run

```
make test      # unit + crash-recovery + gRPC tests
make bench     # pipeline / operator benchmarks
make run       # server on :7070 (-pprof :6060 for profiling)
make stress    # gRPC load generator against a running server
make proto     # regenerate protobuf (needs protoc, protoc-gen-go, protoc-gen-go-grpc)
```

Useful server flags: `-window`, `-slide` (sliding windows), `-gap` (session windows), `-lateness`,
`-disorder`, `-inflight`, `-shed` (reject instead of stall), `-ckevery`, `-ckdir`, `-maxpending`,
`-metrics :9090` (Prometheus text at /metrics), `-pprof :6060`.

## What it does

- **Ingestion**: bidirectional gRPC stream, protobuf. A producer is pinned to a
  lane; events are routed to workers by `Hash(key)` or `RoundRobin`.
- **Event time**: tumbling, sliding and session windows, bounded out-of-orderness
  watermark, allowed lateness with refire, late events counted and dropped.
  Manual watermark punctuation uses the min across lanes.
- **Backpressure**: bounded channels plus a cap on total in-flight events.
  Credits return only after the sink accepted the output.
- **Checkpoints**: numbered barriers, aligned per task, snapshots written
  atomically, a checkpoint exists only once its marker file does.
- **Rescaling**: restore into a different number of lanes and workers. State is
  split by key group (128 fixed groups, workers own ranges) and producer
  offsets are re-homed by producer hash. Needs the `Hash` partitioner.
- **Exactly-once**: restore, producer replays, per-producer `Seq` dedup in the
  lane, sink output staged inside the checkpoint and released after it is
  durable.

## Numbers

Laptop, 8 threads, Windows, Go 1.27. These move a lot with machine load, so
treat them as ranges.

| | |
|---|---|
| In-process pipeline (`make bench`) | ~0.9-1.9M events/s, 2 allocs per event batch slot |
| Window operator `Add` | ~130-150 ns/event, 0 allocs |
| gRPC, 4 clients, batch 500 (`make stress`) | ~0.9M events/s, ack P99 ~4 ms |

The latency histogram is log-bucketed, so quantiles are within ~25%.

## Not done

- A crash between sink emit and its marker re-sends that one epoch, so the
  guarantee for it is at-least-once; downstream dedups on (key, window start).
- Single node. No networked workers, no leader election.
- Not tested under the race detector yet (needs cgo).
