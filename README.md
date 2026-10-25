# AMENAH SALEH

[![ci](https://github.com/monaaoua9/amenah-saleh/actions/workflows/ci.yml/badge.svg)](https://github.com/monaaoua9/amenah-saleh/actions/workflows/ci.yml)

High-throughput distributed stream processing engine. Author: AMENAH SALEH.

A small distributed-style stream processing engine in Go: gRPC ingestion,
event-time windows with watermarks, credit-based backpressure, and
Chandy-Lamport style checkpoints with exactly-once results across a crash.

Runs in one process (goroutines and channels) or with the window workers split
out into separate processes that talk to a master over gRPC.

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

## Cluster demo

```
dspe -role master -expect 3 -ckdir ./ck      # ingest + coordinator on :7070
dspe -role worker -master localhost:7070     # run three of these
loadgen -c 4 -d 20s                          # producers talk to the master
```

Kill a worker while loadgen runs: the master logs `engine failed ... recovering`,
producers see `Unavailable`, and once a replacement node connects the engine
restores from the last checkpoint (producers replay; `Seq` dedup does the rest).

## What it does

- **Ingestion**: bidirectional gRPC stream, protobuf. A producer is pinned to a
  lane; events are routed to workers by `Hash(key)` or `RoundRobin`.
- **Event time**: tumbling, sliding and session windows, bounded out-of-orderness
  watermark, allowed lateness with refire, late events counted and dropped.
  Manual watermark punctuation uses the min across lanes.
- **Cluster mode**: window workers can run as separate processes. A master owns
  ingest, lanes, the sink and checkpoints; worker nodes dial in over gRPC, get
  their state from the master and send results back on the same stream. If a
  node dies the master notices, waits for a replacement, and restores everyone
  from the last checkpoint.
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
| In-process pipeline (`make bench`, 4 workers) | ~4-5M events/s, ~150 B and 0 allocs per event, batch P99 ~10-17 ms |
| Window operator `Add` | ~130-150 ns/event, 0 allocs |
| gRPC, 4 clients, batch 500 (`make stress`) | ~0.9M events/s, ack P99 ~4 ms |

The latency histogram is log-bucketed, so quantiles are within ~25%.

## Not done

- A crash between sink emit and its marker re-sends that one epoch, so the
  guarantee for it is at-least-once; downstream dedups on (key, window start).
- The master is a single point of failure (no leader election), and recovery is
  a full rollback to the last checkpoint, not a per-worker restart.
- Worker streams are plaintext gRPC with no auth.
- Not tested under the race detector yet (needs cgo).
