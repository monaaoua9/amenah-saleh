| `-distinct` | add an approximate distinct-value count (HyperLogLog, ~3%) to every window result |
# AMENAH SALEH â€” Distributed Stream Processing Engine

[![ci](https://github.com/monaaoua9/amenah-saleh/actions/workflows/ci.yml/badge.svg)](https://github.com/monaaoua9/amenah-saleh/actions/workflows/ci.yml)
![go](https://img.shields.io/badge/go-1.27+-00ADD8)
![license](https://img.shields.io/badge/license-MIT-blue)

A stream processing engine in Go. Events come in over gRPC, get partitioned
across workers, are aggregated in event-time windows, and come out exactly
once, even if the process or a worker node dies in the middle.

It runs either as one process (goroutines and channels) or as a master plus
separate worker processes. Both modes run the same worker code.

```
 producers --gRPC--> ingest --> lanes --hash(key)--> workers --> sink --> results
                                  |                    |          |
                                  +---- barriers ------+----------+--> checkpoint store
```

## What's in it

| | |
|---|---|
| **Ingestion** | Bidirectional gRPC streams (protobuf). Each producer is pinned to a lane. Events are routed by `Hash(key)` or `RoundRobin`. |
| **Event time** | Tumbling, sliding and session windows. Bounded out-of-orderness watermarks, allowed lateness with re-fire, late events counted and dropped. |
| **Backpressure** | Bounded channels, plus a hard cap on in-flight events across the whole pipeline. Credits return only after the sink has taken the output, so a slow sink slows ingest. `-shed` rejects with `RESOURCE_EXHAUSTED` instead of stalling. |
| **Checkpoints** | Chandy-Lamport style barriers with per-task alignment. Snapshots are written atomically (tmp, fsync, rename); a checkpoint exists only once its marker file does. |
| **Exactly-once** | Restore from the last checkpoint, producers replay, lanes drop already-seen `Seq`. Output is staged inside the checkpoint and released only after it is durable. |
| **Rescaling** | Restore into a different number of lanes and workers. State is split by key group (128 groups, workers own ranges). |
| **Cluster mode** | Window workers run as separate processes that dial a master. A lost node is detected, a replacement is awaited, everyone is restored from the last checkpoint. |
| **Aggregates** | Count, sum, min, max, mean and stddev per window; optional p50/p95/p99 from a mergeable relative-error sketch (`-quantiles`) and distinct values (`-distinct`). |
| **Outputs** | Rolling JSONL file, batched webhook with retry, late-event side output, alert rules with cooldown. |
| **Ingest control** | Per-producer rate limit, event filter expressions, bearer-token auth and TLS. |
| **Observability** | Prometheus text endpoint, `pprof`, a lock-free latency histogram, and a small HTTP API (recent results, top keys, stats, health). |

How and why each of these works: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Quick start

Needs Go 1.27 or newer.

```
make test        # unit, crash-recovery, rescale, cluster and gRPC tests
make run         # standalone server on :7070
make stress      # in another terminal: gRPC load generator
make bench       # pipeline and operator benchmarks
```

Useful flags (`go run ./cmd/engine -h` for all of them):

| flag | meaning |
|---|---|
| `-window`, `-slide`, `-gap` | window size; slide for sliding windows; gap in ms for session windows |
| `-lateness`, `-disorder` | allowed lateness; max out-of-orderness for the watermark |
| `-workers`, `-lanes` | parallelism (standalone mode) |
| `-inflight` | cap on events admitted but not yet fully processed |
| `-shed` | reject instead of stalling when full |
| `-ckdir`, `-ckevery`, `-maxpending` | checkpoint directory, interval, and the staged-output size that forces an early checkpoint |
| `-metrics :9090`, `-pprof :6060` | Prometheus text at `/metrics`; profiling |
| `-http :8080` | JSON API: `/results?key=`, `/top?by=p99&n=10`, `/stats`, `/healthz`, `/readyz`, `/metrics` |
| `-out`, `-outmax`, `-webhook`, `-latefile` | append results as JSON lines (rolling), POST batches, log dropped-late events |
| `-quantiles` | add p50, p95, p99 to every window result |
| `-filter` | drop events at ingest, e.g. `value >= 0 && key ~ 'sensor-'` |
| `-rate` | per-producer events/sec cap |
| `-alert`, `-alertcooldown`, `-alerthook` | rules like `avg > 80 key=cpu-; p99 > 250`, quiet period in event time, optional webhook |
| `-token`, `-tls-cert`, `-tls-key` | shared bearer token and TLS for ingest and worker streams |
| `-ckgzip` | gzip checkpoint snapshots |
| `-config file.json` | any flag as a JSON key; the command line wins |

## Cluster demo

One command runs the whole thing (builds into a temp dir, cleans up after):

```
bash scripts/demo.sh        # or: make demo
```

It starts a master and three worker processes, runs two producers for 16
seconds, kills a worker after 5, and adds a replacement 3 seconds later. Output
from a run on a laptop:

```
== killing worker node 2
== starting a replacement node

== master log
master listening on :7070
waiting for 3 worker nodes
engine failed: worker 0 lost: rpc error: code = Canceled desc = context canceled; recovering
waiting for 3 worker nodes
restored from checkpoint 8

== load generator
events=7082000  442598 ev/s  ack p50=1.048576ms p99=4.194304ms
```

The run finishes without errors on the producer side: they got `Unavailable`
while the engine was down and retried by themselves. `ingest.Producer`
resends whatever the last `committed_seq` in an ack doesn't cover, and
sequence-number dedup removes the overlap. The throughput figure includes the
seconds of downtime.

By hand:

```
go run ./cmd/engine -role master -expect 3 -ckdir ./ck    # ingest + coordinator on :7070
go run ./cmd/engine -role worker -master localhost:7070   # start three of these
go run ./cmd/loadgen -c 4 -d 20s                          # producers
```

## Feeding it a file

```
go run ./cmd/engine -http :8080 -quantiles -alert 'p99 > 250 key=api-'
go run ./cmd/replay -speed 10 events.csv     # key,value,time_ms; or .jsonl
curl 'localhost:8080/top?by=p99&n=5'
```

`-speed 1` replays at the pace of the event timestamps, `0` (default) goes as
fast as the engine accepts. With `-token` or `-tls-cert` set on the server, pass
`-token` and `-tls`/`-ca` to `replay` and `loadgen`.

## Guarantees and how they're checked

These are tests, not claims. Each compares the output with an uninterrupted
run on the same input.

- `TestExactlyOnceAcrossCrash`: crash after a checkpoint, restart, replay. Same windows, same counts and sums.
- `TestStagedOutputSurvivesCrashBeforeEmit`: crash between the checkpoint and the emit; the output is re-sent on restart.
- `TestRescaleOnRestore`: restore into 7, 2 and 4 workers (with different lane counts).
- `TestWorkerLossThenRecovery`, `TestRemoteRescale`: real gRPC worker nodes; kill one, recover, or come back with fewer.
- `TestProducerReplaysAfterServerCrash`: server killed mid-stream and restarted on the same address; the producer reconnects and replays.
- `TestSubmitBlocksUntilCreditsFree`, `TestTrySubmitOverload`: backpressure.

CI runs everything with `-race` on Linux. An independent review pass (a fresh
reviewer with no context, told to find bugs and prove them) found six issues,
all fixed with a regression test that fails without the fix: a panic when
`Start` fails, a poisoned watermark from one extreme timestamp, a panic on
invalid window config, a permanent error that was retried forever, staged sink
output lost after a failed checkpoint followed by a crash, and missing fsyncs.

## Performance

One laptop, 8 threads, Windows. Results move a lot with machine load, so read
these as ranges.

| | |
|---|---|
| In-process pipeline, 4 workers (`make bench`) | ~4-5M events/s, ~150 B/event, batch P99 ~10-17 ms |
| Window operator `Add` | ~130-150 ns/event, 0 allocs |
| gRPC, 4 clients, 500 events/batch (`make stress`) | ~0.9M events/s, ack P99 ~4 ms |
| Cluster, master + 3 worker processes, same machine | ~0.8M events/s |

The latency histogram is log-bucketed, so quantiles are accurate to ~25%.
The in-process benchmark number was originally ~1.4M events/s; most of that was
the benchmark's own key formatting; the commit that fixed it has the details.

## Limits

- The master is a single point of failure and there is no leader election.
- Recovery is a full rollback to the last checkpoint, not a per-worker restart.
- Auth is one shared bearer token; TLS is server-side only (no client certificates).
- `-latefile` only sees events dropped by in-process workers, not remote ones.
- A crash between the sink emit and its marker re-sends that one epoch:
  at-least-once for it, and downstream can dedup on (key, window start).
- After a rescale the watermark restarts at the slowest old worker's, so a very
  late event may be accepted that an unscaled run would have dropped.
- Rescaling and cluster mode need the `Hash` partitioner.
- Session windows have no allowed lateness.
- Event times beyond ±2^60 are always dropped; `-maxjump` additionally drops events
  too far ahead of the newest one so a single bad timestamp cannot freeze a window.

## Layout

```
cmd/engine          server: standalone | master | worker
cmd/loadgen         gRPC load generator (uses ingest.Producer)
cmd/replay          feed a CSV or JSONL file through a producer
internal/engine     lanes, workers, aligned barriers, sink, credits, remote workers
internal/window     tumbling / sliding / session operator, watermark, snapshots
internal/checkpoint snapshot store and ack coordinator
internal/cluster    master-side registry of worker nodes
internal/ingest     gRPC ingest service, client, reconnecting Producer
internal/partition  Hash (key groups) and RoundRobin
internal/metrics    latency histogram
internal/sketch     quantile sketch
internal/hll        HyperLogLog
internal/output     JSONL file and webhook sinks
internal/api        HTTP API and recent-results ring
internal/alert      alert rules
internal/filter     ingest filter expressions
internal/security   token auth and TLS
internal/config     JSON config loader
internal/replay     file readers and pacing for cmd/replay
proto/              wire definitions (make proto regenerates the Go code)
```

## License

MIT. See [LICENSE](LICENSE).
