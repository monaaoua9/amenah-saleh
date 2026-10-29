package engine

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	cpb "github.com/monaaoua9/streamdb/internal/cluster/pb"
	"github.com/monaaoua9/streamdb/internal/event"
	ipb "github.com/monaaoua9/streamdb/internal/ingest/pb"
)

// snapshots of a big window state go over the same stream
const MaxMsgSize = 256 << 20

// Master-side end of a worker node's stream. Send and Recv are used from one
// goroutine each; Close must unblock a pending Recv.
type WorkerConn interface {
	Send(*cpb.ToWorker) error
	Recv() (*cpb.FromWorker, error)
	Close()
}

// Stands in for a worker inside the master: lanes still push workerMsg into
// `in`, this forwards them over the wire and feeds the replies back into the
// sink, the coordinator and the credit pool.
type remoteWorker struct {
	id   int
	conn WorkerConn
	e    *Engine
	in   <-chan workerMsg
	late atomic.Uint64
}

func (r *remoteWorker) hello() error {
	init := &cpb.Init{
		WorkerId: int32(r.id),
		Workers:  int32(r.e.cfg.Workers),
		Lanes:    int32(r.e.cfg.Lanes),
		Window:   windowToWire(r.e.cfg.Window),
	}
	if r.e.restored {
		if r.e.rsame {
			init.Snapshots, init.SameCount = [][]byte{r.e.rblobs[r.id]}, true
		} else {
			init.Snapshots = r.e.rblobs
		}
	}
	return r.conn.Send(&cpb.ToWorker{Msg: &cpb.ToWorker_Init{Init: init}})
}

func (r *remoteWorker) sendLoop(ctx context.Context) {
	for {
		var m workerMsg
		select {
		case <-ctx.Done():
			return
		case m = <-r.in:
		}

		var msg *cpb.ToWorker
		switch {
		case m.barrier != 0:
			msg = &cpb.ToWorker{Msg: &cpb.ToWorker_Barrier{Barrier: &cpb.Barrier{From: int32(m.from), Id: m.barrier}}}
		case m.punct:
			msg = &cpb.ToWorker{Msg: &cpb.ToWorker_Punct{Punct: &cpb.Punct{From: int32(m.from), Wm: m.wm}}}
		default:
			b := *m.batch
			evs := make([]*ipb.Event, len(b))
			for i := range b {
				evs[i] = &ipb.Event{Key: b[i].Key, Value: b[i].Value, Time: b[i].Time, Seq: b[i].Seq}
			}
			batchPool.Put(m.batch)
			msg = &cpb.ToWorker{Msg: &cpb.ToWorker_Data{Data: &cpb.Data{
				From: int32(m.from), Events: evs, AtNs: m.at.UnixNano(),
			}}}
		}

		if err := r.conn.Send(msg); err != nil {
			if ctx.Err() == nil {
				r.e.fail(fmt.Errorf("send to worker %d: %w", r.id, err))
			}
			return
		}
	}
}

func (r *remoteWorker) recvLoop(ctx context.Context) {
	sink := func(m sinkMsg) bool {
		select {
		case r.e.sink.in <- m:
			return true
		case <-ctx.Done():
			return false
		}
	}

	for {
		msg, err := r.conn.Recv()
		if err != nil {
			if ctx.Err() == nil {
				r.e.fail(fmt.Errorf("worker %d lost: %w", r.id, err))
			}
			return
		}

		switch x := msg.Msg.(type) {
		case *cpb.FromWorker_Results:
			out := make([]event.Result, len(x.Results.Results))
			for i, p := range x.Results.Results {
				out[i] = event.Result{Key: p.Key, Start: p.Start, End: p.End, Count: p.Count, Sum: p.Sum, Min: p.Min, Max: p.Max, Late: p.Late}
			}
			if !sink(sinkMsg{from: r.id, results: out}) {
				return
			}
		case *cpb.FromWorker_SinkBarrier:
			if !sink(sinkMsg{from: r.id, barrier: x.SinkBarrier.Id}) {
				return
			}
		case *cpb.FromWorker_Snap:
			var serr error
			if x.Snap.Err != "" {
				serr = errors.New(x.Snap.Err)
			}
			r.e.coord.Ack(x.Snap.Id, workerTask(r.id), x.Snap.Data, serr)
		case *cpb.FromWorker_Done:
			if x.Done.N < 0 || x.Done.N > r.e.cfg.MaxInflight {
				r.e.fail(fmt.Errorf("worker %d reported bad Done.N=%d", r.id, x.Done.N))
				return
			}
			r.e.release(x.Done.N)
			r.e.lat.Observe(time.Since(time.Unix(0, x.Done.AtNs)))
			r.late.Store(x.Done.Late)
		}
	}
}
