package engine

import (
	"context"
	"errors"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	cpb "github.com/monaaoua9/amenah-saleh/internal/cluster/pb"
	"github.com/monaaoua9/amenah-saleh/internal/event"
	"github.com/monaaoua9/amenah-saleh/internal/partition"
)

// RunWorkerNode connects to a master and runs one worker until the stream
// ends. The master decides which worker we are and what state we start with,
// so a restarted node needs nothing locally. Returns nil when ctx is cancelled.
func RunWorkerNode(ctx context.Context, master string) error {
	cc, err := grpc.NewClient(master,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(MaxMsgSize), grpc.MaxCallSendMsgSize(MaxMsgSize)),
	)
	if err != nil {
		return err
	}
	defer cc.Close()

	st, err := cpb.NewClusterClient(cc).Run(ctx)
	if err != nil {
		return err
	}
	first, err := st.Recv()
	if err != nil {
		return err
	}
	init := first.GetInit()
	if init == nil {
		return errors.New("first message was not Init")
	}

	env := &streamEnv{st: st}
	w := newWorker(int(init.WorkerId), int(init.Lanes), windowFromWire(init.Window), env)
	env.op = w.op

	if len(init.Snapshots) > 0 {
		var err error
		if init.SameCount {
			err = w.op.Restore(init.Snapshots[0])
		} else {
			id, total := int(init.WorkerId), int(init.Workers)
			err = w.op.RestoreMerge(init.Snapshots, func(k string) bool {
				return partition.Hash{}.Owner(k, total) == id
			})
		}
		if err != nil {
			return fmt.Errorf("restore: %w", err)
		}
	}

	for {
		msg, err := st.Recv()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}

		switch x := msg.Msg.(type) {
		case *cpb.ToWorker_Data:
			b := getBatch()
			for _, e := range x.Data.Events {
				*b = append(*b, event.Event{Key: e.Key, Value: e.Value, Time: e.Time, Seq: e.Seq})
			}
			w.handle(ctx, workerMsg{from: int(x.Data.From), batch: b, at: time.Unix(0, x.Data.AtNs)})
		case *cpb.ToWorker_Barrier:
			w.handle(ctx, workerMsg{from: int(x.Barrier.From), barrier: x.Barrier.Id})
		case *cpb.ToWorker_Punct:
			w.handle(ctx, workerMsg{from: int(x.Punct.From), punct: true, wm: x.Punct.Wm})
		}
		if env.err != nil {
			return env.err
		}
	}
}

// All sends come from the single goroutine running the receive loop.
type streamEnv struct {
	st  grpc.BidiStreamingClient[cpb.FromWorker, cpb.ToWorker]
	op  interface{ Late() uint64 }
	err error
}

func (s *streamEnv) send(m *cpb.FromWorker) {
	if s.err == nil {
		s.err = s.st.Send(m)
	}
}

func (s *streamEnv) sink(_ context.Context, m sinkMsg) {
	if m.barrier != 0 {
		s.send(&cpb.FromWorker{Msg: &cpb.FromWorker_SinkBarrier{SinkBarrier: &cpb.SinkBarrier{Id: m.barrier}}})
		return
	}
	out := make([]*cpb.Result, len(m.results))
	for i, r := range m.results {
		out[i] = &cpb.Result{Key: r.Key, Start: r.Start, End: r.End, Count: r.Count, Sum: r.Sum, Min: r.Min, Max: r.Max, Late: r.Late}
	}
	s.send(&cpb.FromWorker{Msg: &cpb.FromWorker_Results{Results: &cpb.Results{Results: out}}})
}

func (s *streamEnv) ack(id uint64, _ string, data []byte, err error) {
	sn := &cpb.Snap{Id: id, Data: data}
	if err != nil {
		sn.Err = err.Error()
	}
	s.send(&cpb.FromWorker{Msg: &cpb.FromWorker_Snap{Snap: sn}})
}

func (s *streamEnv) processed(n int, at time.Time) {
	s.send(&cpb.FromWorker{Msg: &cpb.FromWorker_Done{Done: &cpb.Done{
		N: int64(n), AtNs: at.UnixNano(), Late: s.op.Late(),
	}}})
}
