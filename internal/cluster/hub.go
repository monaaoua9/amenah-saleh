package cluster

import (
	"context"
	"sync"

	"github.com/monaaoua9/streamdb/internal/cluster/pb"
	"github.com/monaaoua9/streamdb/internal/engine"
)

// Hub is the master's registry of worker nodes. A node registers by opening
// the Run stream; Wait hands the next n live ones to the engine.
type Hub struct {
	pb.UnimplementedClusterServer
	ready chan *conn
}

func NewHub() *Hub { return &Hub{ready: make(chan *conn, 1024)} }

type conn struct {
	st   pb.Cluster_RunServer
	done chan struct{}
	once sync.Once
}

func (c *conn) Send(m *pb.ToWorker) error     { return c.st.Send(m) }
func (c *conn) Recv() (*pb.FromWorker, error) { return c.st.Recv() }
func (c *conn) Close()                        { c.once.Do(func() { close(c.done) }) }

func (h *Hub) Run(st pb.Cluster_RunServer) error {
	c := &conn{st: st, done: make(chan struct{})}
	select {
	case h.ready <- c:
	case <-st.Context().Done():
		return nil
	}
	// the stream lives as long as this handler does
	select {
	case <-c.done:
	case <-st.Context().Done():
	}
	return nil
}

// Wait blocks until n live worker nodes are connected. Entries whose stream
// already died while queued are skipped.
func (h *Hub) Wait(ctx context.Context, n int) ([]engine.WorkerConn, error) {
	var out []engine.WorkerConn
	for len(out) < n {
		select {
		case c := <-h.ready:
			if c.st.Context().Err() == nil {
				out = append(out, c)
			}
		case <-ctx.Done():
			for _, c := range out {
				c.Close()
			}
			return nil, ctx.Err()
		}
	}
	return out, nil
}
