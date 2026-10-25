package ingest

import (
	"context"
	"errors"
	"io"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/monaaoua9/amenah-saleh/internal/engine"
	"github.com/monaaoua9/amenah-saleh/internal/event"
	"github.com/monaaoua9/amenah-saleh/internal/ingest/pb"
)

type Submitter interface {
	Submit(ctx context.Context, producer string, evs []event.Event) error
	TrySubmit(ctx context.Context, producer string, evs []event.Event) error
	Committed(producer string) uint64
}

type Server struct {
	pb.UnimplementedIngestServer
	sub Submitter
	// shed = reject with RESOURCE_EXHAUSTED when full instead of stalling the stream
	shed bool
}

func NewServer(sub Submitter, shed bool) *Server { return &Server{sub: sub, shed: shed} }

func (s *Server) Register(g *grpc.Server) { pb.RegisterIngestServer(g, s) }

func (s *Server) Stream(st grpc.BidiStreamingServer[pb.Batch, pb.Ack]) error {
	ctx := st.Context()
	for {
		b, err := st.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		evs := make([]event.Event, len(b.Events))
		for i, e := range b.Events {
			evs[i] = event.Event{Key: e.Key, Value: e.Value, Time: e.Time, Seq: e.Seq}
		}

		if s.shed {
			err = s.sub.TrySubmit(ctx, b.Producer, evs)
		} else {
			// blocking here stops us reading from the stream, http/2 flow control
			// then pushes the stall back to the client
			err = s.sub.Submit(ctx, b.Producer, evs)
		}
		switch {
		case errors.Is(err, engine.ErrOverloaded):
			return status.Error(codes.ResourceExhausted, "overloaded")
		case err != nil:
			return status.Error(codes.Unavailable, err.Error())
		}

		var last uint64
		if n := len(evs); n > 0 {
			last = evs[n-1].Seq
		}
		if err := st.Send(&pb.Ack{Producer: b.Producer, LastSeq: last, CommittedSeq: s.sub.Committed(b.Producer)}); err != nil {
			return err
		}
	}
}

type Client struct {
	conn *grpc.ClientConn
	st   grpc.BidiStreamingClient[pb.Batch, pb.Ack]
}

func Dial(ctx context.Context, addr string, opts ...grpc.DialOption) (*Client, error) {
	conn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		return nil, err
	}
	st, err := pb.NewIngestClient(conn).Stream(ctx)
	if err != nil {
		conn.Close()
		return nil, err
	}
	return &Client{conn, st}, nil
}

func (c *Client) Send(producer string, evs []event.Event) error {
	out := make([]*pb.Event, len(evs))
	for i := range evs {
		e := &evs[i]
		out[i] = &pb.Event{Key: e.Key, Value: e.Value, Time: e.Time, Seq: e.Seq}
	}
	return c.st.Send(&pb.Batch{Producer: producer, Events: out})
}

func (c *Client) Recv() (*pb.Ack, error) { return c.st.Recv() }

func (c *Client) Close() error {
	c.st.CloseSend()
	return c.conn.Close()
}
