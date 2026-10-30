package ingest

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/monaaoua9/streamdb/internal/engine"
	"github.com/monaaoua9/streamdb/internal/event"
	"github.com/monaaoua9/streamdb/internal/ingest/pb"
	"github.com/monaaoua9/streamdb/internal/metrics"
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
	shed      bool
	lim       *limiter
	filt      func(event.Event) bool
	drop      atomic.Uint64
	throttled atomic.Uint64 // batches the rate limiter delayed or rejected
}

func NewServer(sub Submitter, shed bool) *Server { return &Server{sub: sub, shed: shed} }

// WithRate caps each producer at rate events/sec (burst tokens of slack). When
// the server sheds it rejects over-limit batches, otherwise it slows the stream.
func (s *Server) WithRate(rate float64, burst int) *Server {
	s.lim = &limiter{rate: rate, burst: burst}
	return s
}

// WithFilter drops events f rejects before they reach the engine. They still
// count as received for the ack, otherwise a producer would resend them forever.
func (s *Server) WithFilter(f func(event.Event) bool) *Server {
	s.filt = f
	return s
}

func (s *Server) Filtered() uint64 { return s.drop.Load() }

func (s *Server) Throttled() uint64 { return s.throttled.Load() }

// Prometheus text, appended to the engine's on /metrics.
func (s *Server) WriteMetrics(w io.Writer) {
	metrics.Write(w, "dspe_ingest_filtered_total", "Events dropped by the ingest filter.", "counter", float64(s.Filtered()))
	metrics.Write(w, "dspe_ingest_throttled_total", "Batches delayed or rejected by the rate limiter.", "counter", float64(s.Throttled()))
}

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

		var last uint64
		if n := len(evs); n > 0 {
			last = evs[n-1].Seq
		}
		if s.filt != nil {
			kept := evs[:0]
			for _, e := range evs {
				if s.filt(e) {
					kept = append(kept, e)
				}
			}
			s.drop.Add(uint64(len(evs) - len(kept)))
			evs = kept
		}

		if s.lim != nil {
			bk := s.lim.bucket(b.Producer)
			if wait := bk.take(len(evs)); wait > 0 {
				s.throttled.Add(1)
				if s.shed {
					bk.refund(len(evs))
					return status.Error(codes.ResourceExhausted, "rate limit")
				}
				select {
				case <-time.After(wait):
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}

		if s.shed {
			err = s.sub.TrySubmit(ctx, b.Producer, evs)
		} else {
			// blocking here stops us reading from the stream, http/2 flow control
			// then pushes the stall back to the client
			err = s.sub.Submit(ctx, b.Producer, evs)
		}
		switch {
		case errors.Is(err, engine.ErrBatchTooLarge):
			return status.Error(codes.InvalidArgument, err.Error())
		case errors.Is(err, engine.ErrOverloaded):
			return status.Error(codes.ResourceExhausted, "overloaded")
		case err != nil:
			return status.Error(codes.Unavailable, err.Error())
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
