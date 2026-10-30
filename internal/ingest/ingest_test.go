package ingest

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/monaaoua9/amenah-saleh/internal/engine"
	"github.com/monaaoua9/amenah-saleh/internal/event"
)

type fake struct {
	got [][]event.Event
	err error
}

func (f *fake) Submit(_ context.Context, _ string, evs []event.Event) error {
	f.got = append(f.got, evs)
	return f.err
}
func (f *fake) Committed(string) uint64 { return 0 }
func (f *fake) TrySubmit(ctx context.Context, p string, evs []event.Event) error {
	return f.Submit(ctx, p, evs)
}

func serve(t *testing.T, sub Submitter, shed bool) *Client {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	g := grpc.NewServer()
	NewServer(sub, shed).Register(g)
	go g.Serve(lis)
	t.Cleanup(g.Stop)

	c, err := Dial(context.Background(), lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func TestRoundTrip(t *testing.T) {
	f := &fake{}
	c := serve(t, f, false)

	evs := []event.Event{{Key: "a", Value: 1.5, Time: 10, Seq: 1}, {Key: "b", Value: 2, Time: 20, Seq: 2}}
	if err := c.Send("p1", evs); err != nil {
		t.Fatal(err)
	}
	ack, err := c.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if ack.Producer != "p1" || ack.LastSeq != 2 {
		t.Fatalf("ack %+v", ack)
	}
	if len(f.got) != 1 || f.got[0][0] != evs[0] || f.got[0][1] != evs[1] {
		t.Fatalf("server saw %+v", f.got)
	}
}

func TestOverloadMapsToResourceExhausted(t *testing.T) {
	c := serve(t, &fake{err: engine.ErrOverloaded}, true)
	if err := c.Send("p", []event.Event{{Key: "a", Seq: 1}}); err != nil {
		t.Fatal(err)
	}
	_, err := c.Recv()
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("got %v", err)
	}
}

func TestFilterDropsButAcksEverything(t *testing.T) {
	f := &fake{}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	g := grpc.NewServer()
	srv := NewServer(f, false).WithFilter(func(e event.Event) bool { return e.Value > 1 })
	srv.Register(g)
	go g.Serve(lis)
	t.Cleanup(g.Stop)
	c, err := Dial(context.Background(), lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	evs := []event.Event{{Key: "a", Value: 1, Seq: 1}, {Key: "b", Value: 5, Seq: 2}, {Key: "c", Value: 0, Seq: 3}}
	if err := c.Send("p", evs); err != nil {
		t.Fatal(err)
	}
	ack, err := c.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if ack.LastSeq != 3 {
		t.Fatalf("ack covers %d, want 3", ack.LastSeq)
	}
	if len(f.got) != 1 || len(f.got[0]) != 1 || f.got[0][0].Key != "b" {
		t.Fatalf("engine saw %+v", f.got)
	}
	if srv.Filtered() != 2 {
		t.Fatalf("filtered=%d", srv.Filtered())
	}
}
