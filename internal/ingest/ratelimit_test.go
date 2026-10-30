package ingest

import (
	"bytes"
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/monaaoua9/streamdb/internal/event"
)

func TestBucketMath(t *testing.T) {
	now := time.Unix(1000, 0)
	b := newBucket(100, 50)
	b.now = func() time.Time { return now }
	b.last = now

	if w := b.take(50); w != 0 {
		t.Fatalf("burst should pass, wait %v", w)
	}
	if w := b.take(100); w != time.Second {
		t.Fatalf("100 tokens of debt at 100/s = 1s, got %v", w)
	}
	now = now.Add(2 * time.Second)
	if w := b.take(10); w != 0 {
		t.Fatalf("refilled after 2s, wait %v", w)
	}
	b.refund(1000)
	if b.tokens != 50 {
		t.Fatalf("refund must cap at burst, got %v", b.tokens)
	}
}

func rateServer(t *testing.T, shed bool, rate float64, burst int) *Client {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	g := grpc.NewServer()
	NewServer(&fake{}, shed).WithRate(rate, burst).Register(g)
	go g.Serve(lis)
	t.Cleanup(g.Stop)
	c, err := Dial(context.Background(), lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func roundTrip(c *Client, n int) error {
	evs := make([]event.Event, n)
	for i := range evs {
		evs[i] = event.Event{Key: "k", Seq: uint64(i + 1)}
	}
	if err := c.Send("p", evs); err != nil {
		return err
	}
	_, err := c.Recv()
	return err
}

func TestRateLimitSlowsStream(t *testing.T) {
	c := rateServer(t, false, 1000, 100)
	t0 := time.Now()
	for i := 0; i < 4; i++ {
		if err := roundTrip(c, 100); err != nil {
			t.Fatal(err)
		}
	}
	// 400 events, 100 free, the other 300 at 1000/s
	if el := time.Since(t0); el < 250*time.Millisecond {
		t.Fatalf("finished in %v, limiter did nothing", el)
	}
}

func TestRateLimitShedRejects(t *testing.T) {
	c := rateServer(t, true, 10, 100)
	if err := roundTrip(c, 100); err != nil {
		t.Fatal(err)
	}
	err := roundTrip(c, 100)
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("got %v", err)
	}
}

func TestThrottleAndFilterMetrics(t *testing.T) {
	f := &fake{}
	lis, _ := net.Listen("tcp", "127.0.0.1:0")
	g := grpc.NewServer()
	srv := NewServer(f, true).WithRate(10, 5).WithFilter(func(e event.Event) bool { return e.Value >= 0 })
	srv.Register(g)
	go g.Serve(lis)
	t.Cleanup(g.Stop)
	c, err := Dial(context.Background(), lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	c.Send("p", []event.Event{{Key: "a", Value: -1, Seq: 1}, {Key: "b", Value: 1, Seq: 2}})
	if _, err := c.Recv(); err != nil {
		t.Fatal(err)
	}
	c.Send("p", []event.Event{{Key: "c", Value: 1, Seq: 3}, {Key: "d", Value: 1, Seq: 4}, {Key: "e", Value: 1, Seq: 5}, {Key: "f", Value: 1, Seq: 6}, {Key: "g", Value: 1, Seq: 7}})
	if _, err := c.Recv(); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("got %v", err)
	}

	var buf bytes.Buffer
	srv.WriteMetrics(&buf)
	out := buf.String()
	for _, want := range []string{"dspe_ingest_filtered_total 1", "dspe_ingest_throttled_total 1"} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
}
