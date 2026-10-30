package e2e

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/monaaoua9/amenah-saleh/internal/alert"
	"github.com/monaaoua9/amenah-saleh/internal/api"
	"github.com/monaaoua9/amenah-saleh/internal/engine"
	"github.com/monaaoua9/amenah-saleh/internal/event"
	"github.com/monaaoua9/amenah-saleh/internal/filter"
	"github.com/monaaoua9/amenah-saleh/internal/ingest"
	"github.com/monaaoua9/amenah-saleh/internal/output"
	"github.com/monaaoua9/amenah-saleh/internal/security"
	"github.com/monaaoua9/amenah-saleh/internal/window"
)

type holder struct{ e *engine.Engine }

func (h holder) Engine() *engine.Engine { return h.e }
func (h holder) Submit(ctx context.Context, p string, evs []event.Event) error {
	return h.e.Submit(ctx, p, evs)
}
func (h holder) TrySubmit(ctx context.Context, p string, evs []event.Event) error {
	return h.e.TrySubmit(ctx, p, evs)
}
func (h holder) Committed(p string) uint64 { return h.e.Committed(p) }

// token auth -> filter -> engine -> alert + recent ring -> http api, all wired
// the way cmd/engine does it
func TestPipelineEndToEnd(t *testing.T) {
	recent := api.NewRecent(100)
	var alerts []alert.Alert
	rule, err := alert.ParseRule("avg > 10 key=hot")
	if err != nil {
		t.Fatal(err)
	}
	ev := alert.NewEvaluator([]alert.Rule{rule}, 0, func(a alert.Alert) { alerts = append(alerts, a) })

	e := engine.New(engine.Config{
		Lanes: 1, Workers: 2, CheckpointDir: t.TempDir(),
		Window:   window.Config{Size: 1000},
		OnResult: output.Fanout(recent, ev),
	})
	if err := e.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer e.Stop()
	h := holder{e}

	f, err := filter.Parse("value >= 0")
	if err != nil {
		t.Fatal(err)
	}
	so, _ := security.ServerOptions("tok", "", "")
	g := grpc.NewServer(so...)
	srv := ingest.NewServer(h, false).WithFilter(f)
	srv.Register(g)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go g.Serve(lis)
	defer g.Stop()

	do, _ := security.DialOptions("tok", false, "")
	prod := ingest.NewProducer(lis.Addr().String(), "e2e", do...)
	defer prod.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = prod.Send(ctx, []event.Event{
		{Key: "hot-1", Value: 20, Time: 100},
		{Key: "hot-1", Value: 30, Time: 200},
		{Key: "cold-1", Value: 1, Time: 300},
		{Key: "cold-1", Value: -5, Time: 400}, // filtered out
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.AdvanceWatermark(ctx, 1<<40); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Checkpoint(ctx); err != nil {
		t.Fatal(err)
	}

	if srv.Filtered() != 1 {
		t.Fatalf("filtered=%d", srv.Filtered())
	}
	if len(alerts) != 1 || alerts[0].Result.Key != "hot-1" || alerts[0].Value != 25 {
		t.Fatalf("alerts: %+v", alerts)
	}

	api := httptest.NewServer(api.Handler(h, recent))
	defer api.Close()
	resp, err := http.Get(api.URL + "/top?by=sum&n=5")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var body struct {
		Data []struct {
			Key   string
			Value float64
		}
	}
	json.NewDecoder(resp.Body).Decode(&body)
	if len(body.Data) != 2 || body.Data[0].Key != "hot-1" || body.Data[0].Value != 50 || body.Data[1].Value != 1 {
		t.Fatalf("top: %+v", body.Data)
	}
}
