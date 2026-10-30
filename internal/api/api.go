package api

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/monaaoua9/amenah-saleh/internal/engine"
	"github.com/monaaoua9/amenah-saleh/internal/event"
)

// Engine is looked up per request because the master swaps it out after a worker loss.
type Source interface {
	Engine() *engine.Engine
}

type resultView struct {
	event.Result
	Avg    float64 `json:"avg"`
	StdDev float64 `json:"stddev"`
}

type statsView struct {
	Ingested    uint64  `json:"ingested"`
	Duplicates  uint64  `json:"duplicates"`
	Late        uint64  `json:"late"`
	Emitted     uint64  `json:"emitted"`
	Checkpoints uint64  `json:"checkpoints"`
	Inflight    int64   `json:"inflight"`
	LastCkSec   float64 `json:"last_checkpoint_seconds"`
	P50Ms       float64 `json:"p50_ms"`
	P99Ms       float64 `json:"p99_ms"`
}

func Handler(src Source, recent *Recent) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		io.WriteString(w, "ok\n")
	})

	// not ready while the master is waiting for nodes or restoring
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if src.Engine() == nil {
			http.Error(w, "engine not running", http.StatusServiceUnavailable)
			return
		}
		io.WriteString(w, "ready\n")
	})

	mux.HandleFunc("/stats", func(w http.ResponseWriter, _ *http.Request) {
		e := src.Engine()
		if e == nil {
			http.Error(w, "engine not running", http.StatusServiceUnavailable)
			return
		}
		s := e.Stats()
		writeJSON(w, statsView{
			Ingested: s.Ingested, Duplicates: s.Dups, Late: s.Late, Emitted: s.Emitted,
			Checkpoints: s.Checkpoints, Inflight: s.Inflight, LastCkSec: s.LastCheckpoint.Seconds(),
			P50Ms: ms(s.P50), P99Ms: ms(s.P99),
		})
	})

	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		if e := src.Engine(); e != nil {
			e.WriteMetrics(w)
		}
	})

	mux.HandleFunc("/results", func(w http.ResponseWriter, r *http.Request) {
		limit := 100
		if v := r.URL.Query().Get("limit"); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil || n <= 0 {
				http.Error(w, "bad limit", http.StatusBadRequest)
				return
			}
			limit = min(n, 1000)
		}
		rs := recent.Query(r.URL.Query().Get("key"), limit)
		out := make([]resultView, len(rs))
		for i, x := range rs {
			out[i] = resultView{x, x.Avg(), x.StdDev()}
		}
		writeJSON(w, map[string]any{"data": out, "meta": map[string]any{"count": len(out)}})
	})

	return mux
}

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}
