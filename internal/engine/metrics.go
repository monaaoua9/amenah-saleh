package engine

import (
	"fmt"
	"io"
)

// Prometheus text format, hand-rolled to avoid pulling in the client library.
func (e *Engine) WriteMetrics(w io.Writer) {
	s := e.Stats()
	counter := func(name, help string, v uint64) {
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s counter\n%s %d\n", name, help, name, name, v)
	}
	gauge := func(name, help string, v float64) {
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s gauge\n%s %g\n", name, help, name, name, v)
	}

	counter("dspe_events_ingested_total", "Events accepted by Submit.", s.Ingested)
	counter("dspe_events_duplicate_total", "Events dropped by sequence dedup.", s.Dups)
	counter("dspe_events_late_total", "Events dropped as too late for any window.", s.Late)
	counter("dspe_results_emitted_total", "Window results released after a durable checkpoint.", s.Emitted)
	counter("dspe_checkpoints_total", "Completed checkpoints.", s.Checkpoints)
	gauge("dspe_inflight_events", "Events admitted but not yet fully processed.", float64(s.Inflight))
	gauge("dspe_checkpoint_last_seconds", "Duration of the last checkpoint.", s.LastCheckpoint.Seconds())

	fmt.Fprintf(w, "# HELP dspe_batch_latency_seconds Submit to worker-done latency per batch.\n# TYPE dspe_batch_latency_seconds summary\n")
	fmt.Fprintf(w, "dspe_batch_latency_seconds{quantile=\"0.5\"} %g\n", s.P50.Seconds())
	fmt.Fprintf(w, "dspe_batch_latency_seconds{quantile=\"0.99\"} %g\n", s.P99.Seconds())
}
