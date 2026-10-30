package metrics

import (
	"fmt"
	"io"
)

// one metric in Prometheus text format; typ is "counter" or "gauge"
func Write(w io.Writer, name, help, typ string, v float64) {
	fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n%s %g\n", name, help, name, typ, name, v)
}
