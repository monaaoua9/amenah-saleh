package replay

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/monaaoua9/amenah-saleh/internal/event"
)

// CSV rows are key,value,time(ms). A first row that doesn't parse as numbers is
// taken for a header. JSONL lines are {"key":..,"value":..,"time":..}.
// Seq is left alone; the Producer numbers events.
func Read(r io.Reader, format string, fn func(event.Event) error) error {
	switch format {
	case "csv":
		return readCSV(r, fn)
	case "jsonl":
		return readJSONL(r, fn)
	}
	return fmt.Errorf("replay: unknown format %q", format)
}

func readCSV(r io.Reader, fn func(event.Event) error) error {
	cr := csv.NewReader(r)
	cr.FieldsPerRecord = 3
	for line := 1; ; line++ {
		rec, err := cr.Read()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		v, verr := strconv.ParseFloat(rec[1], 64)
		ts, terr := strconv.ParseInt(rec[2], 10, 64)
		if verr != nil || terr != nil {
			if line == 1 {
				continue
			}
			return fmt.Errorf("replay: line %d: bad value or time", line)
		}
		if err := fn(event.Event{Key: rec[0], Value: v, Time: ts}); err != nil {
			return err
		}
	}
}

func readJSONL(r io.Reader, fn func(event.Event) error) error {
	sc := bufio.NewScanner(r)
	sc.Buffer(nil, 1<<20)
	for line := 1; sc.Scan(); line++ {
		if len(sc.Bytes()) == 0 {
			continue
		}
		var e event.Event
		if err := json.Unmarshal(sc.Bytes(), &e); err != nil {
			return fmt.Errorf("replay: line %d: %w", line, err)
		}
		e.Seq = 0
		if err := fn(e); err != nil {
			return err
		}
	}
	return sc.Err()
}

// Pacer maps gaps in event time to wall-clock sleeps, divided by speed.
// speed <= 0 never sleeps. Event time going backwards costs nothing.
type Pacer struct {
	speed float64
	prev  int64
	init  bool
}

func NewPacer(speed float64) *Pacer { return &Pacer{speed: speed} }

func (p *Pacer) Delay(t int64) time.Duration {
	if p.speed <= 0 {
		return 0
	}
	if !p.init {
		p.prev, p.init = t, true
		return 0
	}
	d := t - p.prev
	if d <= 0 {
		return 0
	}
	p.prev = t
	return time.Duration(float64(d) / p.speed * float64(time.Millisecond))
}
