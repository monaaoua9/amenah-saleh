package replay

import (
	"strings"
	"testing"
	"time"

	"github.com/monaaoua9/amenah-saleh/internal/event"
)

func collect(t *testing.T, src, format string) ([]event.Event, error) {
	t.Helper()
	var out []event.Event
	err := Read(strings.NewReader(src), format, func(e event.Event) error {
		out = append(out, e)
		return nil
	})
	return out, err
}

func TestCSVWithHeader(t *testing.T) {
	got, err := collect(t, "key,value,time\na,1.5,100\nb,-2,200\n", "csv")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != (event.Event{Key: "a", Value: 1.5, Time: 100}) || got[1].Value != -2 {
		t.Fatalf("%+v", got)
	}
}

func TestCSVNoHeader(t *testing.T) {
	got, err := collect(t, "a,1,10\n", "csv")
	if err != nil || len(got) != 1 {
		t.Fatalf("%v %+v", err, got)
	}
}

func TestCSVBadRowAfterFirst(t *testing.T) {
	_, err := collect(t, "a,1,10\nb,oops,20\n", "csv")
	if err == nil || !strings.Contains(err.Error(), "line 2") {
		t.Fatalf("got %v", err)
	}
	if _, err := collect(t, "a,1\n", "csv"); err == nil {
		t.Fatal("short row accepted")
	}
}

func TestJSONL(t *testing.T) {
	got, err := collect(t, `{"key":"a","value":2,"time":5,"seq":99}`+"\n\n"+`{"key":"b","value":3,"time":6}`+"\n", "jsonl")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Seq != 0 || got[1].Key != "b" {
		t.Fatalf("%+v", got)
	}
	if _, err := collect(t, "{nope\n", "jsonl"); err == nil {
		t.Fatal("bad json accepted")
	}
}

func TestStopsWhenCallbackFails(t *testing.T) {
	n := 0
	err := Read(strings.NewReader("a,1,1\nb,1,2\nc,1,3\n"), "csv", func(event.Event) error {
		n++
		return errStop
	})
	if err != errStop || n != 1 {
		t.Fatalf("n=%d err=%v", n, err)
	}
}

var errStop = &stopErr{}

type stopErr struct{}

func (*stopErr) Error() string { return "stop" }

func TestPacer(t *testing.T) {
	p := NewPacer(10)
	if d := p.Delay(1000); d != 0 {
		t.Fatalf("first event waits %v", d)
	}
	if d := p.Delay(2000); d != 100*time.Millisecond {
		t.Fatalf("1s of event time at 10x = 100ms, got %v", d)
	}
	if d := p.Delay(1500); d != 0 {
		t.Fatalf("backwards step waits %v", d)
	}
	if d := NewPacer(0).Delay(5); d != 0 {
		t.Fatal("speed 0 must not sleep")
	}
}
