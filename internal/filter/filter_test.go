package filter

import (
	"testing"

	"github.com/monaaoua9/streamdb/internal/event"
)

func TestParseAndEval(t *testing.T) {
	cases := []struct {
		src  string
		e    event.Event
		want bool
	}{
		{`value > 10`, event.Event{Value: 11}, true},
		{`value > 10`, event.Event{Value: 10}, false},
		{`value >= 10 && key ~ "sensor-"`, event.Event{Key: "sensor-7", Value: 10}, true},
		{`value >= 10 && key ~ "sensor-"`, event.Event{Key: "other", Value: 10}, false},
		{`key == 'a' || key == 'b'`, event.Event{Key: "b"}, true},
		{`!(key == "a")`, event.Event{Key: "a"}, false},
		{`!(key == "a")`, event.Event{Key: "c"}, true},
		{`value < -2.5`, event.Event{Value: -3}, true},
		{`time > 1000 && time <= 2000`, event.Event{Time: 2000}, true},
		{`key !~ "test"`, event.Event{Key: "prod-1"}, true},
		{`key !~ "test"`, event.Event{Key: "test-1"}, false},
		// && binds tighter than ||
		{`key == "x" || key == "y" && value > 5`, event.Event{Key: "x", Value: 1}, true},
		{`key == "x" || key == "y" && value > 5`, event.Event{Key: "y", Value: 1}, false},
		{`value == 1e3`, event.Event{Value: 1000}, true},
	}
	for _, c := range cases {
		f, err := Parse(c.src)
		if err != nil {
			t.Errorf("%s: %v", c.src, err)
			continue
		}
		if got := f(c.e); got != c.want {
			t.Errorf("%s on %+v = %v, want %v", c.src, c.e, got, c.want)
		}
	}
}

func TestParseErrors(t *testing.T) {
	for _, src := range []string{
		``, `value >`, `value > "x"`, `key > "x"`, `key == 5`, `foo == 1`,
		`(value > 1`, `value > 1 )`, `value > 1 value`, `key == "abc`, `value @ 1`, `value ~ 1`,
	} {
		if _, err := Parse(src); err == nil {
			t.Errorf("%q parsed, want an error", src)
		}
	}
}

func FuzzParse(f *testing.F) {
	for _, s := range []string{`value > 10`, `key ~ "a" && (value < 1 || !(time == 3))`, `key == 'x'`, ``, `((((`, `value > 1e999`, `"`, `!!!!value>1`} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, src string) {
		fn, err := Parse(src)
		if err != nil {
			return
		}
		// anything that parses has to be safe to run
		fn(event.Event{Key: "k", Value: 1, Time: 2})
		fn(event.Event{})
	})
}
