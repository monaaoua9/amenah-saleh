package output

import "github.com/monaaoua9/amenah-saleh/internal/event"

type Sink interface{ Write(event.Result) }

func Fanout(sinks ...Sink) func(event.Result) {
	return func(r event.Result) {
		for _, s := range sinks {
			s.Write(r)
		}
	}
}

type Func func(event.Result)

func (f Func) Write(r event.Result) { f(r) }
