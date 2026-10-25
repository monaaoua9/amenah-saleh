package engine

import (
	"bytes"
	"context"
	"encoding/gob"
	"sync"

	"github.com/monaaoua9/amenah-saleh/internal/event"
)

type sinkMsg struct {
	from    int
	results []event.Result
	barrier uint64
}

type staged struct {
	id  uint64
	res []event.Result
}

// Output is held per checkpoint epoch and only released once that checkpoint
// is durable. Anything staged after a crash is dropped; replay regenerates it.
type sink struct {
	in  chan sinkMsg
	e   *Engine
	al  *aligner[sinkMsg]
	out func(event.Result)

	pending []event.Result

	mu sync.Mutex
	st []staged
}

func (s *sink) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case m := <-s.in:
			s.handle(m)
		}
	}
}

func (s *sink) handle(m sinkMsg) {
	if s.al.blocked(m.from) {
		s.al.hold(m)
		return
	}
	if m.barrier == 0 {
		s.pending = append(s.pending, m.results...)
		return
	}
	if !s.al.arrive(m.from) {
		return
	}
	s.mu.Lock()
	s.st = append(s.st, staged{m.barrier, s.pending})
	s.mu.Unlock()
	snap, err := encodeResults(s.pending)
	s.pending = nil

	s.e.coord.Ack(m.barrier, "sink", snap, err)
	for _, h := range s.al.release() {
		s.handle(h)
	}
}

func (s *sink) commit(id uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	i := 0
	for ; i < len(s.st) && s.st[i].id <= id; i++ {
		for _, r := range s.st[i].res {
			s.out(r)
		}
	}
	s.st = s.st[i:]
}

func encodeResults(r []event.Result) ([]byte, error) {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(r)
	return buf.Bytes(), err
}

func decodeResults(b []byte) (r []event.Result, err error) {
	err = gob.NewDecoder(bytes.NewReader(b)).Decode(&r)
	return
}
