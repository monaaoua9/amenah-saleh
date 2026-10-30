package engine

import (
	"bytes"
	"context"
	"encoding/gob"
	"sync"

	"github.com/monaaoua9/streamdb/internal/event"
)

type sinkMsg struct {
	from    int
	results []event.Result
	barrier uint64
}

type staged struct {
	id  uint64
	res [][]event.Result
}

// Output is held per checkpoint epoch and only released once that checkpoint
// is durable. Anything staged after a crash is dropped; replay regenerates it.
type sink struct {
	in  chan sinkMsg
	e   *Engine
	al  *aligner[sinkMsg]
	out func(event.Result)

	// chunks exactly as the workers sent them: no copy, no regrowth
	pending  [][]event.Result
	pendingN int

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
		s.pending = append(s.pending, m.results)
		s.pendingN += len(m.results)
		if max := s.e.cfg.MaxPending; max > 0 && s.pendingN >= max {
			select {
			case s.e.ckNow <- struct{}{}:
			default: // one is already queued
			}
		}
		return
	}
	if !s.al.arrive(m.from) {
		return
	}
	s.mu.Lock()
	// an earlier checkpoint may have failed after staging; its output is still
	// owed, so this snapshot has to carry it too or a crash here loses it
	all := make([][]event.Result, 0, len(s.pending)+8)
	for _, st := range s.st {
		all = append(all, st.res...)
	}
	all = append(all, s.pending...)
	s.st = append(s.st, staged{m.barrier, s.pending})
	s.mu.Unlock()
	snap, err := encodeResults(all)
	s.pending, s.pendingN = nil, 0

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
		for _, chunk := range s.st[i].res {
			for _, r := range chunk {
				s.out(r)
			}
			s.e.emitted.Add(uint64(len(chunk)))
		}
	}
	s.st = s.st[i:]
}

func encodeResults(r [][]event.Result) ([]byte, error) {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(r)
	return buf.Bytes(), err
}

func decodeResults(b []byte) ([]event.Result, error) {
	var chunks [][]event.Result
	if err := gob.NewDecoder(bytes.NewReader(b)).Decode(&chunks); err != nil {
		return nil, err
	}
	var flat []event.Result
	for _, c := range chunks {
		flat = append(flat, c...)
	}
	return flat, nil
}
