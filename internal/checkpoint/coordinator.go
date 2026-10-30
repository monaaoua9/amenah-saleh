package checkpoint

import (
	"context"
	"fmt"
	"sync"
)

type round struct {
	acks int
	err  error
	done chan struct{}
}

// Tracks acks for in-flight checkpoints. Barrier injection is the engine's job.
type Coordinator struct {
	store  *Store
	expect int

	mu     sync.Mutex
	next   uint64
	rounds map[uint64]*round
}

func NewCoordinator(s *Store, expect int, last uint64) *Coordinator {
	return &Coordinator{store: s, expect: expect, next: last, rounds: map[uint64]*round{}}
}

func (c *Coordinator) Begin() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.next++
	c.rounds[c.next] = &round{done: make(chan struct{})}
	return c.next
}

func (c *Coordinator) Ack(id uint64, task string, data []byte, err error) {
	if err == nil {
		err = c.store.Put(id, task, data)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	r := c.rounds[id]
	if r == nil {
		return
	}
	if err != nil && r.err == nil {
		r.err = err
	}
	r.acks++
	if r.acks == c.expect {
		close(r.done)
	}
}

func (c *Coordinator) Wait(ctx context.Context, id uint64) error {
	c.mu.Lock()
	r := c.rounds[id]
	c.mu.Unlock()
	if r == nil {
		return fmt.Errorf("checkpoint %d is not in flight", id)
	}

	select {
	case <-r.done:
	case <-ctx.Done():
		// forget it, otherwise an abandoned round stays in the map for good
		c.mu.Lock()
		delete(c.rounds, id)
		c.mu.Unlock()
		return ctx.Err()
	}
	c.mu.Lock()
	delete(c.rounds, id)
	c.mu.Unlock()
	return r.err
}
