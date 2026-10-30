package checkpoint

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRoundCompletesAfterAllAcks(t *testing.T) {
	s := NewStore(t.TempDir())
	c := NewCoordinator(s, 3, 7)
	id := c.Begin()
	if id != 8 {
		t.Fatalf("first id after 7 should be 8, got %d", id)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- c.Wait(ctx, id) }()

	c.Ack(id, "a", []byte("1"), nil)
	c.Ack(id, "b", []byte("2"), nil)
	select {
	case <-done:
		t.Fatal("finished with 2 of 3 acks")
	case <-time.After(50 * time.Millisecond):
	}
	c.Ack(id, "c", []byte("3"), nil)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	for task, want := range map[string]string{"a": "1", "b": "2", "c": "3"} {
		if got, err := s.Get(id, task); err != nil || string(got) != want {
			t.Fatalf("%s: %q %v", task, got, err)
		}
	}
}

func TestFirstErrorWinsAndStillFinishes(t *testing.T) {
	c := NewCoordinator(NewStore(t.TempDir()), 2, 0)
	id := c.Begin()
	first, second := errors.New("first"), errors.New("second")
	c.Ack(id, "a", nil, first)
	c.Ack(id, "b", nil, second)
	if err := c.Wait(context.Background(), id); err != first {
		t.Fatalf("got %v", err)
	}
}

func TestWaitCancelledDropsTheRound(t *testing.T) {
	c := NewCoordinator(NewStore(t.TempDir()), 2, 0)
	id := c.Begin()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := c.Wait(ctx, id); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
	c.mu.Lock()
	n := len(c.rounds)
	c.mu.Unlock()
	if n != 0 {
		t.Fatalf("%d rounds leaked", n)
	}
	c.Ack(id, "late", []byte("x"), nil) // ack for a forgotten round must not panic
}

func TestWaitUnknownRound(t *testing.T) {
	c := NewCoordinator(NewStore(t.TempDir()), 1, 0)
	if err := c.Wait(context.Background(), 99); err == nil {
		t.Fatal("want an error, not a nil dereference")
	}
}

func TestEmittedMarkerAndCommitFailure(t *testing.T) {
	s := NewStore(t.TempDir())
	s.Put(1, "t", []byte("x"))
	if s.Emitted(1) {
		t.Fatal("emitted before the marker")
	}
	if err := s.MarkEmitted(1); err != nil || !s.Emitted(1) {
		t.Fatalf("marker: %v", err)
	}
	s.FailCommitForTest(true)
	if err := s.Commit(1); err == nil {
		t.Fatal("injected failure not reported")
	}
	if _, ok := s.Latest(); ok {
		t.Fatal("a checkpoint whose commit failed must not count")
	}
	s.FailCommitForTest(false)
	if err := s.Commit(1); err != nil {
		t.Fatal(err)
	}
	if id, ok := s.Latest(); !ok || id != 1 {
		t.Fatalf("latest %d %v", id, ok)
	}
	if s.Size(1) == 0 {
		t.Fatal("size should count the files")
	}
}
