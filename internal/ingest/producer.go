package ingest

import (
	"context"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/monaaoua9/amenah-saleh/internal/event"
)

type pending struct {
	evs  []event.Event
	last uint64
}

// Producer is the client half of the exactly-once contract. It numbers events,
// keeps every batch until the server says a checkpoint covers it, and when the
// stream breaks it reconnects and replays what is still uncovered. The engine's
// seq dedup drops whatever it had already seen.
//
// Not safe for concurrent use; one Producer is one ordered stream.
type Producer struct {
	addr, name string
	opts       []grpc.DialOption

	ctx    context.Context
	cancel context.CancelFunc

	seq  uint64
	buf  []pending
	sent int // buf[:sent] already went out on the current connection
	c    *Client
}

func NewProducer(addr, name string, opts ...grpc.DialOption) *Producer {
	ctx, cancel := context.WithCancel(context.Background())
	return &Producer{addr: addr, name: name, opts: opts, ctx: ctx, cancel: cancel}
}

// Send assigns sequence numbers to evs (overwriting Seq) and returns once the
// server has accepted the batch. Connection trouble is retried until ctx ends.
func (p *Producer) Send(ctx context.Context, evs []event.Event) error {
	for i := range evs {
		p.seq++
		evs[i].Seq = p.seq
	}
	p.buf = append(p.buf, pending{evs, p.seq})

	backoff := 50 * time.Millisecond
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if p.c == nil {
			c, err := Dial(p.ctx, p.addr, p.opts...)
			if err != nil {
				if !sleep(ctx, &backoff) {
					return ctx.Err()
				}
				continue
			}
			p.c, p.sent = c, 0
		}

		err := p.pump()
		if err == nil {
			return nil
		}
		if status.Code(err) == codes.InvalidArgument {
			// the server will never take this batch: forget it instead of replaying forever
			p.buf = p.buf[:len(p.buf)-1]
			p.seq -= uint64(len(evs))
			p.sent = min(p.sent, len(p.buf))
			p.c.Close()
			p.c = nil
			return err
		}
		p.c.Close()
		p.c = nil
		if !sleep(ctx, &backoff) {
			return ctx.Err()
		}
	}
}

// stop-and-wait: send one, read its ack, drop whatever the ack says is durable
func (p *Producer) pump() error {
	for p.sent < len(p.buf) {
		b := p.buf[p.sent]
		if err := p.c.Send(p.name, b.evs); err != nil {
			return err
		}
		ack, err := p.c.Recv()
		if err != nil {
			return err
		}
		p.sent++

		drop := 0
		for drop < len(p.buf) && p.buf[drop].last <= ack.CommittedSeq {
			drop++
		}
		p.buf = p.buf[drop:]
		// after a restore the ack can already cover batches we haven't resent
		p.sent = max(p.sent-drop, 0)
	}
	return nil
}

// Unconfirmed is how many batches would be replayed after a crash right now.
func (p *Producer) Unconfirmed() int { return len(p.buf) }

func (p *Producer) Close() {
	p.cancel()
	if p.c != nil {
		p.c.Close()
	}
}

func sleep(ctx context.Context, d *time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(*d):
	}
	*d = min(*d*2, time.Second)
	return true
}
