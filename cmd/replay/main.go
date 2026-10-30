package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"time"

	"github.com/monaaoua9/streamdb/internal/event"
	"github.com/monaaoua9/streamdb/internal/ingest"
	"github.com/monaaoua9/streamdb/internal/replay"
	"github.com/monaaoua9/streamdb/internal/security"
)

func main() {
	addr := flag.String("addr", "localhost:7070", "engine address")
	name := flag.String("producer", "replay", "producer name (sequence numbers are per producer)")
	format := flag.String("format", "", "csv or jsonl, guessed from the file extension when empty")
	speed := flag.Float64("speed", 0, "1 = real time by event timestamps, 10 = ten times faster, 0 = as fast as possible")
	batch := flag.Int("batch", 500, "events per batch")
	token := flag.String("token", "", "bearer token")
	tlsOn := flag.Bool("tls", false, "connect with TLS")
	ca := flag.String("ca", "", "CA or self-signed cert to trust (implies -tls)")
	flag.Parse()

	if flag.NArg() != 1 {
		log.Fatal("usage: replay [flags] <file>")
	}
	path := flag.Arg(0)
	if *format == "" {
		*format = strings.TrimPrefix(strings.ToLower(filepath.Ext(path)), ".")
	}

	f, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	opts, err := security.DialOptions(*token, *tlsOn, *ca)
	if err != nil {
		log.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	prod := ingest.NewProducer(*addr, *name, opts...)
	defer prod.Close()

	pace := replay.NewPacer(*speed)
	buf := make([]event.Event, 0, *batch)
	var total int
	t0 := time.Now()

	flush := func() error {
		if len(buf) == 0 {
			return nil
		}
		if err := prod.Send(ctx, buf); err != nil {
			return err
		}
		total += len(buf)
		buf = make([]event.Event, 0, *batch) // Send keeps the slice until it is confirmed
		return nil
	}

	err = replay.Read(f, *format, func(e event.Event) error {
		if d := pace.Delay(e.Time); d > 0 {
			if err := flush(); err != nil {
				return err
			}
			select {
			case <-time.After(d):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		buf = append(buf, e)
		if len(buf) >= *batch {
			return flush()
		}
		return nil
	})
	if err == nil {
		err = flush()
	}
	if err != nil {
		log.Fatalf("after %d events: %v", total, err)
	}
	fmt.Printf("sent %d events in %v\n", total, time.Since(t0).Round(time.Millisecond))
}
