package output

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/monaaoua9/amenah-saleh/internal/event"
)

// Batches results and POSTs them as a JSON array. 5xx and network errors are
// retried with backoff; a 4xx drops the batch (retrying a bad request is pointless).
// Receivers should dedup on (key, start): the engine can re-send after a crash.
type Webhook struct {
	url    string
	client *http.Client
	max    int
	every  time.Duration

	mu    sync.Mutex
	buf   []event.Result
	kick  chan struct{}
	done  chan struct{}
	wg    sync.WaitGroup
	fails int
}

func NewWebhook(url string, batch int, every time.Duration) *Webhook {
	w := &Webhook{
		url: url, max: batch, every: every,
		client: &http.Client{Timeout: 10 * time.Second},
		kick:   make(chan struct{}, 1), done: make(chan struct{}),
	}
	w.wg.Add(1)
	go w.loop()
	return w
}

func (w *Webhook) Write(r event.Result) {
	w.mu.Lock()
	w.buf = append(w.buf, r)
	full := len(w.buf) >= w.max
	w.mu.Unlock()
	if full {
		select {
		case w.kick <- struct{}{}:
		default:
		}
	}
}

func (w *Webhook) loop() {
	defer w.wg.Done()
	t := time.NewTicker(w.every)
	defer t.Stop()
	for {
		select {
		case <-w.done:
			w.flush()
			return
		case <-t.C:
		case <-w.kick:
		}
		w.flush()
	}
}

func (w *Webhook) flush() {
	for {
		w.mu.Lock()
		n := min(len(w.buf), w.max)
		batch := append([]event.Result(nil), w.buf[:n]...)
		w.buf = w.buf[n:]
		w.mu.Unlock()
		if n == 0 {
			return
		}
		if err := w.post(batch); err != nil {
			w.mu.Lock()
			w.fails++
			w.mu.Unlock()
			log.Printf("webhook: dropped %d results: %v", n, err)
		}
	}
}

func (w *Webhook) post(batch []event.Result) error {
	body, err := json.Marshal(batch)
	if err != nil {
		return err
	}
	backoff := 100 * time.Millisecond
	var last error
	for try := 0; try < 5; try++ {
		if try > 0 {
			select {
			case <-time.After(backoff):
			case <-w.done:
				// shutting down: one more attempt without waiting
			}
			backoff *= 2
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, w.url, bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		resp, err := w.client.Do(req)
		cancel()
		if err != nil {
			last = err
			continue
		}
		resp.Body.Close()
		switch {
		case resp.StatusCode < 300:
			return nil
		case resp.StatusCode < 500:
			return fmt.Errorf("status %d", resp.StatusCode)
		}
		last = fmt.Errorf("status %d", resp.StatusCode)
	}
	return last
}

// Failed is the number of batches given up on so far.
func (w *Webhook) Failed() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.fails
}

func (w *Webhook) Close() {
	close(w.done)
	w.wg.Wait()
}
