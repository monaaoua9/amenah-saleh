package alert

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"
)

// Posts each alert as JSON from a single goroutine. If the endpoint is slower
// than the alerts, extra ones are dropped (and logged) rather than blocking the sink.
type Hook struct {
	url string
	ch  chan Alert
	wg  sync.WaitGroup
}

func NewHook(url string) *Hook {
	h := &Hook{url: url, ch: make(chan Alert, 1024)}
	h.wg.Add(1)
	go h.run()
	return h
}

func (h *Hook) Notify(a Alert) {
	select {
	case h.ch <- a:
	default:
		log.Printf("alert hook backed up, dropped: %s on %s", a.Rule, a.Result.Key)
	}
}

func (h *Hook) run() {
	defer h.wg.Done()
	c := &http.Client{Timeout: 5 * time.Second}
	for a := range h.ch {
		b, _ := json.Marshal(a)
		resp, err := c.Post(h.url, "application/json", bytes.NewReader(b))
		if err != nil {
			log.Printf("alert hook: %v", err)
			continue
		}
		resp.Body.Close()
	}
}

func (h *Hook) Close() {
	close(h.ch)
	h.wg.Wait()
}
