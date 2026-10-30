package output

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/monaaoua9/streamdb/internal/event"
)

// JSONL writer that rolls to out.1, out.2, ... once the current file passes maxBytes.
type File struct {
	path     string
	maxBytes int64

	mu   sync.Mutex
	f    *os.File
	w    *bufio.Writer
	size int64
	n    int

	stop chan struct{}
	done chan struct{}
}

func NewFile(path string, maxBytes int64) (*File, error) {
	f := &File{path: path, maxBytes: maxBytes, stop: make(chan struct{}), done: make(chan struct{})}
	if err := f.open(); err != nil {
		return nil, err
	}
	go f.flushLoop()
	return f, nil
}

func (f *File) open() error {
	fh, err := os.OpenFile(f.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	st, err := fh.Stat()
	if err != nil {
		fh.Close()
		return err
	}
	f.f, f.w, f.size = fh, bufio.NewWriterSize(fh, 64<<10), st.Size()
	return nil
}

func (f *File) Write(r event.Result) { f.line(r) }

func (f *File) WriteEvent(e event.Event) { f.line(e) }

func (f *File) line(v any) {
	b, _ := json.Marshal(v)
	b = append(b, '\n')

	f.mu.Lock()
	defer f.mu.Unlock()
	if f.maxBytes > 0 && f.size > 0 && f.size+int64(len(b)) > f.maxBytes {
		f.roll()
	}
	n, _ := f.w.Write(b)
	f.size += int64(n)
}

func (f *File) roll() {
	f.w.Flush()
	f.f.Close()
	f.n++
	os.Rename(f.path, fmt.Sprintf("%s.%d", f.path, f.n))
	f.open()
}

func (f *File) Flush() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.w.Flush(); err != nil {
		return err
	}
	return f.f.Sync()
}

// without this a quiet stream leaves its last results sitting in the buffer
func (f *File) flushLoop() {
	defer close(f.done)
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-f.stop:
			return
		case <-t.C:
			f.mu.Lock()
			f.w.Flush()
			f.mu.Unlock()
		}
	}
}

func (f *File) Close() error {
	close(f.stop)
	<-f.done
	f.mu.Lock()
	defer f.mu.Unlock()
	f.w.Flush()
	return f.f.Close()
}
