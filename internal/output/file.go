package output

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sync"

	"github.com/monaaoua9/amenah-saleh/internal/event"
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
}

func NewFile(path string, maxBytes int64) (*File, error) {
	f := &File{path: path, maxBytes: maxBytes}
	if err := f.open(); err != nil {
		return nil, err
	}
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

func (f *File) Write(r event.Result) {
	b, _ := json.Marshal(r)
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

func (f *File) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.w.Flush()
	return f.f.Close()
}
