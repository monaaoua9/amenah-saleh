package checkpoint

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync/atomic"
)

type Store struct {
	dir      string
	failNext atomic.Bool // tests only
}

func (s *Store) FailCommitForTest(on bool) { s.failNext.Store(on) }

func NewStore(dir string) *Store { return &Store{dir: dir} }

func (s *Store) path(id uint64) string {
	return filepath.Join(s.dir, fmt.Sprintf("chk-%08d", id))
}

func (s *Store) Put(id uint64, task string, data []byte) error {
	d := s.path(id)
	if err := os.MkdirAll(d, 0o755); err != nil {
		return err
	}
	tmp := filepath.Join(d, task+".tmp")
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(d, task+".snap")); err != nil {
		return err
	}
	syncDir(d)
	return nil
}

// marker files: write, fsync, then fsync the directory so the entry survives power loss
func (s *Store) marker(id uint64, name string) error {
	d := s.path(id)
	f, err := os.Create(filepath.Join(d, name))
	if err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	syncDir(d)
	return nil
}

// best effort: not every platform lets you open a directory
func syncDir(d string) {
	if runtime.GOOS == "windows" {
		return // directories cannot be fsynced there
	}
	if f, err := os.Open(d); err == nil {
		f.Sync()
		f.Close()
	}
}

func (s *Store) Get(id uint64, task string) ([]byte, error) {
	return os.ReadFile(filepath.Join(s.path(id), task+".snap"))
}

// a checkpoint doesn't exist until this marker does
func (s *Store) Commit(id uint64) error {
	if s.failNext.Load() {
		return errors.New("injected commit failure")
	}
	return s.marker(id, "_COMPLETE")
}

// set once the sink has handed the epoch's output downstream
func (s *Store) MarkEmitted(id uint64) error {
	return s.marker(id, "_EMITTED")
}

func (s *Store) Emitted(id uint64) bool {
	_, err := os.Stat(filepath.Join(s.path(id), "_EMITTED"))
	return err == nil
}

func (s *Store) Latest() (uint64, bool) {
	ids := s.complete()
	if len(ids) == 0 {
		return 0, false
	}
	return ids[len(ids)-1], true
}

func (s *Store) Prune(keep int) {
	ids := s.complete()
	for len(ids) > keep {
		os.RemoveAll(s.path(ids[0]))
		ids = ids[1:]
	}
}

func (s *Store) complete() []uint64 {
	ents, _ := os.ReadDir(s.dir)
	var ids []uint64
	for _, e := range ents {
		var id uint64
		if !strings.HasPrefix(e.Name(), "chk-") {
			continue
		}
		fmt.Sscanf(e.Name(), "chk-%d", &id)
		if _, err := os.Stat(filepath.Join(s.dir, e.Name(), "_COMPLETE")); err == nil {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}
