package window

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"math"
	"sort"

	"github.com/monaaoua9/streamdb/internal/hll"
	"github.com/monaaoua9/streamdb/internal/sketch"
)

// A worker builds its snapshot on its own goroutine while the barrier is
// aligned, so processing stops until it's done. gob made that proportional to
// reflection over every open window; this is a flat binary form instead.
// Snapshots from before it are gob and still load (no magic prefix).
//
//	"WSN1" | late | wm(max, init, disorder) | nEntries | entries | nSess | sessions
var snapMagic = []byte("WSN1")

const (
	afFired = 1 << iota
	afQuant
	afDistinct
)

func encodeSnap(s *snap) []byte {
	b := make([]byte, 0, 64+len(s.Entries)*64+len(s.Sess)*80)
	b = append(b, snapMagic...)
	b = binary.AppendUvarint(b, s.Late)
	b = binary.AppendVarint(b, s.WM.Max)
	b = append(b, boolByte(s.WM.Init))
	b = binary.AppendVarint(b, s.WM.Disorder)

	b = binary.AppendUvarint(b, uint64(len(s.Entries)))
	for i := range s.Entries {
		e := &s.Entries[i]
		b = appendStr(b, e.K.Key)
		b = binary.AppendVarint(b, e.K.Start)
		b = appendAcc(b, &e.A)
	}
	b = binary.AppendUvarint(b, uint64(len(s.Sess)))
	for i := range s.Sess {
		e := &s.Sess[i]
		b = appendStr(b, e.Key)
		b = binary.AppendVarint(b, e.S.Start)
		b = binary.AppendVarint(b, e.S.End)
		b = appendAcc(b, &e.S.A)
	}
	return b
}

func boolByte(v bool) byte {
	if v {
		return 1
	}
	return 0
}

func appendStr(b []byte, s string) []byte {
	b = binary.AppendUvarint(b, uint64(len(s)))
	return append(b, s...)
}

func appendF64(b []byte, f float64) []byte {
	return binary.LittleEndian.AppendUint64(b, math.Float64bits(f))
}

func appendAcc(b []byte, a *acc) []byte {
	var fl byte
	if a.Fired {
		fl |= afFired
	}
	if a.Q != nil {
		fl |= afQuant
	}
	if a.D != nil {
		fl |= afDistinct
	}
	b = append(b, fl)
	b = binary.AppendUvarint(b, uint64(a.Count))
	b = appendF64(b, a.Sum)
	b = appendF64(b, a.SumSq)
	b = appendF64(b, a.Min)
	b = appendF64(b, a.Max)
	if a.Q != nil {
		b = appendSketch(b, a.Q)
	}
	if a.D != nil {
		b = appendHLL(b, a.D)
	}
	return b
}

func appendSketch(b []byte, q *sketch.Sketch) []byte {
	b = binary.AppendUvarint(b, q.N)
	b = binary.AppendUvarint(b, uint64(q.Zero))
	b = appendBuckets(b, q.Pos)
	return appendBuckets(b, q.Neg)
}

// sorted so the same state always produces the same bytes
func appendBuckets(b []byte, m map[int32]uint32) []byte {
	idx := make([]int32, 0, len(m))
	for i := range m {
		idx = append(idx, i)
	}
	sort.Slice(idx, func(a, c int) bool { return idx[a] < idx[c] })
	b = binary.AppendUvarint(b, uint64(len(idx)))
	for _, i := range idx {
		b = binary.AppendVarint(b, int64(i))
		b = binary.AppendUvarint(b, uint64(m[i]))
	}
	return b
}

func appendHLL(b []byte, h *hll.Sketch) []byte {
	// registers are mostly zero in a young window: store only the used ones
	n := 0
	for _, r := range h.R {
		if r != 0 {
			n++
		}
	}
	b = binary.AppendUvarint(b, uint64(len(h.R)))
	b = binary.AppendUvarint(b, uint64(n))
	for i, r := range h.R {
		if r != 0 {
			b = binary.AppendUvarint(b, uint64(i))
			b = append(b, r)
		}
	}
	return b
}

var errBadSnap = errors.New("window: corrupt snapshot")

type reader struct {
	b   []byte
	err error
}

func (r *reader) fail() { r.err = errBadSnap }

func (r *reader) uvarint() uint64 {
	v, n := binary.Uvarint(r.b)
	if n <= 0 {
		r.fail()
		return 0
	}
	r.b = r.b[n:]
	return v
}

func (r *reader) varint() int64 {
	v, n := binary.Varint(r.b)
	if n <= 0 {
		r.fail()
		return 0
	}
	r.b = r.b[n:]
	return v
}

func (r *reader) f64() float64 {
	if len(r.b) < 8 {
		r.fail()
		return 0
	}
	v := math.Float64frombits(binary.LittleEndian.Uint64(r.b))
	r.b = r.b[8:]
	return v
}

func (r *reader) byte() byte {
	if len(r.b) == 0 {
		r.fail()
		return 0
	}
	v := r.b[0]
	r.b = r.b[1:]
	return v
}

func (r *reader) str() string {
	n := r.uvarint()
	if r.err != nil || n > uint64(len(r.b)) {
		r.fail()
		return ""
	}
	s := string(r.b[:n])
	r.b = r.b[n:]
	return s
}

// a count can't exceed the bytes left, which stops a corrupt length from
// allocating gigabytes
func (r *reader) count() int {
	n := r.uvarint()
	if r.err != nil || n > uint64(len(r.b)) {
		r.fail()
		return 0
	}
	return int(n)
}

func (r *reader) acc() acc {
	var a acc
	fl := r.byte()
	a.Fired = fl&afFired != 0
	a.Count = int64(r.uvarint())
	a.Sum, a.SumSq, a.Min, a.Max = r.f64(), r.f64(), r.f64(), r.f64()
	if fl&afQuant != 0 {
		a.Q = r.sketch()
	}
	if fl&afDistinct != 0 {
		a.D = r.hll()
	}
	return a
}

func (r *reader) sketch() *sketch.Sketch {
	q := sketch.New()
	q.N = r.uvarint()
	q.Zero = uint32(r.uvarint())
	q.Pos = r.buckets()
	q.Neg = r.buckets()
	return q
}

func (r *reader) buckets() map[int32]uint32 {
	n := r.count()
	if n == 0 {
		return nil
	}
	m := make(map[int32]uint32, n)
	for ; n > 0 && r.err == nil; n-- {
		i := int32(r.varint())
		m[i] = uint32(r.uvarint())
	}
	return m
}

func (r *reader) hll() *hll.Sketch {
	size := r.uvarint()
	used := r.count()
	h := hll.New()
	if r.err != nil || size != uint64(len(h.R)) {
		r.fail()
		return h
	}
	for ; used > 0 && r.err == nil; used-- {
		i := r.uvarint()
		v := r.byte()
		if i >= size {
			r.fail()
			break
		}
		h.R[i] = v
	}
	return h
}

func decodeSnap(b []byte) (snap, error) {
	var s snap
	if !bytes.HasPrefix(b, snapMagic) {
		err := gob.NewDecoder(bytes.NewReader(b)).Decode(&s)
		return s, err
	}
	r := reader{b: b[len(snapMagic):]}
	s.Late = r.uvarint()
	s.WM.Max = r.varint()
	s.WM.Init = r.byte() == 1
	s.WM.Disorder = r.varint()

	n := r.count()
	s.Entries = make([]entry, 0, n)
	for ; n > 0 && r.err == nil; n-- {
		var e entry
		e.K.Key = r.str()
		e.K.Start = r.varint()
		e.A = r.acc()
		s.Entries = append(s.Entries, e)
	}
	n = r.count()
	s.Sess = make([]sessEntry, 0, n)
	for ; n > 0 && r.err == nil; n-- {
		var e sessEntry
		e.Key = r.str()
		e.S.Start = r.varint()
		e.S.End = r.varint()
		e.S.A = r.acc()
		s.Sess = append(s.Sess, e)
	}
	if r.err != nil {
		return snap{}, r.err
	}
	return s, nil
}
