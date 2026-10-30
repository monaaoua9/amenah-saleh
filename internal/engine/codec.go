package engine

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"math"

	"github.com/monaaoua9/streamdb/internal/event"
)

// The sink snapshot holds every staged result, so it sits on the checkpoint
// path. gob was spending more time reflecting over Result's fields than
// encoding them; this is a flat binary form, about 3x smaller too.
//
//	"DSR1" | uvarint chunks | { uvarint n | n * result }
//	result = uvarint len(key) key | varint start | varint end | uvarint count |
//	         flags | sum sumsq min max (f64 LE) | [p50 p95 p99] | [uvarint distinct]
//
// Checkpoints written before this are gob; decodeResults still reads them.
var resultsMagic = []byte("DSR1")

const (
	flLate = 1 << iota
	flQuant
	flDistinct
)

func encodeResults(chunks [][]event.Result) ([]byte, error) {
	n := 0
	for _, c := range chunks {
		n += len(c)
	}
	b := make([]byte, 0, len(resultsMagic)+n*72)
	b = append(b, resultsMagic...)
	b = binary.AppendUvarint(b, uint64(len(chunks)))
	for _, c := range chunks {
		b = binary.AppendUvarint(b, uint64(len(c)))
		for i := range c {
			b = appendResult(b, &c[i])
		}
	}
	return b, nil
}

func appendResult(b []byte, r *event.Result) []byte {
	b = binary.AppendUvarint(b, uint64(len(r.Key)))
	b = append(b, r.Key...)
	b = binary.AppendVarint(b, r.Start)
	b = binary.AppendVarint(b, r.End)
	b = binary.AppendUvarint(b, uint64(r.Count))

	var fl byte
	if r.Late {
		fl |= flLate
	}
	if r.P50 != 0 || r.P95 != 0 || r.P99 != 0 {
		fl |= flQuant
	}
	if r.Distinct != 0 {
		fl |= flDistinct
	}
	b = append(b, fl)
	b = appendF64(b, r.Sum)
	b = appendF64(b, r.SumSq)
	b = appendF64(b, r.Min)
	b = appendF64(b, r.Max)
	if fl&flQuant != 0 {
		b = appendF64(b, r.P50)
		b = appendF64(b, r.P95)
		b = appendF64(b, r.P99)
	}
	if fl&flDistinct != 0 {
		b = binary.AppendUvarint(b, r.Distinct)
	}
	return b
}

func appendF64(b []byte, f float64) []byte {
	return binary.LittleEndian.AppendUint64(b, math.Float64bits(f))
}

var errCorrupt = errors.New("corrupt staged results")

func decodeResults(b []byte) ([]event.Result, error) {
	if !bytes.HasPrefix(b, resultsMagic) {
		return decodeGobResults(b)
	}
	d := decoder{b: b[len(resultsMagic):]}
	chunks := d.uvarint()
	var out []event.Result
	for ; chunks > 0 && d.err == nil; chunks-- {
		n := d.uvarint()
		if d.err == nil && n > uint64(len(d.b)) { // every result is at least a few bytes
			d.err = errCorrupt
		}
		for ; n > 0 && d.err == nil; n-- {
			out = append(out, d.result())
		}
	}
	if d.err != nil {
		return nil, d.err
	}
	return out, nil
}

type decoder struct {
	b   []byte
	err error
}

func (d *decoder) uvarint() uint64 {
	v, n := binary.Uvarint(d.b)
	if n <= 0 {
		d.err = errCorrupt
		return 0
	}
	d.b = d.b[n:]
	return v
}

func (d *decoder) varint() int64 {
	v, n := binary.Varint(d.b)
	if n <= 0 {
		d.err = errCorrupt
		return 0
	}
	d.b = d.b[n:]
	return v
}

func (d *decoder) f64() float64 {
	if len(d.b) < 8 {
		d.err = errCorrupt
		return 0
	}
	v := math.Float64frombits(binary.LittleEndian.Uint64(d.b))
	d.b = d.b[8:]
	return v
}

func (d *decoder) result() event.Result {
	var r event.Result
	kl := d.uvarint()
	if d.err != nil || kl > uint64(len(d.b)) {
		d.err = errCorrupt
		return r
	}
	r.Key = string(d.b[:kl])
	d.b = d.b[kl:]
	r.Start, r.End = d.varint(), d.varint()
	r.Count = int64(d.uvarint())
	if d.err != nil || len(d.b) == 0 {
		d.err = errCorrupt
		return r
	}
	fl := d.b[0]
	d.b = d.b[1:]
	r.Late = fl&flLate != 0
	r.Sum, r.SumSq, r.Min, r.Max = d.f64(), d.f64(), d.f64(), d.f64()
	if fl&flQuant != 0 {
		r.P50, r.P95, r.P99 = d.f64(), d.f64(), d.f64()
	}
	if fl&flDistinct != 0 {
		r.Distinct = d.uvarint()
	}
	return r
}

func decodeGobResults(b []byte) ([]event.Result, error) {
	var chunks [][]event.Result
	if err := gob.NewDecoder(bytes.NewReader(b)).Decode(&chunks); err != nil {
		return nil, err
	}
	var flat []event.Result
	for _, c := range chunks {
		flat = append(flat, c...)
	}
	return flat, nil
}
