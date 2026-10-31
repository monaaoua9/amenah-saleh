package ingest

import (
	"errors"
	"math"
	"unicode/utf8"

	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding"
	_ "google.golang.org/grpc/encoding/proto"
	"google.golang.org/grpc/mem"
	"google.golang.org/protobuf/encoding/protowire"

	"github.com/monaaoua9/streamdb/internal/event"
)

// What the server reads instead of pb.Batch. Same wire format, but it decodes
// straight into []event.Event: no pb.Event per event, and keys repeat a lot so
// they are interned per stream.
type fastBatch struct {
	Producer string
	Events   []event.Event
	keys     map[string]string
	hint     int // size of the previous batch, producers tend to send the same
}

const maxInterned = 4096

var errBadBatch = errors.New("ingest: malformed batch")

func (b *fastBatch) parse(buf []byte) error {
	b.Producer, b.Events = "", make([]event.Event, 0, b.hint)
	for len(buf) > 0 {
		num, typ, n := protowire.ConsumeTag(buf)
		if n < 0 {
			return errBadBatch
		}
		buf = buf[n:]
		if num == 1 && typ == protowire.BytesType {
			v, n := protowire.ConsumeBytes(buf)
			if n < 0 || !utf8.Valid(v) {
				return errBadBatch
			}
			b.Producer = string(v)
			buf = buf[n:]
			continue
		}
		if num == 2 && typ == protowire.BytesType {
			v, n := protowire.ConsumeBytes(buf)
			if n < 0 {
				return errBadBatch
			}
			buf = buf[n:]
			var e event.Event
			if err := b.event(v, &e); err != nil {
				return err
			}
			b.Events = append(b.Events, e)
			continue
		}
		n = protowire.ConsumeFieldValue(num, typ, buf)
		if n < 0 {
			return errBadBatch
		}
		buf = buf[n:]
	}
	b.hint = len(b.Events)
	return nil
}

func f64(v uint64) float64 { return math.Float64frombits(v) }

func (b *fastBatch) event(buf []byte, e *event.Event) error {
	for len(buf) > 0 {
		num, typ, n := protowire.ConsumeTag(buf)
		if n < 0 {
			return errBadBatch
		}
		buf = buf[n:]
		switch {
		case num == 1 && typ == protowire.BytesType:
			v, n := protowire.ConsumeBytes(buf)
			if n < 0 {
				return errBadBatch
			}
			k, ok := b.keys[string(v)] // no alloc for the lookup
			if !ok {
				if !utf8.Valid(v) {
					return errBadBatch
				}
				k = string(v)
				if len(b.keys) < maxInterned {
					if b.keys == nil {
						b.keys = make(map[string]string)
					}
					b.keys[k] = k
				}
			}
			e.Key = k
			buf = buf[n:]
		case num == 2 && typ == protowire.Fixed64Type:
			v, n := protowire.ConsumeFixed64(buf)
			if n < 0 {
				return errBadBatch
			}
			e.Value = f64(v)
			buf = buf[n:]
		case num == 3 && typ == protowire.VarintType:
			v, n := protowire.ConsumeVarint(buf)
			if n < 0 {
				return errBadBatch
			}
			e.Time = int64(v)
			buf = buf[n:]
		case num == 4 && typ == protowire.VarintType:
			v, n := protowire.ConsumeVarint(buf)
			if n < 0 {
				return errBadBatch
			}
			e.Seq = v
			buf = buf[n:]
		default:
			n := protowire.ConsumeFieldValue(num, typ, buf)
			if n < 0 {
				return errBadBatch
			}
			buf = buf[n:]
		}
	}
	return nil
}

// registered under the name "proto" so clients need no change; anything that
// isn't a fastBatch goes to the stock codec
type fastCodec struct{ base encoding.CodecV2 }

func (c fastCodec) Name() string { return c.base.Name() }

func (c fastCodec) Marshal(v any) (mem.BufferSlice, error) { return c.base.Marshal(v) }

func (c fastCodec) Unmarshal(data mem.BufferSlice, v any) error {
	fb, ok := v.(*fastBatch)
	if !ok {
		return c.base.Unmarshal(data, v)
	}
	if len(data) == 1 {
		return fb.parse(data[0].ReadOnlyData())
	}
	return fb.parse(data.Materialize())
}

func init() {
	encoding.RegisterCodecV2(fastCodec{base: encoding.GetCodecV2("proto")})
}

type streamer interface{ serve(grpc.ServerStream) error }

var ingestDesc = grpc.ServiceDesc{
	ServiceName: "dspe.v1.Ingest",
	HandlerType: (*streamer)(nil),
	Streams: []grpc.StreamDesc{{
		StreamName:    "Stream",
		Handler:       func(srv any, st grpc.ServerStream) error { return srv.(*Server).serve(st) },
		ServerStreams: true,
		ClientStreams: true,
	}},
	Metadata: "proto/ingest.proto",
}
