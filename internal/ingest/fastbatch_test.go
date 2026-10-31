package ingest

import (
	"fmt"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/monaaoua9/streamdb/internal/ingest/pb"
)

func sample(n int) []byte {
	b := &pb.Batch{Producer: "p1"}
	for i := 0; i < n; i++ {
		b.Events = append(b.Events, &pb.Event{Key: fmt.Sprintf("k%d", i%1000), Value: float64(i) * 1.5, Time: 1700000000000 + int64(i), Seq: uint64(i + 1)})
	}
	out, _ := proto.Marshal(b)
	return out
}

func TestFastBatchMatchesProto(t *testing.T) {
	raw := sample(300)
	var want pb.Batch
	if err := proto.Unmarshal(raw, &want); err != nil {
		t.Fatal(err)
	}
	var got fastBatch
	if err := got.parse(raw); err != nil {
		t.Fatal(err)
	}
	if got.Producer != want.Producer || len(got.Events) != len(want.Events) {
		t.Fatalf("producer/len mismatch: %q %d", got.Producer, len(got.Events))
	}
	for i, e := range want.Events {
		g := got.Events[i]
		if g.Key != e.Key || g.Value != e.Value || g.Time != e.Time || g.Seq != e.Seq {
			t.Fatalf("event %d: %+v vs %+v", i, g, e)
		}
	}
}

func TestFastBatchRejectsGarbage(t *testing.T) {
	raw := sample(5)
	var fb fastBatch
	for i := 1; i < len(raw); i += 7 {
		fb.parse(raw[:i]) // must not panic
	}
	if err := fb.parse([]byte{0x12, 0xff, 0xff, 0xff, 0x0f, 1}); err == nil {
		t.Fatal("expected error for overlong length")
	}
	if err := fb.parse([]byte{0x0a, 0x02, 0xff, 0xfe}); err == nil {
		t.Fatal("expected error for invalid utf8 producer")
	}
}

func FuzzFastBatch(f *testing.F) {
	f.Add(sample(3))
	f.Fuzz(func(t *testing.T, b []byte) {
		var fb fastBatch
		fb.parse(b)
	})
}

func BenchmarkDecode(b *testing.B) {
	raw := sample(500)
	b.Run("proto", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			var m pb.Batch
			proto.Unmarshal(raw, &m)
		}
	})
	b.Run("fast", func(b *testing.B) {
		b.ReportAllocs()
		var m fastBatch
		for i := 0; i < b.N; i++ {
			m.parse(raw)
		}
	})
}
