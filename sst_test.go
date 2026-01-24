package isledb

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/segmentio/ksuid"
)

type sliceSSTIter struct {
	entries []MemEntry
	idx     int
	err     error
}

func (it *sliceSSTIter) Next() bool {
	if it.idx >= len(it.entries) {
		return false
	}
	it.idx++
	return true
}

func (it *sliceSSTIter) Entry() MemEntry {
	return it.entries[it.idx-1]
}

func (it *sliceSSTIter) Err() error {
	return it.err
}

type memReadable struct {
	data []byte
	r    *bytes.Reader
	rh   objstorage.NoopReadHandle
}

func newMemReadable(data []byte) *memReadable {
	m := &memReadable{
		data: data,
		r:    bytes.NewReader(data),
	}
	m.rh = objstorage.MakeNoopReadHandle(m)
	return m
}

func (m *memReadable) ReadAt(_ context.Context, p []byte, off int64) error {
	n, err := m.r.ReadAt(p, off)
	if err != nil {
		return err
	}
	if n != len(p) {
		return io.ErrUnexpectedEOF
	}
	return nil
}

func (*memReadable) Close() error {
	return nil
}

func (m *memReadable) Size() int64 {
	return int64(len(m.data))
}

func (m *memReadable) NewReadHandle(_ context.Context) objstorage.ReadHandle {
	return &m.rh
}

func findVLogRef(refs []VLogRef, id ksuid.KSUID) (VLogRef, bool) {
	for _, ref := range refs {
		if ref.VLogID == id {
			return ref, true
		}
	}
	return VLogRef{}, false
}

func TestWriteSST_InlineAndPending(t *testing.T) {
	inline := []byte("x")
	pending := make([]byte, DefaultInlineThreshold+10)
	for i := range pending {
		pending[i] = byte(i % 251)
	}

	entries := []MemEntry{
		{Key: []byte("a"), Seq: 2, Kind: OpPut, Inline: true, Value: inline},
		{Key: []byte("b"), Seq: 1, Kind: OpPut, PendingValue: pending},
	}
	it := &sliceSSTIter{entries: entries}

	res, err := WriteSST(context.Background(), it, SSTWriterOptions{BlockSize: 4096, Compression: "none"}, 1)
	if err != nil {
		t.Fatalf("WriteSST error: %v", err)
	}
	if len(res.SSTData) == 0 {
		t.Fatalf("expected SST data")
	}
	if len(res.VLogData) == 0 {
		t.Fatalf("expected VLog data")
	}
	if res.Meta.SeqLo != 1 || res.Meta.SeqHi != 2 {
		t.Errorf("seq range mismatch: got %d-%d", res.Meta.SeqLo, res.Meta.SeqHi)
	}
	if !bytes.Equal(res.Meta.MinKey, []byte("a")) || !bytes.Equal(res.Meta.MaxKey, []byte("b")) {
		t.Errorf("key range mismatch: %s-%s", res.Meta.MinKey, res.Meta.MaxKey)
	}
	if res.Meta.VLogID.IsNil() || res.Meta.VLogID != res.VLogID {
		t.Errorf("vlog id mismatch: meta=%s result=%s", res.Meta.VLogID, res.VLogID)
	}

	reader, err := sstable.NewReader(newMemReadable(res.SSTData), sstable.ReaderOptions{})
	if err != nil {
		t.Fatalf("reader error: %v", err)
	}
	defer reader.Close()

	iter, err := reader.NewIter(nil, nil)
	if err != nil {
		t.Fatalf("iter error: %v", err)
	}
	defer iter.Close()

	type seenEntry struct {
		key   []byte
		value KeyEntry
		seq   uint64
	}
	var seen []seenEntry

	key, val := iter.First()
	for key != nil {
		v, _, err := val.Value(nil)
		if err != nil {
			t.Fatalf("value error: %v", err)
		}
		decoded, err := DecodeKeyEntry(key.UserKey, v)
		if err != nil {
			t.Fatalf("decode error: %v", err)
		}
		seen = append(seen, seenEntry{
			key:   append([]byte(nil), key.UserKey...),
			value: decoded,
			seq:   key.Trailer >> 8,
		})
		key, val = iter.Next()
	}
	if err := iter.Error(); err != nil {
		t.Fatalf("iter error: %v", err)
	}
	if len(seen) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(seen))
	}

	if !bytes.Equal(seen[0].key, []byte("a")) || seen[0].seq != 2 {
		t.Fatalf("first entry mismatch: key=%s seq=%d", seen[0].key, seen[0].seq)
	}
	if !seen[0].value.Inline || !bytes.Equal(seen[0].value.Value, inline) {
		t.Fatalf("inline entry mismatch")
	}

	if !bytes.Equal(seen[1].key, []byte("b")) || seen[1].seq != 1 {
		t.Fatalf("second entry mismatch: key=%s seq=%d", seen[1].key, seen[1].seq)
	}
	if seen[1].value.Inline {
		t.Fatalf("expected pointer entry")
	}
	if seen[1].value.VLogID != res.VLogID {
		t.Fatalf("vlog id mismatch: %s %s", seen[1].value.VLogID, res.VLogID)
	}

	ptr := VLogPointer{
		VLogID:   seen[1].value.VLogID,
		Offset:   seen[1].value.VOffset,
		Length:   seen[1].value.VLength,
		Checksum: seen[1].value.VChecksum,
	}
	value, err := NewVLogReader(res.VLogData).ReadValue(ptr)
	if err != nil {
		t.Fatalf("vlog read error: %v", err)
	}
	if !bytes.Equal(value, pending) {
		t.Fatalf("vlog value mismatch")
	}
}

func TestWriteSST_EmptyIterator(t *testing.T) {
	it := &sliceSSTIter{}
	_, err := WriteSST(context.Background(), it, SSTWriterOptions{BlockSize: 4096, Compression: "none"}, 1)
	if !errors.Is(err, ErrEmptyIterator) {
		t.Fatalf("expected ErrEmptyIterator, got %v", err)
	}
}

func TestWriteSST_OutOfOrder(t *testing.T) {
	entries := []MemEntry{
		{Key: []byte("b"), Seq: 1, Kind: OpPut, Inline: true, Value: []byte("x")},
		{Key: []byte("a"), Seq: 2, Kind: OpPut, Inline: true, Value: []byte("y")},
	}
	it := &sliceSSTIter{entries: entries}

	_, err := WriteSST(context.Background(), it, SSTWriterOptions{BlockSize: 4096, Compression: "none"}, 1)
	if err == nil {
		t.Fatalf("expected error")
	}
	if !errors.Is(err, ErrOutOfOrder) {
		t.Fatalf("expected ErrOutOfOrder, got %v", err)
	}
}

func TestWriteSST_DuplicateKeySeqOrder(t *testing.T) {
	entries := []MemEntry{
		{Key: []byte("a"), Seq: 1, Kind: OpPut, Inline: true, Value: []byte("v1")},
		{Key: []byte("a"), Seq: 2, Kind: OpPut, Inline: true, Value: []byte("v2")},
	}
	it := &sliceSSTIter{entries: entries}

	_, err := WriteSST(context.Background(), it, SSTWriterOptions{BlockSize: 4096, Compression: "none"}, 1)
	if !errors.Is(err, ErrOutOfOrder) {
		t.Fatalf("expected ErrOutOfOrder, got %v", err)
	}
}

func TestWriteSST_DeleteEntry(t *testing.T) {
	entries := []MemEntry{
		{Key: []byte("a"), Seq: 1, Kind: OpDelete},
	}
	it := &sliceSSTIter{entries: entries}

	res, err := WriteSST(context.Background(), it, SSTWriterOptions{BlockSize: 4096, Compression: "none"}, 1)
	if err != nil {
		t.Fatalf("WriteSST error: %v", err)
	}

	reader, err := sstable.NewReader(newMemReadable(res.SSTData), sstable.ReaderOptions{})
	if err != nil {
		t.Fatalf("reader error: %v", err)
	}
	defer reader.Close()

	iter, err := reader.NewIter(nil, nil)
	if err != nil {
		t.Fatalf("iter error: %v", err)
	}
	defer iter.Close()

	key, val := iter.First()
	if key == nil {
		t.Fatalf("expected entry")
	}
	v, _, err := val.Value(nil)
	if err != nil {
		t.Fatalf("value error: %v", err)
	}
	decoded, err := DecodeKeyEntry(key.UserKey, v)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if decoded.Kind != OpDelete {
		t.Fatalf("expected delete, got %v", decoded.Kind)
	}
}

func TestWriteSST_VLogRefs_Pending(t *testing.T) {
	pending := make([]byte, DefaultInlineThreshold+10)
	for i := range pending {
		pending[i] = byte(i % 251)
	}
	entries := []MemEntry{
		{Key: []byte("a"), Seq: 1, Kind: OpPut, PendingValue: pending},
	}
	it := &sliceSSTIter{entries: entries}

	res, err := WriteSST(context.Background(), it, SSTWriterOptions{BlockSize: 4096, Compression: "none"}, 1)
	if err != nil {
		t.Fatalf("WriteSST error: %v", err)
	}
	if len(res.Meta.VLogRefs) != 1 {
		t.Fatalf("expected 1 vlog ref, got %d", len(res.Meta.VLogRefs))
	}
	ref, ok := findVLogRef(res.Meta.VLogRefs, res.VLogID)
	if !ok {
		t.Fatalf("missing vlog ref for %s", res.VLogID)
	}
	if ref.LiveBytes != int64(len(res.VLogData)) {
		t.Fatalf("live bytes mismatch: got %d, want %d", ref.LiveBytes, len(res.VLogData))
	}
}

func TestWriteSST_VLogRefs_Pointer(t *testing.T) {
	ptrID := ksuid.New()
	entries := []MemEntry{
		{
			Key:     []byte("a"),
			Seq:     1,
			Kind:    OpPut,
			VLogPtr: &VLogPointer{VLogID: ptrID, Offset: 0, Length: 100, Checksum: 1},
		},
	}
	it := &sliceSSTIter{entries: entries}

	res, err := WriteSST(context.Background(), it, SSTWriterOptions{BlockSize: 4096, Compression: "none"}, 1)
	if err != nil {
		t.Fatalf("WriteSST error: %v", err)
	}
	if len(res.VLogData) != 0 {
		t.Fatalf("expected no vlog data")
	}
	if len(res.Meta.VLogRefs) != 1 {
		t.Fatalf("expected 1 vlog ref, got %d", len(res.Meta.VLogRefs))
	}
	ref, ok := findVLogRef(res.Meta.VLogRefs, ptrID)
	if !ok {
		t.Fatalf("missing vlog ref for %s", ptrID)
	}
	expected := int64(VLogEntryHeaderSize + 100)
	if ref.LiveBytes != expected {
		t.Fatalf("live bytes mismatch: got %d, want %d", ref.LiveBytes, expected)
	}
}
