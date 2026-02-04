package isledb

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"errors"
	"io"
	"testing"

	"github.com/ankur-anand/isledb/internal"
	"github.com/cockroachdb/pebble/v2/objstorage"
	"github.com/cockroachdb/pebble/v2/sstable"
)

type sliceSSTIter struct {
	entries []internal.MemEntry
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

func (it *sliceSSTIter) Entry() internal.MemEntry {
	return it.entries[it.idx-1]
}

func (it *sliceSSTIter) Err() error {
	return it.err
}

func (it *sliceSSTIter) Close() error {
	return nil
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

func (m *memReadable) NewReadHandle(_ objstorage.ReadBeforeSize) objstorage.ReadHandle {
	return &m.rh
}

func TestWriteSST_Inline(t *testing.T) {
	inline := []byte("x")

	entries := []internal.MemEntry{
		{Key: []byte("a"), Seq: 2, Kind: internal.OpPut, Inline: true, Value: inline},
		{Key: []byte("b"), Seq: 1, Kind: internal.OpPut, Inline: true, Value: []byte("y")},
	}
	it := &sliceSSTIter{entries: entries}

	res, err := writeSST(context.Background(), it, SSTWriterOptions{BlockSize: 4096, Compression: "none"}, 1)
	if err != nil {
		t.Fatalf("writeSST error: %v", err)
	}
	if len(res.SSTData) == 0 {
		t.Fatalf("expected SST data")
	}
	if res.Meta.SeqLo != 1 || res.Meta.SeqHi != 2 {
		t.Errorf("seq range mismatch: got %d-%d", res.Meta.SeqLo, res.Meta.SeqHi)
	}
	if !bytes.Equal(res.Meta.MinKey, []byte("a")) || !bytes.Equal(res.Meta.MaxKey, []byte("b")) {
		t.Errorf("key range mismatch: %s-%s", res.Meta.MinKey, res.Meta.MaxKey)
	}

	reader, err := sstable.NewReader(context.Background(), newMemReadable(sstPayload(t, res.Meta, res.SSTData)), sstable.ReaderOptions{})
	if err != nil {
		t.Fatalf("reader error: %v", err)
	}
	defer reader.Close()

	iter, err := reader.NewIter(sstable.NoTransforms, nil, nil, sstable.AssertNoBlobHandles)
	if err != nil {
		t.Fatalf("iter error: %v", err)
	}
	defer iter.Close()

	type seenEntry struct {
		key   []byte
		value internal.KeyEntry
		seq   uint64
	}
	var seen []seenEntry

	for kv := iter.First(); kv != nil; kv = iter.Next() {
		v, _, err := kv.V.Value(nil)
		if err != nil {
			t.Fatalf("value error: %v", err)
		}
		decoded, err := internal.DecodeKeyEntry(kv.K.UserKey, v)
		if err != nil {
			t.Fatalf("decode error: %v", err)
		}
		seen = append(seen, seenEntry{
			key:   append([]byte(nil), kv.K.UserKey...),
			value: decoded,
			seq:   uint64(kv.K.Trailer >> 8),
		})
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
	if !seen[1].value.Inline || !bytes.Equal(seen[1].value.Value, []byte("y")) {
		t.Fatalf("expected inline entry")
	}
}

func TestWriteSST_BlobReference(t *testing.T) {
	blobID := internal.ComputeBlobID([]byte("large-value-content"))

	entries := []internal.MemEntry{
		{Key: []byte("a"), Seq: 1, Kind: internal.OpPut, Inline: false, BlobID: blobID},
	}
	it := &sliceSSTIter{entries: entries}

	res, err := writeSST(context.Background(), it, SSTWriterOptions{BlockSize: 4096, Compression: "none"}, 1)
	if err != nil {
		t.Fatalf("writeSST error: %v", err)
	}
	if len(res.SSTData) == 0 {
		t.Fatalf("expected SST data")
	}

	reader, err := sstable.NewReader(context.Background(), newMemReadable(sstPayload(t, res.Meta, res.SSTData)), sstable.ReaderOptions{})
	if err != nil {
		t.Fatalf("reader error: %v", err)
	}
	defer reader.Close()

	iter, err := reader.NewIter(sstable.NoTransforms, nil, nil, sstable.AssertNoBlobHandles)
	if err != nil {
		t.Fatalf("iter error: %v", err)
	}
	defer iter.Close()

	kv := iter.First()
	if kv == nil {
		t.Fatalf("expected entry")
	}
	v, _, err := kv.V.Value(nil)
	if err != nil {
		t.Fatalf("value error: %v", err)
	}
	decoded, err := internal.DecodeKeyEntry(kv.K.UserKey, v)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if decoded.Inline {
		t.Fatalf("expected blob reference, got inline")
	}
	if !decoded.HasBlobID() {
		t.Fatalf("expected blob ID")
	}
	if decoded.BlobID != blobID {
		t.Fatalf("blob id mismatch: got %x, want %x", decoded.BlobID, blobID)
	}
}

func TestWriteSST_EmptyIterator(t *testing.T) {
	it := &sliceSSTIter{}
	_, err := writeSST(context.Background(), it, SSTWriterOptions{BlockSize: 4096, Compression: "none"}, 1)
	if !errors.Is(err, ErrEmptyIterator) {
		t.Fatalf("expected ErrEmptyIterator, got %v", err)
	}
}

func TestWriteSST_OutOfOrder(t *testing.T) {
	entries := []internal.MemEntry{
		{Key: []byte("b"), Seq: 1, Kind: internal.OpPut, Inline: true, Value: []byte("x")},
		{Key: []byte("a"), Seq: 2, Kind: internal.OpPut, Inline: true, Value: []byte("y")},
	}
	it := &sliceSSTIter{entries: entries}

	_, err := writeSST(context.Background(), it, SSTWriterOptions{BlockSize: 4096, Compression: "none"}, 1)
	if err == nil {
		t.Fatalf("expected error")
	}
	if !errors.Is(err, ErrOutOfOrder) {
		t.Fatalf("expected ErrOutOfOrder, got %v", err)
	}
}

func TestWriteSST_DuplicateKeySeqOrder(t *testing.T) {
	entries := []internal.MemEntry{
		{Key: []byte("a"), Seq: 1, Kind: internal.OpPut, Inline: true, Value: []byte("v1")},
		{Key: []byte("a"), Seq: 2, Kind: internal.OpPut, Inline: true, Value: []byte("v2")},
	}
	it := &sliceSSTIter{entries: entries}

	_, err := writeSST(context.Background(), it, SSTWriterOptions{BlockSize: 4096, Compression: "none"}, 1)
	if !errors.Is(err, ErrOutOfOrder) {
		t.Fatalf("expected ErrOutOfOrder, got %v", err)
	}
}

func TestWriteSST_DeleteEntry(t *testing.T) {
	entries := []internal.MemEntry{
		{Key: []byte("a"), Seq: 1, Kind: internal.OpDelete},
	}
	it := &sliceSSTIter{entries: entries}

	res, err := writeSST(context.Background(), it, SSTWriterOptions{BlockSize: 4096, Compression: "none"}, 1)
	if err != nil {
		t.Fatalf("writeSST error: %v", err)
	}

	reader, err := sstable.NewReader(context.Background(), newMemReadable(sstPayload(t, res.Meta, res.SSTData)), sstable.ReaderOptions{})
	if err != nil {
		t.Fatalf("reader error: %v", err)
	}
	defer reader.Close()

	iter, err := reader.NewIter(sstable.NoTransforms, nil, nil, sstable.AssertNoBlobHandles)
	if err != nil {
		t.Fatalf("iter error: %v", err)
	}
	defer iter.Close()

	kv := iter.First()
	if kv == nil {
		t.Fatalf("expected entry")
	}
	v, _, err := kv.V.Value(nil)
	if err != nil {
		t.Fatalf("value error: %v", err)
	}
	decoded, err := internal.DecodeKeyEntry(kv.K.UserKey, v)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if decoded.Kind != internal.OpDelete {
		t.Fatalf("expected delete, got %v", decoded.Kind)
	}
}

type ed25519Signer struct {
	priv  ed25519.PrivateKey
	keyID string
}

func (s *ed25519Signer) Algorithm() string { return "ed25519" }
func (s *ed25519Signer) KeyID() string     { return s.keyID }
func (s *ed25519Signer) SignHash(hash []byte) ([]byte, error) {
	return ed25519.Sign(s.priv, hash), nil
}

func TestWriteSST_Signature(t *testing.T) {
	entries := []internal.MemEntry{
		{Key: []byte("a"), Seq: 1, Kind: internal.OpPut, Inline: true, Value: []byte("v")},
	}
	it := &sliceSSTIter{entries: entries}
	seed := bytes.Repeat([]byte{0x42}, ed25519.SeedSize)
	priv := ed25519.NewKeyFromSeed(seed)
	pub := priv.Public().(ed25519.PublicKey)
	signer := &ed25519Signer{
		priv:  priv,
		keyID: hex.EncodeToString(pub),
	}

	res, err := writeSST(context.Background(), it, SSTWriterOptions{BlockSize: 4096, Compression: "none", Signer: signer}, 1)
	if err != nil {
		t.Fatalf("writeSST error: %v", err)
	}
	if res.Meta.Signature == nil {
		t.Fatalf("expected signature")
	}
	if res.Meta.Signature.Algorithm != "ed25519" {
		t.Fatalf("algorithm mismatch: got %s, want ed25519", res.Meta.Signature.Algorithm)
	}
	if res.Meta.Signature.KeyID != signer.keyID {
		t.Fatalf("key id mismatch: got %s, want %s", res.Meta.Signature.KeyID, signer.keyID)
	}
	if res.Meta.Signature.Hash == "" {
		t.Fatalf("expected hash")
	}
	hashBytes, err := hex.DecodeString(res.Meta.Signature.Hash)
	if err != nil {
		t.Fatalf("hash decode error: %v", err)
	}
	if !ed25519.Verify(pub, hashBytes, res.Meta.Signature.Signature) {
		t.Fatalf("signature verify failed")
	}
}

func sstPayload(tb testing.TB, meta SSTMeta, data []byte) []byte {
	tb.Helper()
	if meta.Size <= 0 {
		return data
	}
	if int64(len(data)) < meta.Size {
		tb.Fatalf("sst payload too short: %d < %d", len(data), meta.Size)
	}
	return data[:meta.Size]
}
