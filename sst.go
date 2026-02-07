package isledb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"strings"
	"time"

	"github.com/ankur-anand/isledb/internal"
	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/bloom"
	"github.com/cockroachdb/pebble/v2/sstable"
)

var ErrEmptyIterator = errors.New("iterator produced no entries")

var ErrOutOfOrder = errors.New("iterator out of order")

type SSTIterator interface {
	Next() bool
	Entry() internal.MemEntry
	Err() error
	Close() error
}

type writeSSTResult struct {
	Meta    SSTMeta
	SSTData []byte
}

func writeSST(ctx context.Context, it SSTIterator, opts SSTWriterOptions, epoch uint64) (writeSSTResult, error) {
	defer it.Close()

	var result writeSSTResult

	sstBuf := new(bytes.Buffer)
	writable := newHashingWritable(sstBuf)
	var hashes []uint64

	wo := sstable.WriterOptions{
		BlockSize:   opts.BlockSize,
		Compression: compressionFromString(opts.Compression),
	}
	if opts.BloomBitsPerKey > 0 {
		wo.FilterPolicy = bloom.FilterPolicy(opts.BloomBitsPerKey)
	}

	sst := sstable.NewWriter(writable, wo)

	state := newSSTBuildState()
	hasBlobRefs := false
	abort := func(err error) (writeSSTResult, error) {
		writable.Abort()
		_ = sst.Close()
		return result, err
	}

	for it.Next() {
		if err := ctx.Err(); err != nil {
			return abort(err)
		}

		e := it.Entry()
		k := append([]byte(nil), e.Key...)
		if opts.BloomBitsPerKey > 0 {
			hashes = append(hashes, bloomHashKey(k))
		}

		keyEntry, err := buildKeyEntry(e, k)
		if err != nil {
			return abort(err)
		}
		if keyEntry.Kind == internal.OpPut && !keyEntry.Inline {
			hasBlobRefs = true
		}

		encodedValue := internal.EncodeKeyEntry(keyEntry)

		if err := state.updateOrder(k, e.Seq); err != nil {
			return abort(err)
		}

		kind := pebble.InternalKeyKindSet
		if e.Kind == internal.OpDelete {
			kind = pebble.InternalKeyKindDelete
		}

		ikey := pebble.MakeInternalKey(k, pebble.SeqNum(e.Seq), kind)

		if err := sst.Raw().Add(ikey, encodedValue, false); err != nil {
			return abort(err)
		}

		state.updateBounds(k, e.Seq)
	}

	if err := it.Err(); err != nil {
		return abort(err)
	}
	if !state.found {
		return abort(ErrEmptyIterator)
	}
	if err := sst.Close(); err != nil {
		return result, err
	}

	sstSize := writable.size
	var bloomBytes []byte
	var bloomK int
	if opts.BloomBitsPerKey > 0 {
		var err error
		bloomBytes, bloomK, err = buildBloomBytes(hashes, opts.BloomBitsPerKey)
		if err != nil {
			return result, err
		}
		if len(bloomBytes) > 0 {
			if _, err := sstBuf.Write(bloomBytes); err != nil {
				return result, err
			}
			if err := appendBloomTrailer(sstBuf, int64(len(bloomBytes))); err != nil {
				return result, err
			}
		}
	}

	hashBytes := writable.sumBytes()
	hashStr := hex.EncodeToString(hashBytes)

	result.SSTData = sstBuf.Bytes()

	result.Meta = SSTMeta{
		ID:       buildSSTID(epoch, state.seqLo, state.seqHi, hashStr),
		Epoch:    epoch,
		SeqLo:    state.seqLo,
		SeqHi:    state.seqHi,
		MinKey:   state.minKey,
		MaxKey:   state.maxKey,
		Size:     sstSize,
		Checksum: "sha256:" + hashStr,
		Bloom: BloomMeta{
			BitsPerKey: opts.BloomBitsPerKey,
			K:          bloomK,
			Offset:     sstSize,
			Length:     int64(len(bloomBytes)),
		},
		CreatedAt:   time.Now().UTC(),
		Level:       0,
		HasBlobRefs: hasBlobRefs,
	}
	if opts.Signer != nil {
		sig, err := opts.Signer.SignHash(hashBytes)
		if err != nil {
			return result, err
		}
		result.Meta.Signature = &SSTSignature{
			Algorithm: opts.Signer.Algorithm(),
			KeyID:     opts.Signer.KeyID(),
			Hash:      hashStr,
			Signature: sig,
		}
	}

	return result, nil
}

type sstBuildState struct {
	minKey  []byte
	maxKey  []byte
	found   bool
	seqLo   uint64
	seqHi   uint64
	prevKey []byte
	prevSeq uint64
}

func newSSTBuildState() *sstBuildState {
	return &sstBuildState{
		seqLo: ^uint64(0),
	}
}

func (s *sstBuildState) updateOrder(key []byte, seq uint64) error {
	if s.found {
		switch cmp := bytes.Compare(s.prevKey, key); {
		case cmp > 0:
			return fmt.Errorf("%w: keys must be sorted ascending (prev=%q curr=%q)", ErrOutOfOrder, s.prevKey, key)
		case cmp == 0 && seq > s.prevSeq:
			return fmt.Errorf("%w: sequence must be non-increasing for duplicate keys (prev=%d curr=%d)", ErrOutOfOrder, s.prevSeq, seq)
		}
	}
	s.prevKey = key
	s.prevSeq = seq
	return nil
}

func (s *sstBuildState) updateBounds(key []byte, seq uint64) {
	if !s.found {
		s.minKey = key
		s.seqLo = seq
		s.found = true
	}
	s.maxKey = key
	if seq < s.seqLo {
		s.seqLo = seq
	}
	if seq > s.seqHi {
		s.seqHi = seq
	}
}

func buildKeyEntry(e internal.MemEntry, key []byte) (internal.KeyEntry, error) {
	keyEntry := internal.KeyEntry{
		Key:      key,
		Seq:      e.Seq,
		Kind:     e.Kind,
		ExpireAt: e.ExpireAt,
	}

	if e.Kind == internal.OpDelete {
		return keyEntry, nil
	}

	if e.Inline {
		keyEntry.Inline = true
		keyEntry.Value = e.Value
		return keyEntry, nil
	}

	var zeroBlobID [32]byte
	if e.BlobID != zeroBlobID {
		keyEntry.Inline = false
		keyEntry.BlobID = e.BlobID
		return keyEntry, nil
	}

	return internal.KeyEntry{}, fmt.Errorf("corrupt entry: non-inline, non-blob for key %q", key)
}

func buildSSTID(epoch, seqLo, seqHi uint64, hashHex string) string {
	shortHash := hashHex
	if len(shortHash) > 12 {
		shortHash = shortHash[:12]
	}
	return fmt.Sprintf("%d-%d-%d-%s.sst", epoch, seqLo, seqHi, shortHash)
}

func compressionFromString(name string) *sstable.CompressionProfile {
	switch strings.ToLower(name) {
	case "none", "no":
		return sstable.NoCompression
	case "zstd":
		return sstable.ZstdCompression
	case "snappy", "":
		return sstable.SnappyCompression
	default:
		return sstable.SnappyCompression
	}
}

type hashingWritable struct {
	w       io.Writer
	hash    hash.Hash
	size    int64
	aborted bool
}

func newHashingWritable(w io.Writer) *hashingWritable {
	return &hashingWritable{
		w:    w,
		hash: sha256.New(),
	}
}

func (h *hashingWritable) Write(p []byte) error {
	if h.aborted {
		return errors.New("write after abort")
	}
	n, err := h.w.Write(p)
	h.size += int64(n)
	if n > 0 {
		_, _ = h.hash.Write(p[:n])
	}
	if err != nil {
		return err
	}
	if n != len(p) {
		return io.ErrShortWrite
	}
	return nil
}

func (h *hashingWritable) Finish() error {
	if h.aborted {
		return errors.New("finish after abort")
	}
	return nil
}

func (h *hashingWritable) Abort() { h.aborted = true }

func (h *hashingWritable) checksum() string {
	return hex.EncodeToString(h.hash.Sum(nil))
}

func (h *hashingWritable) sumBytes() []byte {
	return h.hash.Sum(nil)
}
