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

type WriteSSTResult struct {
	Meta    SSTMeta
	SSTData []byte
}

func WriteSST(ctx context.Context, it SSTIterator, opts SSTWriterOptions, epoch uint64) (WriteSSTResult, error) {
	defer it.Close()

	var result WriteSSTResult

	sstBuf := new(bytes.Buffer)
	writable := newHashingWritable(sstBuf)

	wo := sstable.WriterOptions{
		BlockSize:   opts.BlockSize,
		Compression: compressionFromString(opts.Compression),
	}
	if opts.BloomBitsPerKey > 0 {
		wo.FilterPolicy = bloom.FilterPolicy(opts.BloomBitsPerKey)
	}

	sst := sstable.NewWriter(writable, wo)

	state := newSSTBuildState()
	abort := func(err error) (WriteSSTResult, error) {
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

		keyEntry, err := buildKeyEntry(e, k)
		if err != nil {
			return abort(err)
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

	hashBytes := writable.sumBytes()
	hashStr := hex.EncodeToString(hashBytes)

	result.SSTData = sstBuf.Bytes()

	result.Meta = SSTMeta{
		ID:        buildSSTID(epoch, state.seqLo, state.seqHi, hashStr),
		Epoch:     epoch,
		SeqLo:     state.seqLo,
		SeqHi:     state.seqHi,
		MinKey:    state.minKey,
		MaxKey:    state.maxKey,
		Size:      writable.size,
		Checksum:  "sha256:" + hashStr,
		Bloom:     BloomMeta{BitsPerKey: opts.BloomBitsPerKey},
		CreatedAt: time.Now().UTC(),
		Level:     0,
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

func writeMultipleSSTs(ctx context.Context, it SSTIterator, opts SSTWriterOptions, epoch uint64, targetSize int64, onSSTComplete func(*WriteSSTResult) error) ([]WriteSSTResult, error) {
	defer it.Close()

	var results []WriteSSTResult
	var currentSize int64
	var sstBuf *bytes.Buffer
	var writable *hashingWritable
	var sst *sstable.Writer
	var state *sstBuildState

	wo := sstable.WriterOptions{
		BlockSize:   opts.BlockSize,
		Compression: compressionFromString(opts.Compression),
	}
	if opts.BloomBitsPerKey > 0 {
		wo.FilterPolicy = bloom.FilterPolicy(opts.BloomBitsPerKey)
	}

	startNewSST := func() {
		sstBuf = new(bytes.Buffer)
		writable = newHashingWritable(sstBuf)
		sst = sstable.NewWriter(writable, wo)
		state = newSSTBuildState()
		currentSize = 0
	}

	finishCurrentSST := func() error {
		if sst == nil || !state.found {
			return nil
		}

		if err := sst.Close(); err != nil {
			return err
		}

		hashBytes := writable.sumBytes()
		hashStr := hex.EncodeToString(hashBytes)

		result := WriteSSTResult{
			SSTData: sstBuf.Bytes(),
			Meta: SSTMeta{
				ID:        buildSSTID(epoch, state.seqLo, state.seqHi, hashStr),
				Epoch:     epoch,
				SeqLo:     state.seqLo,
				SeqHi:     state.seqHi,
				MinKey:    state.minKey,
				MaxKey:    state.maxKey,
				Size:      writable.size,
				Checksum:  "sha256:" + hashStr,
				Bloom:     BloomMeta{BitsPerKey: opts.BloomBitsPerKey},
				CreatedAt: time.Now().UTC(),
				Level:     0,
			},
		}

		if opts.Signer != nil {
			sig, err := opts.Signer.SignHash(hashBytes)
			if err != nil {
				return err
			}
			result.Meta.Signature = &SSTSignature{
				Algorithm: opts.Signer.Algorithm(),
				KeyID:     opts.Signer.KeyID(),
				Hash:      hashStr,
				Signature: sig,
			}
		}

		if onSSTComplete != nil {
			if err := onSSTComplete(&result); err != nil {
				return err
			}
		}

		results = append(results, result)
		return nil
	}

	startNewSST()

	for it.Next() {
		if err := ctx.Err(); err != nil {
			if sst != nil {
				writable.Abort()
				_ = sst.Close()
			}
			return nil, err
		}

		e := it.Entry()
		k := append([]byte(nil), e.Key...)

		keyEntry, err := buildKeyEntry(e, k)
		if err != nil {
			if sst != nil {
				writable.Abort()
				_ = sst.Close()
			}
			return nil, err
		}

		encodedValue := internal.EncodeKeyEntry(keyEntry)

		if err := state.updateOrder(k, e.Seq); err != nil {
			if sst != nil {
				writable.Abort()
				_ = sst.Close()
			}
			return nil, err
		}

		kind := pebble.InternalKeyKindSet
		if e.Kind == internal.OpDelete {
			kind = pebble.InternalKeyKindDelete
		}

		ikey := pebble.MakeInternalKey(k, pebble.SeqNum(e.Seq), kind)

		if err := sst.Raw().Add(ikey, encodedValue, false); err != nil {
			writable.Abort()
			_ = sst.Close()
			return nil, err
		}

		state.updateBounds(k, e.Seq)
		currentSize = writable.size

		if currentSize >= targetSize {
			if err := finishCurrentSST(); err != nil {
				return nil, err
			}
			startNewSST()
		}
	}

	if err := it.Err(); err != nil {
		if sst != nil {
			writable.Abort()
			_ = sst.Close()
		}
		return nil, err
	}

	if state.found {
		if err := finishCurrentSST(); err != nil {
			return nil, err
		}
	}

	if len(results) == 0 {
		return nil, ErrEmptyIterator
	}

	return results, nil
}
