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

	"github.com/cockroachdb/pebble/bloom"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/segmentio/ksuid"
)

type SSTWriterOptions struct {
	BloomBitsPerKey int
	BlockSize       int
	Compression     string
	Signer          SSTHashSigner
}

// VLogRef tracks how many bytes an SSTable references in a specific VLog.
type VLogRef struct {
	VLogID    ksuid.KSUID `json:"vlog_id"`
	LiveBytes int64       `json:"live_bytes"`
}

// SSTMeta describes a SSTable
type SSTMeta struct {
	ID        string
	Epoch     uint64
	SeqLo     uint64
	SeqHi     uint64
	MinKey    []byte
	MaxKey    []byte
	Size      int64
	Checksum  string
	Signature *SSTSignature
	Bloom     BloomMeta
	CreatedAt time.Time

	Level int

	VLogID   ksuid.KSUID
	VLogSize int64

	// VLogRefs tracks which VLogs this SSTable references and how many bytes.
	VLogRefs []VLogRef
}

func (m SSTMeta) HasVLog() bool {
	return !m.VLogID.IsNil()
}

type BloomMeta struct {
	BitsPerKey int
	K          int
}

var ErrEmptyIterator = errors.New("iterator produced no entries")

var ErrOutOfOrder = errors.New("iterator out of order")

type SSTIterator interface {
	Next() bool
	Entry() MemEntry
	Err() error
	Close() error
}

type WriteSSTResult struct {
	Meta     SSTMeta
	SSTData  []byte
	VLogData []byte
	VLogID   ksuid.KSUID
}

// WriteSST builds a Key SST from memtable entries.
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
	vlog := NewVLogWriter()

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

		keyEntry, err := buildKeyEntry(e, k, vlog, state)
		if err != nil {
			return abort(err)
		}

		encodedValue := EncodeKeyEntry(keyEntry)

		if err := state.updateOrder(k, e.Seq); err != nil {
			return abort(err)
		}

		kind := sstable.InternalKeyKindSet
		if e.Kind == OpDelete {
			kind = sstable.InternalKeyKindDelete
		}

		ikey := sstable.InternalKey{
			UserKey: k,
			Trailer: (e.Seq << 8) | uint64(kind),
		}

		if err := sst.Add(ikey, encodedValue); err != nil {
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
	result.VLogData = vlog.Bytes()
	result.VLogID = vlog.ID()

	var vlogID ksuid.KSUID
	vlogRefs := state.vlogRefs()
	if len(result.VLogData) > 0 {
		vlogID = vlog.ID()
	}

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
		VLogID:    vlogID,
		VLogSize:  vlog.Size(),
		VLogRefs:  vlogRefs,
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
	minKey       []byte
	maxKey       []byte
	found        bool
	seqLo        uint64
	seqHi        uint64
	prevKey      []byte
	prevSeq      uint64
	vlogRefBytes map[ksuid.KSUID]int64
}

func newSSTBuildState() *sstBuildState {
	return &sstBuildState{
		vlogRefBytes: make(map[ksuid.KSUID]int64),
		seqLo:        ^uint64(0),
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

func (s *sstBuildState) addVLogRef(id ksuid.KSUID, length uint32) {
	s.vlogRefBytes[id] += vlogEntrySize(length)
}

func (s *sstBuildState) vlogRefs() []VLogRef {
	if len(s.vlogRefBytes) == 0 {
		return nil
	}
	refs := make([]VLogRef, 0, len(s.vlogRefBytes))
	for id, liveBytes := range s.vlogRefBytes {
		refs = append(refs, VLogRef{
			VLogID:    id,
			LiveBytes: liveBytes,
		})
	}
	return refs
}

func buildKeyEntry(e MemEntry, key []byte, vlog *VLogWriter, state *sstBuildState) (KeyEntry, error) {
	keyEntry := KeyEntry{
		Key:  key,
		Seq:  e.Seq,
		Kind: e.Kind,
	}

	if e.Kind == OpDelete {
		return keyEntry, nil
	}

	if e.Inline {
		keyEntry.Inline = true
		keyEntry.Value = e.Value
		return keyEntry, nil
	}

	if e.VLogPtr != nil {
		keyEntry.Inline = false
		keyEntry.VLogID = e.VLogPtr.VLogID
		keyEntry.VOffset = e.VLogPtr.Offset
		keyEntry.VLength = e.VLogPtr.Length
		keyEntry.VChecksum = e.VLogPtr.Checksum
		state.addVLogRef(e.VLogPtr.VLogID, e.VLogPtr.Length)
		return keyEntry, nil
	}

	ptr, err := vlog.Append(e.PendingValue)
	if err != nil {
		return KeyEntry{}, err
	}
	keyEntry.Inline = false
	keyEntry.VLogID = ptr.VLogID
	keyEntry.VOffset = ptr.Offset
	keyEntry.VLength = ptr.Length
	keyEntry.VChecksum = ptr.Checksum
	state.addVLogRef(ptr.VLogID, ptr.Length)
	return keyEntry, nil
}

func vlogEntrySize(length uint32) int64 {
	return int64(VLogEntryHeaderSize) + int64(length)
}

func buildSSTID(epoch, seqLo, seqHi uint64, hashHex string) string {
	shortHash := hashHex
	if len(shortHash) > 12 {
		shortHash = shortHash[:12]
	}
	return fmt.Sprintf("%d-%d-%d-%s.sst", epoch, seqLo, seqHi, shortHash)
}

func compressionFromString(name string) sstable.Compression {
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
