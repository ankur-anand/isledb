package isledb

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"github.com/ankur-anand/isledb/internal"
	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/bloom"
	"github.com/cockroachdb/pebble/v2/sstable"
	"golang.org/x/sync/errgroup"
)

func buildSSTIDWithTimestamp(epoch, seqLo, seqHi uint64, ts time.Time) string {
	return fmt.Sprintf("%d-%d-%d-%d.sst", epoch, seqLo, seqHi, ts.UnixNano())
}

type streamSSTResult struct {
	Meta SSTMeta
}

// writeSSTStreaming builds and uploads an SST concurrently using io.Pipe.
// The producer goroutine writes SST data to a PipeWriter, while the consumer
// goroutine reads from the PipeReader and uploads to the store.
func writeSSTStreaming(
	ctx context.Context,
	it SSTIterator,
	opts SSTWriterOptions,
	epoch uint64,
	seqLo, seqHi uint64,
	uploadFn func(ctx context.Context, sstID string, r io.Reader) error,
) (streamSSTResult, error) {
	defer it.Close()

	var result streamSSTResult

	ts := time.Now().UTC()
	sstID := buildSSTIDWithTimestamp(epoch, seqLo, seqHi, ts)

	pr, pw := io.Pipe()
	writable := newHashingWritable(pw)
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

	type producerResult struct {
		state *sstBuildState
		bloom BloomMeta
		err   error
	}
	producerDone := make(chan producerResult, 1)

	var uploadErr atomic.Value
	getUploadErr := func() error {
		if v := uploadErr.Load(); v != nil {
			return v.(error)
		}
		return nil
	}

	g, gctx := errgroup.WithContext(ctx)

	// read from the pipe and upload to the blob
	g.Go(func() error {
		err := uploadFn(gctx, sstID, pr)
		if err != nil {
			uploadErr.Store(err)
			_ = pr.CloseWithError(err)
			return fmt.Errorf("sst upload: %w", err)
		}
		return nil
	})

	g.Go(func() error {
		defer func() {
			pw.Close()
		}()

		for it.Next() {
			if err := gctx.Err(); err != nil {
				if ue := getUploadErr(); ue != nil {
					err = fmt.Errorf("sst upload: %w", ue)
				}
				writable.Abort()
				_ = sst.Close()
				producerDone <- producerResult{err: err}
				pw.CloseWithError(err)
				return err
			}

			e := it.Entry()
			k := append([]byte(nil), e.Key...)
			if opts.BloomBitsPerKey > 0 {
				hashes = append(hashes, bloomHashKey(k))
			}

			keyEntry, err := buildKeyEntry(e, k)
			if err != nil {
				writable.Abort()
				_ = sst.Close()
				producerDone <- producerResult{err: err}
				pw.CloseWithError(err)
				return fmt.Errorf("sst producer: %w", err)
			}

			encodedValue := internal.EncodeKeyEntry(keyEntry)

			if err := state.updateOrder(k, e.Seq); err != nil {
				writable.Abort()
				_ = sst.Close()
				producerDone <- producerResult{err: err}
				pw.CloseWithError(err)
				return fmt.Errorf("sst producer: %w", err)
			}

			kind := pebble.InternalKeyKindSet
			if e.Kind == internal.OpDelete {
				kind = pebble.InternalKeyKindDelete
			}

			ikey := pebble.MakeInternalKey(k, pebble.SeqNum(e.Seq), kind)

			if err := sst.Raw().Add(ikey, encodedValue, false); err != nil {
				if ue := getUploadErr(); ue != nil && errors.Is(err, io.ErrClosedPipe) {
					err = fmt.Errorf("sst upload: %w", ue)
				}
				writable.Abort()
				_ = sst.Close()
				producerDone <- producerResult{err: err}
				pw.CloseWithError(err)
				return fmt.Errorf("sst producer: %w", err)
			}

			state.updateBounds(k, e.Seq)
		}

		if err := it.Err(); err != nil {
			writable.Abort()
			_ = sst.Close()
			producerDone <- producerResult{err: err}
			pw.CloseWithError(err)
			return fmt.Errorf("sst producer: %w", err)
		}

		if !state.found {
			writable.Abort()
			_ = sst.Close()
			producerDone <- producerResult{err: ErrEmptyIterator}
			pw.CloseWithError(ErrEmptyIterator)
			return ErrEmptyIterator
		}

		if err := sst.Close(); err != nil {
			if ue := getUploadErr(); ue != nil && errors.Is(err, io.ErrClosedPipe) {
				err = fmt.Errorf("sst upload: %w", ue)
			}
			producerDone <- producerResult{err: err}
			pw.CloseWithError(err)
			return fmt.Errorf("sst producer: %w", err)
		}

		sstSize := writable.size
		var bloomBytes []byte
		var bloomK int
		if opts.BloomBitsPerKey > 0 {
			var err error
			bloomBytes, bloomK, err = buildBloomBytes(hashes, opts.BloomBitsPerKey)
			if err != nil {
				producerDone <- producerResult{err: err}
				pw.CloseWithError(err)
				return fmt.Errorf("sst producer: %w", err)
			}
			if len(bloomBytes) > 0 {
				if _, err := pw.Write(bloomBytes); err != nil {
					if ue := getUploadErr(); ue != nil && errors.Is(err, io.ErrClosedPipe) {
						err = fmt.Errorf("sst upload: %w", ue)
					}
					producerDone <- producerResult{err: err}
					pw.CloseWithError(err)
					return fmt.Errorf("sst producer: %w", err)
				}
				if err := appendBloomTrailer(pw, int64(len(bloomBytes))); err != nil {
					if ue := getUploadErr(); ue != nil && errors.Is(err, io.ErrClosedPipe) {
						err = fmt.Errorf("sst upload: %w", ue)
					}
					producerDone <- producerResult{err: err}
					pw.CloseWithError(err)
					return fmt.Errorf("sst producer: %w", err)
				}
			}
		}

		producerDone <- producerResult{
			state: state,
			bloom: BloomMeta{
				BitsPerKey: opts.BloomBitsPerKey,
				K:          bloomK,
				Offset:     sstSize,
				Length:     int64(len(bloomBytes)),
			},
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		return result, err
	}
	pResult := <-producerDone
	if pResult.err != nil {
		return result, pResult.err
	}

	hashBytes := writable.sumBytes()
	hashStr := hex.EncodeToString(hashBytes)

	result.Meta = SSTMeta{
		ID:        sstID,
		Epoch:     epoch,
		SeqLo:     pResult.state.seqLo,
		SeqHi:     pResult.state.seqHi,
		MinKey:    pResult.state.minKey,
		MaxKey:    pResult.state.maxKey,
		Size:      writable.size,
		Checksum:  "sha256:" + hashStr,
		Bloom:     pResult.bloom,
		CreatedAt: ts,
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

// writeMultipleSSTsStreaming builds and uploads multiple SSTs using streaming.
// Each SST is streamed to the upload function as it's built, with new SSTs
// started when the current one reaches targetSize.
func writeMultipleSSTsStreaming(
	ctx context.Context,
	it SSTIterator,
	opts SSTWriterOptions,
	epoch uint64,
	targetSize int64,
	uploadFn func(ctx context.Context, sstID string, r io.Reader) error,
) ([]streamSSTResult, error) {
	defer it.Close()

	var results []streamSSTResult

	wo := sstable.WriterOptions{
		BlockSize:   opts.BlockSize,
		Compression: compressionFromString(opts.Compression),
	}
	if opts.BloomBitsPerKey > 0 {
		wo.FilterPolicy = bloom.FilterPolicy(opts.BloomBitsPerKey)
	}

	var pr *io.PipeReader
	var pw *io.PipeWriter
	var writable *hashingWritable
	var sst *sstable.Writer
	var state *sstBuildState
	var hashes []uint64
	var sstID string
	var ts time.Time
	var uploadErr error
	var uploadDone chan struct{}
	var started bool
	var sstIndex int

	startNewSST := func() error {
		ts = time.Now().UTC()
		sstIndex++
		sstID = fmt.Sprintf("%d-0-0-%d-%04d.sst", epoch, ts.UnixNano(), sstIndex)

		pr, pw = io.Pipe()
		writable = newHashingWritable(pw)
		sst = sstable.NewWriter(writable, wo)
		state = newSSTBuildState()
		hashes = nil
		uploadErr = nil
		uploadDone = make(chan struct{})
		started = true

		go func(id string, reader *io.PipeReader, done chan struct{}) {
			defer close(done)
			if err := uploadFn(ctx, id, reader); err != nil {
				uploadErr = err
				reader.Close()
			}
		}(sstID, pr, uploadDone)
		return nil
	}

	finishCurrentSST := func() error {
		if !started {
			return nil
		}

		if err := sst.Close(); err != nil {
			pw.CloseWithError(err)
			<-uploadDone
			return err
		}

		sstSize := writable.size
		var bloomBytes []byte
		var bloomK int
		if opts.BloomBitsPerKey > 0 {
			var err error
			bloomBytes, bloomK, err = buildBloomBytes(hashes, opts.BloomBitsPerKey)
			if err != nil {
				pw.CloseWithError(err)
				<-uploadDone
				return err
			}
			if len(bloomBytes) > 0 {
				if _, err := pw.Write(bloomBytes); err != nil {
					pw.CloseWithError(err)
					<-uploadDone
					return err
				}
				if err := appendBloomTrailer(pw, int64(len(bloomBytes))); err != nil {
					pw.CloseWithError(err)
					<-uploadDone
					return err
				}
			}
		}

		pw.Close()

		<-uploadDone
		if uploadErr != nil {
			return fmt.Errorf("sst upload: %w", uploadErr)
		}

		hashBytes := writable.sumBytes()
		hashStr := hex.EncodeToString(hashBytes)

		result := streamSSTResult{
			Meta: SSTMeta{
				ID:       sstID,
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
				CreatedAt: ts,
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

		results = append(results, result)
		started = false
		pr, pw, writable, sst, state, uploadDone = nil, nil, nil, nil, nil, nil
		return nil
	}

	abortCurrentSST := func() {
		if !started {
			return
		}
		if writable != nil {
			writable.Abort()
		}
		if sst != nil {
			_ = sst.Close()
		}
		if pw != nil {
			pw.Close()
		}
		if uploadDone != nil {
			<-uploadDone
		}
		started = false
		pr, pw, writable, sst, state, uploadDone = nil, nil, nil, nil, nil, nil
	}

	for it.Next() {
		if err := ctx.Err(); err != nil {
			abortCurrentSST()
			return nil, err
		}

		if !started {
			if err := startNewSST(); err != nil {
				return nil, err
			}
		}

		e := it.Entry()
		k := append([]byte(nil), e.Key...)
		if opts.BloomBitsPerKey > 0 {
			hashes = append(hashes, bloomHashKey(k))
		}

		keyEntry, err := buildKeyEntry(e, k)
		if err != nil {
			abortCurrentSST()
			return nil, err
		}

		encodedValue := internal.EncodeKeyEntry(keyEntry)

		if err := state.updateOrder(k, e.Seq); err != nil {
			abortCurrentSST()
			return nil, err
		}

		kind := pebble.InternalKeyKindSet
		if e.Kind == internal.OpDelete {
			kind = pebble.InternalKeyKindDelete
		}

		ikey := pebble.MakeInternalKey(k, pebble.SeqNum(e.Seq), kind)

		if err := sst.Raw().Add(ikey, encodedValue, false); err != nil {
			abortCurrentSST()
			return nil, fmt.Errorf("sst producer: %w", err)
		}

		state.updateBounds(k, e.Seq)

		if writable.size >= targetSize {
			if err := finishCurrentSST(); err != nil {
				return nil, err
			}
		}
	}

	if err := it.Err(); err != nil {
		abortCurrentSST()
		return nil, fmt.Errorf("sst producer: %w", err)
	}

	if started && state.found {
		if err := finishCurrentSST(); err != nil {
			return nil, err
		}
	}

	if len(results) == 0 {
		return nil, ErrEmptyIterator
	}

	return results, nil
}
