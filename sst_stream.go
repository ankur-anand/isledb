package isledb

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
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
		err   error
	}
	producerDone := make(chan producerResult, 1)

	g, gctx := errgroup.WithContext(ctx)

	// read from the pipe and upload to the blob
	g.Go(func() error {
		err := uploadFn(gctx, sstID, pr)
		if err != nil {
			pr.Close()
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
				writable.Abort()
				_ = sst.Close()
				producerDone <- producerResult{err: err}
				return err
			}

			e := it.Entry()
			k := append([]byte(nil), e.Key...)

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
			producerDone <- producerResult{err: err}
			pw.CloseWithError(err)
			return fmt.Errorf("sst producer: %w", err)
		}

		producerDone <- producerResult{state: state}
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
		Bloom:     BloomMeta{BitsPerKey: opts.BloomBitsPerKey},
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
