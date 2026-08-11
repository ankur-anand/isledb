package isledb

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal/manifest"
	"golang.org/x/sync/singleflight"
)

var (
	ErrChangeReaderClosed       = errors.New("change reader closed")
	ErrInvalidChangeCursor      = errors.New("invalid change cursor")
	ErrChangeCursorExpired      = errors.New("change cursor expired")
	ErrInvalidChangeReadOptions = errors.New("invalid change read options")
	ErrCorruptChangeFeed        = errors.New("corrupt change feed")
	ErrCorruptChangeBatch       = errors.New("corrupt change batch")
)

const (
	defaultChangeReadMaxChanges = 1024
	defaultChangeReadMaxBytes   = int64(16 << 20)
	maxChangeReadChanges        = 64 * 1024
	maxChangeManifestEntries    = 1024
	changeCursorPrefix          = "cf1_"
	changeCursorPayloadSize     = 16
)

// ChangeOperation identifies the mutation represented by a Change.
type ChangeOperation uint8

const (
	ChangePut ChangeOperation = iota + 1
	ChangeDelete
)

func (op ChangeOperation) String() string {
	switch op {
	case ChangePut:
		return "put"
	case ChangeDelete:
		return "delete"
	default:
		return "unknown"
	}
}

// Change is one committed mutation in writer sequence order.
type Change struct {
	Sequence  uint64
	Operation ChangeOperation
	Key       []byte
	Value     []byte
	HasValue  bool
	ExpiresAt time.Time
}

// ChangeCursor identifies the next mutation to read. Its fields are opaque;
// persist String and restore it with ParseChangeCursor.
type ChangeCursor struct {
	entry uint64
	index uint64
	set   bool
}

// ParseChangeCursor restores a cursor returned by ChangeCursor.String.
func ParseChangeCursor(value string) (ChangeCursor, error) {
	if value == "" {
		return ChangeCursor{}, nil
	}
	if !strings.HasPrefix(value, changeCursorPrefix) {
		return ChangeCursor{}, fmt.Errorf("%w: unsupported version", ErrInvalidChangeCursor)
	}
	payload, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(value, changeCursorPrefix))
	if err != nil || len(payload) != changeCursorPayloadSize {
		return ChangeCursor{}, fmt.Errorf("%w: malformed value", ErrInvalidChangeCursor)
	}
	return changeCursorAt(
		binary.BigEndian.Uint64(payload[:8]),
		binary.BigEndian.Uint64(payload[8:]),
	), nil
}

func (c ChangeCursor) String() string {
	if !c.set {
		return ""
	}
	var payload [changeCursorPayloadSize]byte
	binary.BigEndian.PutUint64(payload[:8], c.entry)
	binary.BigEndian.PutUint64(payload[8:], c.index)
	return changeCursorPrefix + base64.RawURLEncoding.EncodeToString(payload[:])
}

func (c ChangeCursor) IsZero() bool {
	return !c.set
}

func (c ChangeCursor) MarshalText() ([]byte, error) {
	return []byte(c.String()), nil
}

func (c *ChangeCursor) UnmarshalText(text []byte) error {
	if c == nil {
		return fmt.Errorf("%w: nil destination", ErrInvalidChangeCursor)
	}
	parsed, err := ParseChangeCursor(string(text))
	if err != nil {
		return err
	}
	*c = parsed
	return nil
}

func changeCursorAt(entry, index uint64) ChangeCursor {
	return ChangeCursor{entry: entry, index: index, set: true}
}

// ChangeBounds describes the currently retained feed interval. Oldest starts
// at the first retained mutation; Head starts after the latest committed one.
type ChangeBounds struct {
	Oldest  ChangeCursor
	Head    ChangeCursor
	Payload ChangeFeedPayload
}

// ChangeReadOptions bounds one Read call. A single change larger than MaxBytes
// is returned alone so every valid cursor can make progress.
type ChangeReadOptions struct {
	MaxChanges int
	MaxBytes   int64
}

func DefaultChangeReadOptions() ChangeReadOptions {
	return ChangeReadOptions{
		MaxChanges: defaultChangeReadMaxChanges,
		MaxBytes:   defaultChangeReadMaxBytes,
	}
}

func normalizeChangeReadOptions(opts ChangeReadOptions) (ChangeReadOptions, error) {
	defaults := DefaultChangeReadOptions()
	if opts.MaxChanges < 0 {
		return ChangeReadOptions{}, fmt.Errorf(
			"%w: max_changes=%d", ErrInvalidChangeReadOptions, opts.MaxChanges)
	}
	if opts.MaxBytes < 0 {
		return ChangeReadOptions{}, fmt.Errorf(
			"%w: max_bytes=%d", ErrInvalidChangeReadOptions, opts.MaxBytes)
	}
	if opts.MaxChanges == 0 {
		opts.MaxChanges = defaults.MaxChanges
	}
	if opts.MaxChanges > maxChangeReadChanges {
		opts.MaxChanges = maxChangeReadChanges
	}
	if opts.MaxBytes == 0 {
		opts.MaxBytes = defaults.MaxBytes
	}
	return opts, nil
}

// ChangePage is one bounded page of committed mutations.
type ChangePage struct {
	Changes []Change
	Next    ChangeCursor
	Head    ChangeCursor
}

// CaughtUp reports whether Next reached the manifest head observed by Read.
func (p ChangePage) CaughtUp() bool {
	return p.Next == p.Head
}

// ChangeReader reads the durable mutation feed. It is safe for concurrent use;
// each caller owns and persists its own cursor.
type ChangeReader struct {
	store       *blobstore.Store
	manifestLog *manifest.Store
	closed      atomic.Bool
	releaseOnce sync.Once
	release     func()
	viewMu      sync.RWMutex
	view        *manifest.ChangeFeedView
	viewLoad    singleflight.Group

	batchMu    sync.Mutex
	batchPath  string
	batchMeta  manifest.ChangeBatchMeta
	batch      *changeBatchIndex
	batchEntry uint64
	batchView  *manifest.ChangeFeedView
	batchLoad  singleflight.Group

	blockCache      []cachedChangeBlock
	blockCacheBytes uint64
	blockCacheClock uint64
	blockLoad       singleflight.Group
	work            changeReaderWork
}

const maxChangeReaderBlockCacheBytes = 16 << 20

type cachedChangeBlock struct {
	path    string
	meta    manifest.ChangeBatchMeta
	ordinal int
	bytes   uint64
	lastUse uint64
	changes []changeRecord
}

type changeReaderWork struct {
	rangeGETs         atomic.Uint64
	downloadedBytes   atomic.Uint64
	decompressedBytes atomic.Uint64
}

type changeReaderWorkSnapshot struct {
	RangeGETs         uint64
	DownloadedBytes   uint64
	DecompressedBytes uint64
}

func (w *changeReaderWork) snapshot() changeReaderWorkSnapshot {
	return changeReaderWorkSnapshot{
		RangeGETs:         w.rangeGETs.Load(),
		DownloadedBytes:   w.downloadedBytes.Load(),
		DecompressedBytes: w.decompressedBytes.Load(),
	}
}

func (w *changeReaderWork) reset() {
	w.rangeGETs.Store(0)
	w.downloadedBytes.Store(0)
	w.decompressedBytes.Store(0)
}

// Bounds returns the current retained start and committed head.
func (r *ChangeReader) Bounds(ctx context.Context) (ChangeBounds, error) {
	if err := r.checkOpen(ctx); err != nil {
		return ChangeBounds{}, err
	}
	view, err := r.refreshView(ctx)
	if err != nil {
		return ChangeBounds{}, err
	}
	if !view.Enabled() {
		return ChangeBounds{}, ErrChangeFeedDisabled
	}
	return ChangeBounds{
		Oldest:  changeCursorAt(view.RetainedFrom(), 0),
		Head:    changeCursorAt(view.Head(), 0),
		Payload: publicChangeFeedPayload(view.Payload()),
	}, nil
}

// Read returns committed changes beginning at from. A zero cursor starts at
// the oldest retained change. Persist page.Next after processing the page.
func (r *ChangeReader) Read(
	ctx context.Context,
	from ChangeCursor,
	opts ChangeReadOptions,
) (ChangePage, error) {
	if err := r.checkOpen(ctx); err != nil {
		return ChangePage{}, err
	}
	opts, err := normalizeChangeReadOptions(opts)
	if err != nil {
		return ChangePage{}, err
	}

	page, err := r.readPage(ctx, from, opts)
	if !errors.Is(err, blobstore.ErrNotFound) {
		return page, err
	}

	// A missing immutable batch can mean that retention advanced after the
	// view used by the first attempt. Drop only the view association, then
	// replay the complete logical page once from the caller's original cursor.
	// The retry reloads CURRENT and therefore classifies an expired cursor
	// before touching the object again.
	r.clearBatchView()
	page, retryErr := r.readPage(ctx, from, opts)
	if !errors.Is(retryErr, blobstore.ErrNotFound) {
		return page, retryErr
	}
	return ChangePage{}, fmt.Errorf(
		"%w: retained change-batch object is missing for cursor %q after CURRENT refresh: %v",
		ErrCorruptChangeFeed, from.String(), retryErr)
}

func (r *ChangeReader) readPage(
	ctx context.Context,
	from ChangeCursor,
	opts ChangeReadOptions,
) (ChangePage, error) {
	var err error

	view, entries, continuationBatch, ok := r.cachedContinuation(from)
	if !ok {
		view, err = r.refreshView(ctx)
		if err != nil {
			return ChangePage{}, err
		}
		entries, err = r.manifestLog.ReadChangeEntriesFromView(
			ctx, view, from.entry, !from.set, maxChangeManifestEntries)
	}
	if err != nil {
		if errors.Is(err, manifest.ErrChangeFeedHistory) {
			return ChangePage{}, fmt.Errorf("%w: %w", ErrChangeCursorExpired, err)
		}
		if errors.Is(err, manifest.ErrChangeFeedPosition) {
			return ChangePage{}, fmt.Errorf("%w: %w", ErrInvalidChangeCursor, err)
		}
		return ChangePage{}, err
	}
	if !view.Enabled() {
		return ChangePage{}, ErrChangeFeedDisabled
	}
	head := view.Head()

	next := from
	if !next.set {
		if len(entries) > 0 {
			next = changeCursorAt(entries[0].Seq, 0)
		} else {
			next = changeCursorAt(head, 0)
		}
	}
	page := ChangePage{
		Changes: make([]Change, 0),
		Next:    next,
		Head:    changeCursorAt(head, 0),
	}
	if len(entries) == 0 {
		if next.entry != head || next.index != 0 {
			return ChangePage{}, fmt.Errorf(
				"%w: entry=%d index=%d head=%d", ErrInvalidChangeCursor, next.entry, next.index, head)
		}
		return page, nil
	}

	var pageBytes int64
	var pageData []byte
	for _, entry := range entries {
		if entry.Seq != next.entry {
			return ChangePage{}, fmt.Errorf(
				"%w: entry=%d next_entry=%d", ErrInvalidChangeCursor, entry.Seq, next.entry)
		}
		if entry.ChangeBatch == nil {
			if next.index != 0 {
				return ChangePage{}, fmt.Errorf(
					"%w: non-change entry=%d index=%d", ErrInvalidChangeCursor, next.entry, next.index)
			}
			next = changeCursorAt(entry.Seq+1, 0)
			page.Next = next
			continue
		}

		batch := continuationBatch
		continuationBatch = nil
		if batch == nil {
			batch, err = r.readBatch(ctx, entry.ChangeBatch)
			if err != nil {
				return ChangePage{}, err
			}
			r.rememberBatchView(entry.Seq, view, entry.ChangeBatch, batch)
		}
		if next.index > uint64(batch.Count) {
			return ChangePage{}, fmt.Errorf(
				"%w: entry=%d index=%d count=%d",
				ErrInvalidChangeCursor, next.entry, next.index, batch.Count)
		}
		if len(page.Changes) == 0 && cap(page.Changes) == 0 {
			remaining := int(batch.Count) - int(next.index)
			page.Changes = make([]Change, 0, min(opts.MaxChanges, remaining))
			pageData = make([]byte, 0, changePageDataCapacityForIndex(
				batch, next.index, opts.MaxChanges, opts.MaxBytes))
		}
		for next.index < uint64(batch.Count) {
			ordinal, blockMeta, ok := changeBatchBlockForRecord(batch, next.index)
			if !ok {
				return ChangePage{}, fmt.Errorf(
					"%w: entry=%d index=%d is not covered by a block",
					ErrCorruptChangeBatch, entry.Seq, next.index)
			}
			endOrdinal := changeBatchBlockSpan(
				batch,
				ordinal,
				next.index,
				opts.MaxChanges-len(page.Changes),
				opts.MaxBytes-pageBytes,
			)
			blocks, err := r.readBlocks(ctx, entry.ChangeBatch, batch, ordinal, endOrdinal)
			if err != nil {
				return ChangePage{}, err
			}
			for blockOffset, blockChanges := range blocks {
				currentBlock := batch.Blocks[ordinal+blockOffset]
				localStart := 0
				if blockOffset == 0 {
					localStart = int(next.index - uint64(blockMeta.FirstIndex))
				}
				for i := localStart; i < len(blockChanges); i++ {
					globalIndex := uint64(currentBlock.FirstIndex) + uint64(i)
					record := blockChanges[i]
					changeBytes := int64(len(record.Key) + len(record.Value))
					if len(page.Changes) > 0 &&
						(len(page.Changes) >= opts.MaxChanges || pageBytes+changeBytes > opts.MaxBytes) {
						page.Next = changeCursorAt(entry.Seq, globalIndex)
						return page, nil
					}
					change := publicChange(record, &pageData)
					page.Changes = append(page.Changes, change)
					pageBytes += changeBytes
					next = changeCursorAt(entry.Seq, globalIndex+1)
					page.Next = next
					if len(page.Changes) >= opts.MaxChanges || pageBytes >= opts.MaxBytes {
						if globalIndex+1 == uint64(batch.Count) {
							page.Next = changeCursorAt(entry.Seq+1, 0)
						}
						return page, nil
					}
				}
			}
		}
		next = changeCursorAt(entry.Seq+1, 0)
		page.Next = next
	}
	return page, nil
}

func (r *ChangeReader) clearBatchView() {
	r.batchMu.Lock()
	r.batchEntry = 0
	r.batchView = nil
	r.batchMu.Unlock()
}

func (r *ChangeReader) cachedContinuation(
	from ChangeCursor,
) (*manifest.ChangeFeedView, []*manifest.ManifestLogEntry, *changeBatchIndex, bool) {
	if !from.set || from.index == 0 {
		return nil, nil, nil, false
	}
	r.batchMu.Lock()
	defer r.batchMu.Unlock()
	if r.batch == nil || r.batchView == nil || r.batchEntry != from.entry {
		return nil, nil, nil, false
	}
	if r.batchView.ExpiredAt(time.Now()) {
		// The decoded index and blocks remain safe as immutable cached data, but
		// the old manifest view may no longer prove that the batch is retained.
		// Drop only that association so the caller reloads CURRENT before using
		// the cached batch again.
		r.batchEntry = 0
		r.batchView = nil
		return nil, nil, nil, false
	}
	meta := r.batchMeta
	entry := &manifest.ManifestLogEntry{Seq: from.entry, ChangeBatch: &meta}
	return r.batchView, []*manifest.ManifestLogEntry{entry}, r.batch, true
}

func (r *ChangeReader) rememberBatchView(
	entry uint64,
	view *manifest.ChangeFeedView,
	meta *manifest.ChangeBatchMeta,
	batch *changeBatchIndex,
) {
	r.batchMu.Lock()
	defer r.batchMu.Unlock()
	if r.batch == batch && meta != nil && r.batchPath == meta.Path && r.batchMeta == *meta {
		r.batchEntry = entry
		r.batchView = view
	}
}

func (r *ChangeReader) refreshView(ctx context.Context) (*manifest.ChangeFeedView, error) {
	result := r.viewLoad.DoChan("current", func() (any, error) {
		view, err := r.manifestLog.LoadChangeFeedView(ctx)
		if err != nil {
			return nil, err
		}
		if r.closed.Load() {
			return nil, ErrChangeReaderClosed
		}
		r.viewMu.Lock()
		if r.view != nil && view.Head() < r.view.Head() {
			oldHead := r.view.Head()
			r.viewMu.Unlock()
			return nil, fmt.Errorf(
				"%w: manifest head moved backward from=%d to=%d",
				ErrCorruptChangeFeed, oldHead, view.Head())
		}
		r.view = view
		r.viewMu.Unlock()
		return view, nil
	})
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case loaded := <-result:
		if loaded.Err != nil {
			return nil, loaded.Err
		}
		return loaded.Val.(*manifest.ChangeFeedView), nil
	}
}

func (r *ChangeReader) readBatch(ctx context.Context, meta *manifest.ChangeBatchMeta) (*changeBatchIndex, error) {
	if err := validateChangeBatchMeta(meta); err != nil {
		return nil, err
	}
	r.batchMu.Lock()
	if r.batchPath == meta.Path && r.batch != nil {
		if r.batchMeta != *meta {
			r.batchMu.Unlock()
			return nil, fmt.Errorf("%w: metadata changed for path=%q", ErrCorruptChangeBatch, meta.Path)
		}
		batch := r.batch
		r.batchMu.Unlock()
		return batch, nil
	}
	r.batchMu.Unlock()

	loadKey := fmt.Sprintf("%s#index#%d#%d", meta.Path, meta.Size, meta.BlockCount)
	result := r.batchLoad.DoChan(loadKey, func() (any, error) {
		r.batchMu.Lock()
		if r.batchPath == meta.Path && r.batch != nil {
			if r.batchMeta != *meta {
				r.batchMu.Unlock()
				return nil, fmt.Errorf("%w: metadata changed for path=%q", ErrCorruptChangeBatch, meta.Path)
			}
			batch := r.batch
			r.batchMu.Unlock()
			return batch, nil
		}
		r.batchMu.Unlock()

		batch, err := r.loadBatchIndex(ctx, meta)
		if err != nil {
			return nil, err
		}
		if r.closed.Load() {
			return nil, ErrChangeReaderClosed
		}
		r.batchMu.Lock()
		if r.closed.Load() {
			r.batchMu.Unlock()
			return nil, ErrChangeReaderClosed
		}
		r.batchPath = meta.Path
		r.batchMeta = *meta
		r.batch = batch
		r.batchEntry = 0
		r.batchView = nil
		r.batchMu.Unlock()
		return batch, nil
	})
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case loaded := <-result:
		if loaded.Err != nil {
			return nil, loaded.Err
		}
		return loaded.Val.(*changeBatchIndex), nil
	}
}

func validateChangeBatchMeta(meta *manifest.ChangeBatchMeta) error {
	if meta == nil || meta.Path == "" || meta.Size <= 0 || meta.RawSize <= 0 || meta.IndexChecksum == "" ||
		meta.Count == 0 || meta.BlockCount == 0 || meta.BlockCount > meta.Count {
		return fmt.Errorf("%w: incomplete metadata", ErrCorruptChangeBatch)
	}
	if meta.Version != changeBatchVersion {
		return fmt.Errorf(
			"%w: version=%d want=%d", ErrCorruptChangeBatch, meta.Version, changeBatchVersion)
	}
	if meta.RawSize > int64(maxMemtableArenaBytes) || meta.RawSize > int64(maxInt()) {
		return fmt.Errorf(
			"%w: raw_size=%d max=%d", ErrCorruptChangeBatch, meta.RawSize, maxMemtableArenaBytes)
	}
	if meta.Compression != changeBatchCompressionZstd {
		return fmt.Errorf(
			"%w: unsupported compression=%q", ErrCorruptChangeBatch, meta.Compression)
	}
	if !meta.Payload.Valid() {
		return fmt.Errorf("%w: unsupported payload=%q", ErrCorruptChangeBatch, meta.Payload)
	}
	suffixSize := int64(meta.BlockCount)*changeBatchIndexEntrySize + changeBatchTrailerSize
	if suffixSize >= meta.Size {
		return fmt.Errorf(
			"%w: index_and_trailer_size=%d object_size=%d", ErrCorruptChangeBatch, suffixSize, meta.Size)
	}
	return nil
}

func (r *ChangeReader) loadBatchIndex(ctx context.Context, meta *manifest.ChangeBatchMeta) (*changeBatchIndex, error) {
	suffixSize := int64(meta.BlockCount)*changeBatchIndexEntrySize + changeBatchTrailerSize
	data, err := r.store.ReadRange(ctx, meta.Path, meta.Size-suffixSize, suffixSize)
	if err != nil {
		return nil, err
	}
	r.work.rangeGETs.Add(1)
	r.work.downloadedBytes.Add(uint64(len(data)))
	if int64(len(data)) != suffixSize {
		return nil, fmt.Errorf(
			"%w: index_and_trailer_bytes=%d want=%d", ErrCorruptChangeBatch, len(data), suffixSize)
	}
	indexData := data[:len(data)-changeBatchTrailerSize]
	trailer := data[len(data)-changeBatchTrailerSize:]
	if got := fmt.Sprintf("sha256:%x", trailer[64:96]); got != meta.IndexChecksum {
		return nil, fmt.Errorf(
			"%w: index checksum=%q want=%q", ErrCorruptChangeBatch, got, meta.IndexChecksum)
	}
	index, err := decodeChangeBatchIndex(indexData, trailer, meta.Size)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCorruptChangeBatch, err)
	}
	if index.Version != meta.Version || manifestChangeFeedPayload(index.Payload) != meta.Payload ||
		index.Epoch != meta.Epoch || index.SeqLo != meta.SeqLo ||
		index.SeqHi != meta.SeqHi || index.Count != meta.Count || len(index.Blocks) != int(meta.BlockCount) ||
		index.RawSize != uint64(meta.RawSize) {
		return nil, fmt.Errorf("%w: metadata mismatch", ErrCorruptChangeBatch)
	}
	return index, nil
}

func changeBatchBlockForRecord(index *changeBatchIndex, record uint64) (int, changeBatchBlock, bool) {
	if index == nil || record >= uint64(index.Count) {
		return 0, changeBatchBlock{}, false
	}
	ordinal := sort.Search(len(index.Blocks), func(i int) bool {
		block := index.Blocks[i]
		return uint64(block.FirstIndex)+uint64(block.Count) > record
	})
	if ordinal == len(index.Blocks) {
		return 0, changeBatchBlock{}, false
	}
	block := index.Blocks[ordinal]
	if record < uint64(block.FirstIndex) {
		return 0, changeBatchBlock{}, false
	}
	return ordinal, block, true
}

func changeBatchBlockSpan(
	index *changeBatchIndex,
	startOrdinal int,
	startRecord uint64,
	maxChanges int,
	maxBytes int64,
) int {
	if index == nil || startOrdinal < 0 || startOrdinal >= len(index.Blocks) || maxChanges <= 0 {
		return startOrdinal
	}
	startBlock := index.Blocks[startOrdinal]
	changes := uint64(startBlock.FirstIndex) + uint64(startBlock.Count) - startRecord
	rawBytes := uint64(startBlock.RawSize)
	rawBudget := uint64(0)
	if maxBytes > 0 {
		rawBudget = uint64(maxBytes) + uint64(maxChanges)*changeRecordHeaderSize
	}
	end := startOrdinal + 1
	for end < len(index.Blocks) && changes < uint64(maxChanges) {
		block := index.Blocks[end]
		if rawBudget > 0 && rawBytes+uint64(block.RawSize) > rawBudget {
			break
		}
		rawBytes += uint64(block.RawSize)
		changes += uint64(block.Count)
		end++
	}
	return end
}

func (r *ChangeReader) readBlocks(
	ctx context.Context,
	meta *manifest.ChangeBatchMeta,
	index *changeBatchIndex,
	startOrdinal int,
	endOrdinal int,
) ([][]changeRecord, error) {
	if startOrdinal < 0 || endOrdinal <= startOrdinal || endOrdinal > len(index.Blocks) {
		return nil, fmt.Errorf(
			"%w: invalid block span [%d,%d)", ErrCorruptChangeBatch, startOrdinal, endOrdinal)
	}
	if endOrdinal == startOrdinal+1 {
		changes, err := r.readBlock(ctx, meta, index, startOrdinal)
		if err != nil {
			return nil, err
		}
		return [][]changeRecord{changes}, nil
	}
	result := make([][]changeRecord, endOrdinal-startOrdinal)
	for ordinal := startOrdinal; ordinal < endOrdinal; {
		changes, err := r.cachedBlock(meta, ordinal)
		if err != nil {
			return nil, err
		}
		if changes != nil {
			result[ordinal-startOrdinal] = changes
			ordinal++
			continue
		}

		missingStart := ordinal
		ordinal++
		for ordinal < endOrdinal {
			changes, err := r.cachedBlock(meta, ordinal)
			if err != nil {
				return nil, err
			}
			if changes != nil {
				break
			}
			ordinal++
		}
		loaded, err := r.loadBlockSpan(ctx, meta, index, missingStart, ordinal)
		if err != nil {
			return nil, err
		}
		copy(result[missingStart-startOrdinal:ordinal-startOrdinal], loaded)
	}
	return result, nil
}

func (r *ChangeReader) loadBlockSpan(
	ctx context.Context,
	meta *manifest.ChangeBatchMeta,
	index *changeBatchIndex,
	startOrdinal int,
	endOrdinal int,
) ([][]changeRecord, error) {
	first := index.Blocks[startOrdinal]
	last := index.Blocks[endOrdinal-1]
	endOffset := last.Offset + uint64(last.CompressedSize)
	if endOffset < first.Offset || endOffset-first.Offset > uint64(maxInt()) {
		return nil, fmt.Errorf("%w: block span too large", ErrCorruptChangeBatch)
	}
	data, err := r.store.ReadRange(ctx, meta.Path, int64(first.Offset), int64(endOffset-first.Offset))
	if err != nil {
		return nil, err
	}
	r.work.rangeGETs.Add(1)
	r.work.downloadedBytes.Add(uint64(len(data)))
	if uint64(len(data)) != endOffset-first.Offset {
		return nil, fmt.Errorf("%w: short block span", ErrCorruptChangeBatch)
	}

	result := make([][]changeRecord, 0, endOrdinal-startOrdinal)
	for ordinal := startOrdinal; ordinal < endOrdinal; ordinal++ {
		block := index.Blocks[ordinal]
		lo := block.Offset - first.Offset
		hi := lo + uint64(block.CompressedSize)
		changes, err := decodeChangeBatchBlock(data[lo:hi], block, index.Payload)
		if err != nil {
			return nil, fmt.Errorf("%w: block=%d: %v", ErrCorruptChangeBatch, ordinal, err)
		}
		if err := validateDecodedChangeBlock(index, ordinal, changes); err != nil {
			return nil, err
		}
		r.work.decompressedBytes.Add(uint64(block.RawSize))
		result = append(result, changes)
		r.cacheBlock(meta, ordinal, uint64(block.RawSize), changes)
	}
	if r.closed.Load() {
		return nil, ErrChangeReaderClosed
	}
	return result, nil
}

func (r *ChangeReader) readBlock(
	ctx context.Context,
	meta *manifest.ChangeBatchMeta,
	index *changeBatchIndex,
	ordinal int,
) ([]changeRecord, error) {
	if ordinal < 0 || ordinal >= len(index.Blocks) {
		return nil, fmt.Errorf("%w: block=%d", ErrCorruptChangeBatch, ordinal)
	}

	changes, err := r.cachedBlock(meta, ordinal)
	if err != nil {
		return nil, err
	}
	if changes != nil {
		return changes, nil
	}

	loadKey := fmt.Sprintf("%s#block#%d#%d", meta.Path, ordinal, meta.Size)
	result := r.blockLoad.DoChan(loadKey, func() (any, error) {
		changes, err := r.cachedBlock(meta, ordinal)
		if err != nil {
			return nil, err
		}
		if changes != nil {
			return changes, nil
		}

		changes, err = r.loadBlock(ctx, meta, index, ordinal)
		if err != nil {
			return nil, err
		}
		if r.closed.Load() {
			return nil, ErrChangeReaderClosed
		}
		r.cacheBlock(meta, ordinal, uint64(index.Blocks[ordinal].RawSize), changes)
		return changes, nil
	})
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case loaded := <-result:
		if loaded.Err != nil {
			return nil, loaded.Err
		}
		return loaded.Val.([]changeRecord), nil
	}
}

func (r *ChangeReader) cachedBlock(meta *manifest.ChangeBatchMeta, ordinal int) ([]changeRecord, error) {
	r.batchMu.Lock()
	defer r.batchMu.Unlock()
	if r.closed.Load() {
		return nil, ErrChangeReaderClosed
	}
	for i := range r.blockCache {
		cached := &r.blockCache[i]
		if cached.path != meta.Path || cached.ordinal != ordinal {
			continue
		}
		if cached.meta != *meta {
			return nil, fmt.Errorf("%w: metadata changed for path=%q", ErrCorruptChangeBatch, meta.Path)
		}
		r.blockCacheClock++
		cached.lastUse = r.blockCacheClock
		return cached.changes, nil
	}
	return nil, nil
}

func (r *ChangeReader) cacheBlock(
	meta *manifest.ChangeBatchMeta,
	ordinal int,
	rawBytes uint64,
	changes []changeRecord,
) {
	cacheBytes := rawBytes + uint64(unsafe.Sizeof(cachedChangeBlock{})) +
		uint64(len(changes))*uint64(unsafe.Sizeof(changeRecord{}))
	if cacheBytes > maxChangeReaderBlockCacheBytes || len(changes) == 0 {
		return
	}
	r.batchMu.Lock()
	defer r.batchMu.Unlock()
	if r.closed.Load() {
		return
	}
	for i := range r.blockCache {
		cached := &r.blockCache[i]
		if cached.path == meta.Path && cached.ordinal == ordinal {
			return
		}
	}
	for len(r.blockCache) > 0 && r.blockCacheBytes+cacheBytes > maxChangeReaderBlockCacheBytes {
		oldest := 0
		for i := 1; i < len(r.blockCache); i++ {
			if r.blockCache[i].lastUse < r.blockCache[oldest].lastUse {
				oldest = i
			}
		}
		r.blockCacheBytes -= r.blockCache[oldest].bytes
		copy(r.blockCache[oldest:], r.blockCache[oldest+1:])
		r.blockCache = r.blockCache[:len(r.blockCache)-1]
	}
	r.blockCacheClock++
	r.blockCache = append(r.blockCache, cachedChangeBlock{
		path: meta.Path, meta: *meta, ordinal: ordinal, bytes: cacheBytes,
		lastUse: r.blockCacheClock, changes: changes,
	})
	r.blockCacheBytes += cacheBytes
}

func (r *ChangeReader) loadBlock(
	ctx context.Context,
	meta *manifest.ChangeBatchMeta,
	index *changeBatchIndex,
	ordinal int,
) ([]changeRecord, error) {
	block := index.Blocks[ordinal]
	data, err := r.store.ReadRange(ctx, meta.Path, int64(block.Offset), int64(block.CompressedSize))
	if err != nil {
		return nil, err
	}
	r.work.rangeGETs.Add(1)
	r.work.downloadedBytes.Add(uint64(len(data)))
	changes, err := decodeChangeBatchBlock(data, block, index.Payload)
	if err != nil {
		return nil, fmt.Errorf("%w: block=%d: %v", ErrCorruptChangeBatch, ordinal, err)
	}
	r.work.decompressedBytes.Add(uint64(block.RawSize))
	if err := validateDecodedChangeBlock(index, ordinal, changes); err != nil {
		return nil, err
	}
	return changes, nil
}

func validateDecodedChangeBlock(index *changeBatchIndex, ordinal int, changes []changeRecord) error {
	if len(changes) == 0 {
		return fmt.Errorf("%w: empty decoded block=%d", ErrCorruptChangeBatch, ordinal)
	}
	lastSeq := changes[len(changes)-1].Seq
	if ordinal+1 < len(index.Blocks) {
		if lastSeq >= index.Blocks[ordinal+1].SeqLo {
			return fmt.Errorf("%w: overlapping block sequences", ErrCorruptChangeBatch)
		}
	} else if lastSeq != index.SeqHi {
		return fmt.Errorf(
			"%w: final sequence=%d want=%d", ErrCorruptChangeBatch, lastSeq, index.SeqHi)
	}
	return nil
}

func changePageDataCapacityForIndex(
	index *changeBatchIndex,
	start uint64,
	maxChanges int,
	maxBytes int64,
) int {
	if index == nil || start >= uint64(index.Count) || maxChanges <= 0 || maxBytes <= 0 {
		return 0
	}
	remaining := min(uint64(maxChanges), uint64(index.Count)-start)
	headerBytes := uint64(index.Count) * changeRecordHeaderSize
	payloadBytes := uint64(0)
	if index.RawSize > headerBytes {
		payloadBytes = index.RawSize - headerBytes
	}
	estimate := uint64(0)
	if payloadBytes > 0 {
		estimate = (payloadBytes*remaining + uint64(index.Count) - 1) / uint64(index.Count)
	}
	if estimate > uint64(maxBytes) {
		estimate = uint64(maxBytes)
	}
	if estimate > uint64(maxInt()) {
		return maxInt()
	}
	return int(estimate)
}

func publicChange(change changeRecord, pageData *[]byte) Change {
	keyStart := len(*pageData)
	*pageData = append(*pageData, change.Key...)
	key := (*pageData)[keyStart:len(*pageData)]
	var value []byte
	hasValue := change.Kind == changePut && !change.ValueOmitted
	if hasValue {
		valueStart := len(*pageData)
		*pageData = append(*pageData, change.Value...)
		value = (*pageData)[valueStart:len(*pageData)]
		if value == nil {
			value = []byte{}
		}
	}
	result := Change{
		Sequence: change.Seq,
		Key:      key,
		Value:    value,
		HasValue: hasValue,
	}
	switch change.Kind {
	case changePut:
		result.Operation = ChangePut
	case changeDelete:
		result.Operation = ChangeDelete
	}
	if change.ExpireAt != 0 {
		result.ExpiresAt = time.UnixMilli(change.ExpireAt)
	}
	return result
}

func (r *ChangeReader) checkOpen(ctx context.Context) error {
	if r == nil || r.closed.Load() {
		return ErrChangeReaderClosed
	}
	return checkContext(ctx)
}

// Close releases this reader. It does not affect other change or KV readers.
func (r *ChangeReader) Close() error {
	if r == nil || !r.closed.CompareAndSwap(false, true) {
		return nil
	}
	if r.release != nil {
		r.releaseOnce.Do(r.release)
	}
	r.batchMu.Lock()
	r.batchPath = ""
	r.batchMeta = manifest.ChangeBatchMeta{}
	r.batch = nil
	r.batchEntry = 0
	r.batchView = nil
	clear(r.blockCache)
	r.blockCache = nil
	r.blockCacheBytes = 0
	r.blockCacheClock = 0
	r.batchMu.Unlock()
	r.viewMu.Lock()
	r.view = nil
	r.viewMu.Unlock()
	return nil
}

func (r *ChangeReader) closeDB() error {
	return r.Close()
}
