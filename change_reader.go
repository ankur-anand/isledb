package isledb

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

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
	Oldest ChangeCursor
	Head   ChangeCursor
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
	batch      *changeBatch
	batchEntry uint64
	batchView  *manifest.ChangeFeedView
	batchLoad  singleflight.Group
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
		Oldest: changeCursorAt(view.RetainedFrom(), 0),
		Head:   changeCursorAt(view.Head(), 0),
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
		if next.index > uint64(len(batch.Changes)) {
			return ChangePage{}, fmt.Errorf(
				"%w: entry=%d index=%d count=%d",
				ErrInvalidChangeCursor, next.entry, next.index, len(batch.Changes))
		}
		if len(page.Changes) == 0 && cap(page.Changes) == 0 {
			remaining := len(batch.Changes) - int(next.index)
			page.Changes = make([]Change, 0, min(opts.MaxChanges, remaining))
			pageData = make([]byte, 0, changePageDataCapacity(
				batch.Changes[next.index:], opts.MaxChanges, opts.MaxBytes))
		}
		for i := next.index; i < uint64(len(batch.Changes)); i++ {
			record := batch.Changes[i]
			changeBytes := int64(len(record.Key) + len(record.Value))
			if len(page.Changes) > 0 &&
				(len(page.Changes) >= opts.MaxChanges || pageBytes+changeBytes > opts.MaxBytes) {
				page.Next = changeCursorAt(entry.Seq, i)
				return page, nil
			}
			change := publicChange(record, &pageData)
			page.Changes = append(page.Changes, change)
			pageBytes += changeBytes
			next = changeCursorAt(entry.Seq, i+1)
			page.Next = next
			if len(page.Changes) >= opts.MaxChanges {
				if i+1 == uint64(len(batch.Changes)) {
					page.Next = changeCursorAt(entry.Seq+1, 0)
				}
				return page, nil
			}
		}
		next = changeCursorAt(entry.Seq+1, 0)
		page.Next = next
	}
	return page, nil
}

func (r *ChangeReader) cachedContinuation(
	from ChangeCursor,
) (*manifest.ChangeFeedView, []*manifest.ManifestLogEntry, *changeBatch, bool) {
	if !from.set || from.index == 0 {
		return nil, nil, nil, false
	}
	r.batchMu.Lock()
	defer r.batchMu.Unlock()
	if r.batch == nil || r.batchView == nil || r.batchEntry != from.entry {
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
	batch *changeBatch,
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

func (r *ChangeReader) readBatch(ctx context.Context, meta *manifest.ChangeBatchMeta) (*changeBatch, error) {
	if meta == nil || meta.Path == "" || meta.Size < 0 || meta.RawSize < changeBatchHeaderSize {
		return nil, fmt.Errorf("%w: incomplete metadata", ErrCorruptChangeBatch)
	}
	if meta.RawSize > int64(maxMemtableArenaBytes) || meta.RawSize > int64(maxInt()) {
		return nil, fmt.Errorf(
			"%w: raw_size=%d max=%d", ErrCorruptChangeBatch, meta.RawSize, maxMemtableArenaBytes)
	}
	if meta.Compression != "" && meta.Compression != changeBatchCompressionZstd {
		return nil, fmt.Errorf(
			"%w: unsupported compression=%q", ErrCorruptChangeBatch, meta.Compression)
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

	result := r.batchLoad.DoChan(meta.Path, func() (any, error) {
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

		batch, err := r.loadBatch(ctx, meta)
		if err != nil {
			return nil, err
		}
		if r.closed.Load() {
			return nil, ErrChangeReaderClosed
		}
		r.batchMu.Lock()
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
		return loaded.Val.(*changeBatch), nil
	}
}

func (r *ChangeReader) loadBatch(ctx context.Context, meta *manifest.ChangeBatchMeta) (*changeBatch, error) {
	data, attrs, err := r.store.Read(ctx, meta.Path)
	if err != nil {
		return nil, err
	}
	if attrs.Size != meta.Size || int64(len(data)) != meta.Size {
		return nil, fmt.Errorf(
			"%w: size=%d object_size=%d metadata_size=%d",
			ErrCorruptChangeBatch, len(data), attrs.Size, meta.Size)
	}
	sum := sha256.Sum256(data)
	gotChecksum := "sha256:" + hex.EncodeToString(sum[:])
	if meta.Checksum != gotChecksum {
		return nil, fmt.Errorf(
			"%w: checksum=%q want=%q", ErrCorruptChangeBatch, gotChecksum, meta.Checksum)
	}
	batch, err := decodeChangeBatchWithRawSize(data, meta.RawSize)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCorruptChangeBatch, err)
	}
	if meta.Version != 0 && batch.Version != meta.Version {
		return nil, fmt.Errorf(
			"%w: version=%d metadata_version=%d", ErrCorruptChangeBatch, batch.Version, meta.Version)
	}
	if batch.Epoch != meta.Epoch || batch.SeqLo != meta.SeqLo || batch.SeqHi != meta.SeqHi ||
		uint64(len(batch.Changes)) != uint64(meta.Count) {
		return nil, fmt.Errorf("%w: metadata mismatch", ErrCorruptChangeBatch)
	}
	return batch, nil
}

func changePageDataCapacity(changes []changeRecord, maxChanges int, maxBytes int64) int {
	var total int64
	for i := 0; i < len(changes) && i < maxChanges; i++ {
		size := int64(len(changes[i].Key) + len(changes[i].Value))
		if i > 0 && total+size > maxBytes {
			break
		}
		total += size
	}
	if total > int64(maxInt()) {
		return maxInt()
	}
	return int(total)
}

func publicChange(change changeRecord, pageData *[]byte) Change {
	keyStart := len(*pageData)
	*pageData = append(*pageData, change.Key...)
	key := (*pageData)[keyStart:len(*pageData)]
	valueStart := len(*pageData)
	*pageData = append(*pageData, change.Value...)
	value := (*pageData)[valueStart:len(*pageData)]
	result := Change{
		Sequence: change.Seq,
		Key:      key,
		Value:    value,
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
	r.batchMu.Unlock()
	r.viewMu.Lock()
	r.view = nil
	r.viewMu.Unlock()
	return nil
}

func (r *ChangeReader) closeDB() error {
	return r.Close()
}
