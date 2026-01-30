package isledb

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/config"
	"github.com/ankur-anand/isledb/internal"
	"github.com/cockroachdb/pebble/v2/sstable"
)

const (
	DefaultWarmCacheTimeout = 30 * time.Second

	DefaultSyncInterval = 5 * time.Second

	DefaultPrefetchConcurrency = 10
)

type DBOptions struct {
	MemtableSize    int64
	FlushInterval   time.Duration
	BloomBitsPerKey int
	BlockSize       int
	Compression     string

	ValueStorage config.ValueStorageConfig

	AppendOnlyMode bool

	RetentionCompactorMode     RetentionCompactorMode
	AppendOnlyRetentionPeriod  time.Duration
	AppendOnlyRetentionCount   int
	RetentionCompactorInterval time.Duration
	AppendOnlySegmentDuration  time.Duration

	EnableCompaction        bool
	L0CompactionThreshold   int
	MinSources              int
	MaxSources              int
	SizeThreshold           int
	CompactionCheckInterval time.Duration

	SSTCacheSize       int64
	SSTReaderCacheSize int
	BlobCacheSize      int64
	BlobCacheItemSize  int64

	WarmCacheOnOpen     bool
	WarmCacheTimeout    time.Duration
	PrefetchConcurrency int

	BackgroundSync bool
	SyncInterval   time.Duration

	OnFlushError        func(error)
	OnCompactionStart   func(CompactionJob)
	OnCompactionEnd     func(CompactionJob, error)
	OnCacheWarmDone     func(warmed int, duration time.Duration)
	OnAppendOnlyCleanup func(CleanupStats)
}

func DefaultDBOptions() DBOptions {
	return DBOptions{

		MemtableSize:    4 * 1024 * 1024,
		FlushInterval:   time.Second,
		BloomBitsPerKey: 10,
		BlockSize:       4096,
		Compression:     "snappy",
		ValueStorage:    config.DefaultValueStorageConfig(),

		AppendOnlyMode:             false,
		RetentionCompactorMode:     CompactByAge,
		AppendOnlyRetentionPeriod:  7 * 24 * time.Hour,
		AppendOnlyRetentionCount:   10,
		RetentionCompactorInterval: time.Minute,
		AppendOnlySegmentDuration:  time.Hour,

		EnableCompaction:        true,
		L0CompactionThreshold:   8,
		MinSources:              4,
		MaxSources:              8,
		SizeThreshold:           4,
		CompactionCheckInterval: 5 * time.Second,

		SSTCacheSize:       DefaultSSTCacheSize,
		SSTReaderCacheSize: DefaultSSTReaderCacheSize,
		BlobCacheSize:      internal.DefaultBlobCacheSize,
		BlobCacheItemSize:  internal.DefaultBlobCacheMaxItemSize,

		WarmCacheOnOpen:     false,
		WarmCacheTimeout:    DefaultWarmCacheTimeout,
		PrefetchConcurrency: DefaultPrefetchConcurrency,

		BackgroundSync: false,
		SyncInterval:   DefaultSyncInterval,
	}
}

func DefaultAppendOnlyOptions() DBOptions {
	opts := DefaultDBOptions()

	opts.AppendOnlyMode = true
	opts.EnableCompaction = false

	opts.RetentionCompactorMode = CompactByAge
	opts.AppendOnlyRetentionPeriod = 7 * 24 * time.Hour
	opts.AppendOnlyRetentionCount = 10
	opts.RetentionCompactorInterval = time.Minute

	opts.BackgroundSync = true
	opts.SyncInterval = time.Second

	return opts
}

func InProcessDBOptions() DBOptions {
	opts := DefaultDBOptions()
	opts.WarmCacheOnOpen = true
	opts.BackgroundSync = true
	opts.SSTCacheSize = 256 * 1024 * 1024
	opts.SSTReaderCacheSize = 100
	return opts
}

type DB struct {
	store *blobstore.Store
	opts  DBOptions

	writer             *writer
	compactor          *Compactor
	retentionCompactor *RetentionCompactor

	mu     sync.RWMutex
	reader *Reader

	syncCancel context.CancelFunc
	syncWg     sync.WaitGroup

	closed atomic.Bool
}

func Open(ctx context.Context, store *blobstore.Store, opts DBOptions) (*DB, error) {

	defaults := DefaultDBOptions()
	if opts.MemtableSize == 0 {
		opts.MemtableSize = defaults.MemtableSize
	}
	if opts.FlushInterval == 0 {
		opts.FlushInterval = defaults.FlushInterval
	}
	if opts.ValueStorage.BlobThreshold == 0 {
		opts.ValueStorage = defaults.ValueStorage
	}
	if opts.BloomBitsPerKey == 0 {
		opts.BloomBitsPerKey = defaults.BloomBitsPerKey
	}
	if opts.BlockSize == 0 {
		opts.BlockSize = defaults.BlockSize
	}
	if opts.Compression == "" {
		opts.Compression = defaults.Compression
	}
	if opts.L0CompactionThreshold == 0 {
		opts.L0CompactionThreshold = defaults.L0CompactionThreshold
	}
	if opts.MinSources == 0 {
		opts.MinSources = defaults.MinSources
	}
	if opts.MaxSources == 0 {
		opts.MaxSources = defaults.MaxSources
	}
	if opts.SizeThreshold == 0 {
		opts.SizeThreshold = defaults.SizeThreshold
	}
	if opts.CompactionCheckInterval == 0 {
		opts.CompactionCheckInterval = defaults.CompactionCheckInterval
	}
	if opts.SSTCacheSize == 0 {
		opts.SSTCacheSize = defaults.SSTCacheSize
	}
	if opts.SSTReaderCacheSize == 0 {
		opts.SSTReaderCacheSize = defaults.SSTReaderCacheSize
	}
	if opts.BlobCacheSize == 0 {
		opts.BlobCacheSize = defaults.BlobCacheSize
	}
	if opts.BlobCacheItemSize == 0 {
		opts.BlobCacheItemSize = defaults.BlobCacheItemSize
	}
	if opts.WarmCacheTimeout == 0 {
		opts.WarmCacheTimeout = defaults.WarmCacheTimeout
	}
	if opts.PrefetchConcurrency == 0 {
		opts.PrefetchConcurrency = defaults.PrefetchConcurrency
	}
	if opts.SyncInterval == 0 {
		opts.SyncInterval = defaults.SyncInterval
	}

	writerOpts := WriterOptions{
		MemtableSize:    opts.MemtableSize,
		FlushInterval:   opts.FlushInterval,
		BloomBitsPerKey: opts.BloomBitsPerKey,
		BlockSize:       opts.BlockSize,
		Compression:     opts.Compression,
		ValueStorage:    opts.ValueStorage,
		OnFlushError:    opts.OnFlushError,
	}

	writer, err := newWriter(ctx, store, writerOpts)
	if err != nil {
		return nil, err
	}

	readerOpts := ReaderOptions{
		SSTCacheSize:       opts.SSTCacheSize,
		SSTReaderCacheSize: opts.SSTReaderCacheSize,
		BlobCacheSize:      opts.BlobCacheSize,
		BlobCacheItemSize:  opts.BlobCacheItemSize,
		ValueStorageConfig: opts.ValueStorage,
	}

	reader, err := NewReader(ctx, store, readerOpts)
	if err != nil {
		writer.close()
		return nil, err
	}

	db := &DB{
		store:  store,
		opts:   opts,
		writer: writer,
		reader: reader,
	}

	if opts.AppendOnlyMode {

		appendOnlyCleanerOpts := RetentionCompactorOptions{
			Mode:            opts.RetentionCompactorMode,
			RetentionPeriod: opts.AppendOnlyRetentionPeriod,
			RetentionCount:  opts.AppendOnlyRetentionCount,
			CheckInterval:   opts.RetentionCompactorInterval,
			SegmentDuration: opts.AppendOnlySegmentDuration,
			OnCleanup:       opts.OnAppendOnlyCleanup,
		}

		if appendOnlyCleanerOpts.RetentionPeriod == 0 {
			appendOnlyCleanerOpts.RetentionPeriod = 7 * 24 * time.Hour
		}
		if appendOnlyCleanerOpts.RetentionCount == 0 {
			appendOnlyCleanerOpts.RetentionCount = 10
		}
		if appendOnlyCleanerOpts.CheckInterval == 0 {
			appendOnlyCleanerOpts.CheckInterval = time.Minute
		}
		if appendOnlyCleanerOpts.SegmentDuration == 0 {
			appendOnlyCleanerOpts.SegmentDuration = time.Hour
		}

		appendOnlyCleaner, err := NewRetentionCompactor(ctx, store, appendOnlyCleanerOpts)
		if err != nil {
			_ = reader.Close()
			_ = writer.close()
			return nil, err
		}
		db.retentionCompactor = appendOnlyCleaner
		appendOnlyCleaner.Start()

	} else if opts.EnableCompaction {

		compactorOpts := CompactorOptions{
			L0CompactionThreshold: opts.L0CompactionThreshold,
			MinSources:            opts.MinSources,
			MaxSources:            opts.MaxSources,
			SizeThreshold:         opts.SizeThreshold,
			BloomBitsPerKey:       opts.BloomBitsPerKey,
			BlockSize:             opts.BlockSize,
			Compression:           opts.Compression,
			CheckInterval:         opts.CompactionCheckInterval,
			OnCompactionStart:     opts.OnCompactionStart,
			OnCompactionEnd:       opts.OnCompactionEnd,
		}

		compactor, err := NewCompactor(ctx, store, compactorOpts)
		if err != nil {
			_ = reader.Close()
			_ = writer.close()
			return nil, err
		}
		db.compactor = compactor
		compactor.Start()
	}

	if opts.WarmCacheOnOpen {
		start := time.Now()
		warmed, _ := db.warmCache(ctx)

		if opts.OnCacheWarmDone != nil {
			opts.OnCacheWarmDone(warmed, time.Since(start))
		}
	}

	if opts.BackgroundSync {
		syncCtx, cancel := context.WithCancel(context.Background())
		db.syncCancel = cancel
		db.syncWg.Add(1)
		go db.backgroundSyncLoop(syncCtx)
	}

	return db, nil
}

func (db *DB) Put(key, value []byte) error {
	if db.closed.Load() {
		return errors.New("db closed")
	}
	return db.writer.put(key, value)
}

func (db *DB) Get(ctx context.Context, key []byte) ([]byte, bool, error) {
	if db.closed.Load() {
		return nil, false, errors.New("db closed")
	}
	db.mu.RLock()
	reader := db.reader
	db.mu.RUnlock()
	return reader.Get(ctx, key)
}

func (db *DB) Delete(key []byte) error {
	if db.closed.Load() {
		return errors.New("db closed")
	}
	return db.writer.delete(key)
}

func (db *DB) Scan(ctx context.Context, start, end []byte) ([]KV, error) {
	if db.closed.Load() {
		return nil, errors.New("db closed")
	}
	db.mu.RLock()
	reader := db.reader
	db.mu.RUnlock()
	return reader.Scan(ctx, start, end)
}

func (db *DB) Flush(ctx context.Context) error {
	if db.closed.Load() {
		return errors.New("db closed")
	}
	return db.writer.flush(ctx)
}

func (db *DB) Compact(ctx context.Context) error {
	if db.closed.Load() {
		return errors.New("db closed")
	}
	if db.compactor == nil {
		return errors.New("compaction not enabled")
	}
	return db.compactor.RunCompaction(ctx)
}

func (db *DB) Refresh(ctx context.Context) error {
	if db.closed.Load() {
		return errors.New("db closed")
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	return db.refreshLocked(ctx)
}

func (db *DB) refreshLocked(ctx context.Context) error {
	return db.reader.Refresh(ctx)
}

func (db *DB) warmCache(ctx context.Context) (int, error) {
	timeout := db.opts.WarmCacheTimeout
	if timeout == 0 {
		timeout = DefaultWarmCacheTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	db.mu.RLock()
	reader := db.reader
	db.mu.RUnlock()

	m := reader.ManifestUnsafe()
	if m == nil {
		return 0, nil
	}

	sstIDs := m.AllSSTIDs()
	if len(sstIDs) == 0 {
		return 0, nil
	}

	concurrency := db.opts.PrefetchConcurrency
	if concurrency == 0 {
		concurrency = DefaultPrefetchConcurrency
	}
	sem := make(chan struct{}, concurrency)

	var wg sync.WaitGroup
	var warmed atomic.Int32
	var firstErr error
	var errOnce sync.Once

	for _, sstID := range sstIDs {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()

			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				return
			}

			path := db.store.SSTPath(id)
			if err := db.prefetchSST(ctx, path); err != nil {
				errOnce.Do(func() { firstErr = err })
				return
			}
			warmed.Add(1)
		}(sstID)
	}

	wg.Wait()
	return int(warmed.Load()), firstErr
}

func (db *DB) prefetchSST(ctx context.Context, path string) error {
	db.mu.RLock()
	reader := db.reader
	db.mu.RUnlock()

	if _, ok := reader.sstReaderCache.Get(path); ok {
		return nil
	}

	data, _, err := db.store.Read(ctx, path)
	if err != nil {
		return err
	}

	reader.sstCache.Set(path, data)

	sstReader, err := newSSTReader(ctx, data)
	if err != nil {
		return err
	}
	reader.sstReaderCache.Set(path, sstReader, data)

	return nil
}

func (db *DB) backgroundSyncLoop(ctx context.Context) {
	defer db.syncWg.Done()

	interval := db.opts.SyncInterval
	if interval == 0 {
		interval = DefaultSyncInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			db.syncAndPrefetch(ctx)
		}
	}
}

func (db *DB) syncAndPrefetch(ctx context.Context) {

	db.mu.RLock()
	reader := db.reader
	oldManifest := reader.ManifestUnsafe()
	var oldIDs map[string]struct{}
	if oldManifest != nil {
		oldIDs = make(map[string]struct{})
		for _, id := range oldManifest.AllSSTIDs() {
			oldIDs[id] = struct{}{}
		}
	}
	db.mu.RUnlock()

	if err := db.Refresh(ctx); err != nil {
		return
	}

	db.mu.RLock()
	newManifest := db.reader.ManifestUnsafe()
	db.mu.RUnlock()

	if newManifest == nil {
		return
	}

	newSSTs := newManifest.AllSSTIDs()
	for _, id := range newSSTs {
		if oldIDs != nil {
			if _, existed := oldIDs[id]; existed {
				continue
			}
		}

		go func(sstID string) {
			path := db.store.SSTPath(sstID)
			_ = db.prefetchSST(ctx, path)
		}(id)
	}
}

func newSSTReader(ctx context.Context, data []byte) (*sstable.Reader, error) {
	return sstable.NewReader(ctx, newSSTReadable(data), sstable.ReaderOptions{})
}

func (db *DB) Close() error {

	if !db.closed.CompareAndSwap(false, true) {
		return nil
	}

	var firstErr error

	if db.syncCancel != nil {
		db.syncCancel()
		db.syncWg.Wait()
	}

	if db.compactor != nil {
		if err := db.compactor.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if db.retentionCompactor != nil {
		if err := db.retentionCompactor.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	if err := db.writer.close(); err != nil && firstErr == nil {
		firstErr = err
	}

	db.mu.Lock()
	if db.reader != nil {
		if err := db.reader.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	db.mu.Unlock()

	return firstErr
}

func (db *DB) Stats(ctx context.Context) (*DBStats, error) {
	if db.closed.Load() {
		return nil, errors.New("db closed")
	}

	db.mu.RLock()
	reader := db.reader
	db.mu.RUnlock()

	m := reader.Manifest()
	if m == nil {
		return &DBStats{}, nil
	}

	var totalSize int64
	for _, sst := range m.L0SSTs {
		totalSize += sst.Size
	}
	for _, sr := range m.SortedRuns {
		for _, sst := range sr.SSTs {
			totalSize += sst.Size
		}
	}

	return &DBStats{
		L0SSTCount:     m.L0SSTCount(),
		SortedRunCount: m.SortedRunCount(),
		TotalSSTSize:   totalSize,
	}, nil
}

type DBStats struct {
	L0SSTCount     int
	SortedRunCount int
	TotalSSTSize   int64
}
