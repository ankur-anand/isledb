package isledb

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/config"
	"github.com/ankur-anand/isledb/manifest"
	"github.com/cockroachdb/pebble/v2/sstable"
)

const (
	DefaultWarmCacheTimeout    = 30 * time.Second
	DefaultSyncInterval        = 5 * time.Second
	DefaultPrefetchConcurrency = 10
)

type DBOptions struct {
	MemtableSize    int64
	FlushInterval   time.Duration
	BloomBitsPerKey int
	BlockSize       int
	Compression     string

	ValueStorage    config.ValueStorageConfig
	ManifestStorage manifest.Storage

	LogMode bool

	RetentionCompactorMode     RetentionCompactorMode
	LogRetentionPeriod         time.Duration
	LogRetentionCount          int
	RetentionCompactorInterval time.Duration
	LogSegmentDuration         time.Duration

	EnableCompaction        bool
	L0CompactionThreshold   int
	MinSources              int
	MaxSources              int
	SizeThreshold           int
	CompactionCheckInterval time.Duration

	// CacheDir is the directory for disk caches (required).
	CacheDir string

	SSTCacheSize         int64
	BlobCacheSize        int64
	BlobCacheMaxItemSize int64

	WarmCacheOnOpen     bool
	WarmCacheTimeout    time.Duration
	PrefetchConcurrency int

	BackgroundSync bool
	SyncInterval   time.Duration

	OnFlushError      func(error)
	OnCompactionStart func(CompactionJob)
	OnCompactionEnd   func(CompactionJob, error)
	OnCacheWarmDone   func(warmed int, duration time.Duration)
	OnLogCleanup      func(CleanupStats)
}

func DefaultDBOptions() DBOptions {
	return DBOptions{

		MemtableSize:    4 * 1024 * 1024,
		FlushInterval:   time.Second,
		BloomBitsPerKey: 10,
		BlockSize:       4096,
		Compression:     "snappy",
		ValueStorage:    config.DefaultValueStorageConfig(),

		LogMode:                    false,
		RetentionCompactorMode:     CompactByAge,
		LogRetentionPeriod:         7 * 24 * time.Hour,
		LogRetentionCount:          10,
		RetentionCompactorInterval: time.Minute,
		LogSegmentDuration:         time.Hour,

		EnableCompaction:        true,
		L0CompactionThreshold:   8,
		MinSources:              4,
		MaxSources:              8,
		SizeThreshold:           4,
		CompactionCheckInterval: 5 * time.Second,

		SSTCacheSize:  defaultSSTCacheSize,
		BlobCacheSize: defaultBlobCacheSize,

		WarmCacheOnOpen:     false,
		WarmCacheTimeout:    DefaultWarmCacheTimeout,
		PrefetchConcurrency: DefaultPrefetchConcurrency,

		BackgroundSync: false,
		SyncInterval:   DefaultSyncInterval,
	}
}

func DefaultLogModeOptions() DBOptions {
	opts := DefaultDBOptions()

	opts.LogMode = true
	opts.EnableCompaction = false

	opts.RetentionCompactorMode = CompactByAge
	opts.LogRetentionPeriod = 7 * 24 * time.Hour
	opts.LogRetentionCount = 10
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
	return opts
}

// WithDefaults returns a copy of opts with zero values replaced by defaults.
func (o DBOptions) WithDefaults() DBOptions {
	defaults := DefaultDBOptions()

	if o.MemtableSize == 0 {
		o.MemtableSize = defaults.MemtableSize
	}
	if o.FlushInterval == 0 {
		o.FlushInterval = defaults.FlushInterval
	}
	if o.ValueStorage.BlobThreshold == 0 {
		o.ValueStorage = defaults.ValueStorage
	}
	if o.BloomBitsPerKey == 0 {
		o.BloomBitsPerKey = defaults.BloomBitsPerKey
	}
	if o.BlockSize == 0 {
		o.BlockSize = defaults.BlockSize
	}
	if o.Compression == "" {
		o.Compression = defaults.Compression
	}

	if o.RetentionCompactorInterval == 0 {
		o.RetentionCompactorInterval = defaults.RetentionCompactorInterval
	}
	if o.LogRetentionPeriod == 0 {
		o.LogRetentionPeriod = defaults.LogRetentionPeriod
	}
	if o.LogRetentionCount == 0 {
		o.LogRetentionCount = defaults.LogRetentionCount
	}
	if o.LogSegmentDuration == 0 {
		o.LogSegmentDuration = defaults.LogSegmentDuration
	}

	if o.L0CompactionThreshold == 0 {
		o.L0CompactionThreshold = defaults.L0CompactionThreshold
	}
	if o.MinSources == 0 {
		o.MinSources = defaults.MinSources
	}
	if o.MaxSources == 0 {
		o.MaxSources = defaults.MaxSources
	}
	if o.SizeThreshold == 0 {
		o.SizeThreshold = defaults.SizeThreshold
	}
	if o.CompactionCheckInterval == 0 {
		o.CompactionCheckInterval = defaults.CompactionCheckInterval
	}

	if o.SSTCacheSize == 0 {
		o.SSTCacheSize = defaults.SSTCacheSize
	}
	if o.BlobCacheSize == 0 {
		o.BlobCacheSize = defaults.BlobCacheSize
	}

	if o.WarmCacheTimeout == 0 {
		o.WarmCacheTimeout = defaults.WarmCacheTimeout
	}
	if o.PrefetchConcurrency == 0 {
		o.PrefetchConcurrency = defaults.PrefetchConcurrency
	}
	if o.SyncInterval == 0 {
		o.SyncInterval = defaults.SyncInterval
	}

	return o
}

// Validate checks the configuration for invalid combinations.
func (o DBOptions) Validate() error {
	if o.LogMode && o.EnableCompaction {
		return errors.New("log mode and compaction are mutually exclusive")
	}
	if o.BackgroundSync && o.SyncInterval <= 0 {
		return errors.New("background sync requires positive sync interval")
	}
	if o.CacheDir == "" {
		return errors.New("CacheDir is required")
	}
	return nil
}

// ToWriterOptions converts DBOptions to WriterOptions.
func (o DBOptions) ToWriterOptions() WriterOptions {
	return WriterOptions{
		MemtableSize:    o.MemtableSize,
		FlushInterval:   o.FlushInterval,
		BloomBitsPerKey: o.BloomBitsPerKey,
		BlockSize:       o.BlockSize,
		Compression:     o.Compression,
		ValueStorage:    o.ValueStorage,
		OnFlushError:    o.OnFlushError,
		ManifestStorage: o.ManifestStorage,
	}
}

// ToReaderOptions converts DBOptions to ReaderOptions.
func (o DBOptions) ToReaderOptions() ReaderOptions {
	return ReaderOptions{
		CacheDir:             o.CacheDir,
		SSTCacheSize:         o.SSTCacheSize,
		BlobCacheSize:        o.BlobCacheSize,
		BlobCacheMaxItemSize: o.BlobCacheMaxItemSize,
		ValueStorageConfig:   o.ValueStorage,
		ManifestStorage:      o.ManifestStorage,
	}
}

// ToCompactorOptions converts DBOptions to CompactorOptions.
func (o DBOptions) ToCompactorOptions() CompactorOptions {
	return CompactorOptions{
		L0CompactionThreshold: o.L0CompactionThreshold,
		MinSources:            o.MinSources,
		MaxSources:            o.MaxSources,
		SizeThreshold:         o.SizeThreshold,
		BloomBitsPerKey:       o.BloomBitsPerKey,
		BlockSize:             o.BlockSize,
		Compression:           o.Compression,
		CheckInterval:         o.CompactionCheckInterval,
		OnCompactionStart:     o.OnCompactionStart,
		OnCompactionEnd:       o.OnCompactionEnd,
		ManifestStorage:       o.ManifestStorage,
	}
}

// ToRetentionCompactorOptions converts DBOptions to RetentionCompactorOptions.
func (o DBOptions) ToRetentionCompactorOptions() RetentionCompactorOptions {
	return RetentionCompactorOptions{
		Mode:            o.RetentionCompactorMode,
		RetentionPeriod: o.LogRetentionPeriod,
		RetentionCount:  o.LogRetentionCount,
		CheckInterval:   o.RetentionCompactorInterval,
		SegmentDuration: o.LogSegmentDuration,
		OnCleanup:       o.OnLogCleanup,
		ManifestStorage: o.ManifestStorage,
	}
}

type resourceGuard struct {
	closers []func() error
}

func (g *resourceGuard) add(closer func() error) {
	g.closers = append(g.closers, closer)
}

func (g *resourceGuard) abort() error {
	var firstErr error
	for i := len(g.closers) - 1; i >= 0; i-- {
		if err := g.closers[i](); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (g *resourceGuard) disarm() {
	g.closers = nil
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

	started atomic.Bool
	closed  atomic.Bool
}

// New creates a DB instance with writer and reader initialized.
// Unlike Open, it does not start background goroutines (compaction, sync, cache warming).
// Call Start() to begin background operations, or use Open() for the combined behavior.
func New(ctx context.Context, store *blobstore.Store, opts DBOptions) (*DB, error) {
	opts = opts.WithDefaults()
	if err := opts.Validate(); err != nil {
		return nil, err
	}

	var guard resourceGuard
	defer func() {
		guard.abort()
	}()

	writer, err := newWriter(ctx, store, opts.ToWriterOptions())
	if err != nil {
		return nil, err
	}
	guard.add(writer.close)

	reader, err := newReader(ctx, store, opts.ToReaderOptions())
	if err != nil {
		return nil, err
	}
	guard.add(reader.Close)

	db := &DB{
		store:  store,
		opts:   opts,
		writer: writer,
		reader: reader,
	}

	if opts.LogMode {
		retentionCompactor, err := NewRetentionCompactor(ctx, store, opts.ToRetentionCompactorOptions())
		if err != nil {
			return nil, err
		}

		guard.add(retentionCompactor.Close)
		db.retentionCompactor = retentionCompactor

	} else if opts.EnableCompaction {
		compactor, err := newCompactor(ctx, store, opts.ToCompactorOptions())
		if err != nil {
			return nil, err
		}

		guard.add(compactor.Close)
		db.compactor = compactor
	}

	guard.disarm()
	return db, nil
}

// Start begins background processes (compaction, cache warming, background sync).
func (db *DB) Start(ctx context.Context) error {
	if !db.started.CompareAndSwap(false, true) {
		return nil
	}

	if db.retentionCompactor != nil {
		db.retentionCompactor.Start()
	} else if db.compactor != nil {
		db.compactor.Start()
	}

	if db.opts.WarmCacheOnOpen {
		start := time.Now()
		warmed, _ := db.warmCache(ctx)
		if db.opts.OnCacheWarmDone != nil {
			db.opts.OnCacheWarmDone(warmed, time.Since(start))
		}
	}

	if db.opts.BackgroundSync {
		syncCtx, cancel := context.WithCancel(ctx)
		db.syncCancel = cancel
		db.syncWg.Add(1)
		go db.backgroundSyncLoop(syncCtx)
	}

	return nil
}

// Open creates and starts a DB.
func Open(ctx context.Context, store *blobstore.Store, opts DBOptions) (*DB, error) {
	db, err := New(ctx, store, opts)
	if err != nil {
		return nil, err
	}

	if err := db.Start(ctx); err != nil {
		db.Close()
		return nil, err
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

	if _, ok := reader.sstCache.Get(path); ok {
		return nil
	}

	data, _, err := db.store.Read(ctx, path)
	if err != nil {
		return err
	}

	reader.sstCache.Set(path, data)
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
