# isledb API Reference

## Quick Start

```go
import (
    "context"
    "log"
    "time"

    "github.com/ankur-anand/isledb"
)

ctx := context.Background()

// 1. Open the database and its object-store connection.
db, err := isledb.Open(ctx, "s3://my-bucket?region=us-east-1", isledb.DBOptions{
    Prefix: "mydb",
})
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// 2. Write data. Flush is the synchronous visibility boundary.
w, err := db.OpenWriter(ctx, isledb.DefaultWriterOptions())
if err != nil {
    log.Fatal(err)
}
defer w.Close(ctx)

if err := w.Put(ctx, []byte("user:1"), []byte("ankur")); err != nil {
    log.Fatal(err)
}
if err := w.PutWithTTL(ctx, []byte("session:1"), []byte("active"), time.Hour); err != nil {
    log.Fatal(err)
}
if err := w.Flush(ctx); err != nil {
    log.Fatal(err)
}

// 3. Read data. Refresh discovers newly committed SSTs.
r, err := db.OpenReader(ctx, isledb.DefaultReaderOpenOptions("./cache"))
if err != nil {
    log.Fatal(err)
}
defer r.Close()

if err := r.Refresh(ctx); err != nil {
    log.Fatal(err)
}
val, found, err := r.Get(ctx, []byte("user:1"))
_ = val
_ = found
_ = err
```

---

## Core Types

### DB

Entry point for database operations. Manages one writer, one shared concurrent
KV reader, one maintenance owner, any number of change readers, and shared
manifest state.

```go
func Open(ctx context.Context, bucketURL string, opts DBOptions) (*DB, error)
func OpenBucket(ctx context.Context, bucket *blob.Bucket, bucketName string, opts DBOptions) (*DB, error)
```

| Method | Signature |
|--------|-----------|
| OpenWriter | `(ctx context.Context, opts WriterOptions) (*Writer, error)` |
| OpenReader | `(ctx context.Context, opts ReaderOpenOptions) (*Reader, error)` |
| OpenChangeReader | `(ctx context.Context) (*ChangeReader, error)` |
| OpenMaintenance | `(ctx context.Context, opts MaintenanceOptions) (*Maintenance, error)` |
| Close | `() error` |

```go
type DBOptions struct {
    Prefix           string
    EnableChangeFeed bool
    Policy           StorePolicy
}

type StorePolicy struct {
    MaxPinnedViewAge time.Duration // Default: 1 hour
}
```

`Open` owns the bucket connection and closes it from `DB.Close`. `OpenBucket`
borrows an existing Go Cloud bucket and leaves its lifecycle with the caller.
`EnableChangeFeed` is persisted in the manifest and cannot be disabled. Enabling
an existing database starts the feed at its current manifest head.
`MaxPinnedViewAge` is persisted by the first writer. Later writers must present
the same value. Readers and SST garbage collection both derive their safety
deadline from this one store policy.

---

### Writer

Provides write access to the database. A writer buffers mutations in memory,
flushes full memtables into immutable SST files, and commits those SSTs through
the manifest.

`Writer` uses internal locks to protect state and coordinate with background
flushing. Those locks are not a concurrent API contract: concurrent public calls
do not have documented ordering or Close/Flush semantics. Serialize `Put`,
`Delete`, `Flush`, and `Close` for one writer.

Visibility contract:

- `Put` and `Delete` return after the mutation is buffered locally.
- `PutWithTTL` stores an expiry with the value. `ttl <= 0` means no expiration.
- `Flush` is the synchronous publish boundary. It writes pending memtables as
  SST files and commits manifest entries.
- A retry of an explicit `Flush` reuses the same uploaded SST, change batch,
  and logical commit ID. An uncertain `CURRENT` response is reconciled before
  another manifest entry can be appended.
- `Close` stops background flushing and flushes pending writes before returning.
- Readers see newly flushed data after they open or call `Reader.Refresh`.

| Method | Signature | Description |
|--------|-----------|-------------|
| Put | `(ctx context.Context, key, value []byte) error` | Buffer a key-value mutation |
| PutWithTTL | `(ctx context.Context, key, value []byte, ttl time.Duration) error` | Buffer a key-value mutation with time-to-live |
| Delete | `(ctx context.Context, key []byte) error` | Buffer a tombstone |
| Flush | `(ctx context.Context) error` | Publish all currently buffered writes |
| Close | `(ctx context.Context) error` | Stop background flushing and publish pending writes |

```go
type WriterOptions struct {
    OwnerID       string
    Memtable     WriterMemtableOptions
    Flush        WriterFlushOptions
    Maintenance  WriterMaintenanceOptions
    SST          WriterSSTOptions
    Values       ValueOptions
    OnFlushError func(error)
    Metrics      *WriterMetrics
}

type WriterMemtableOptions struct {
    TargetBytes int64 // Approximate active memtable size before rotation. Zero selects the default.
    MaxPendingMemtables int // Max queued or flushing memtables. Zero selects the default.
}

type WriterFlushOptions struct {
    Interval time.Duration // Background flush cadence. Zero disables auto-flush.
}

type WriterMaintenanceOptions struct {
    PollInterval time.Duration // Minimum interval between maintenance mailbox reads.
}

type WriterSSTOptions struct {
    BloomBitsPerKey int    // Zero selects the default.
    BlockBytes      int    // Zero selects the default.
    Compression     string // "none", "snappy", or "zstd"; empty selects the default.
}

type ValueOptions struct {
    MaxKeyBytes      int   // Largest accepted key size.
    InlineValueBytes int   // Values at or above this size are stored outside the SST.
    MaxValueBytes    int64 // Largest accepted value size.
}

func DefaultWriterOptions() WriterOptions
```

**Errors:**
- `ErrBackpressure` - writer hit `Memtable.MaxPendingMemtables`; caller should retry after a delay or flush.
- `ErrInvalidWriterOptions` - writer configuration contains a negative size or
  interval, unsupported compression, oversized identity, invalid value limit,
  or a memtable configuration that exceeds the arena format limit.
- `ErrWriterFailed` - background flushing failed. The writer is terminal and the error wraps the original cause.

Background flush:

- `Flush.Interval > 0` starts a background flush loop.
- The first background flush error makes the writer terminal. It is delivered
  once to `WriterOptions.OnFlushError` after the background worker stops,
  otherwise it is logged. The callback may call `Writer.Close`.
- Later mutations, `Flush`, and `Close` return the stored `ErrWriterFailed`
  value wrapping the original failure.
- Explicit `Flush` and `Close` return flush errors directly.
- An explicit `Flush` failure remains retryable because the caller observes it
  synchronously.

Example:

```go
opts := isledb.DefaultWriterOptions()
opts.Flush.Interval = time.Second
opts.Memtable.TargetBytes = 16 << 20
opts.Memtable.MaxPendingMemtables = 4

w, err := db.OpenWriter(ctx, opts)
if err != nil {
    return err
}
defer w.Close(ctx)

if err := w.Put(ctx, []byte("user:1"), []byte("ankur")); err != nil {
    return err
}
if err := w.PutWithTTL(ctx, []byte("session:1"), []byte("active"), 30*time.Minute); err != nil {
    return err
}
if err := w.Delete(ctx, []byte("lock:1")); err != nil {
    return err
}
return w.Flush(ctx)
```

---

### Snapshot

Immutable read handle over one loaded reader state. A snapshot does not refresh.
It keeps reading the same visible state even if its parent `Reader` is refreshed
later. A snapshot and every iterator created from it inherit the absolute
deadline of the loaded manifest view. Creating a handle does not extend that
deadline.

| Method | Signature | Description |
|--------|-----------|-------------|
| Version | `() Version` | Return an opaque identifier for the loaded visible state |
| Get | `(ctx context.Context, key []byte) ([]byte, bool, error)` | Retrieve value for key from this fixed view |
| NewIterator | `(ctx context.Context, opts IteratorOptions) (*Iterator, error)` | Create a bounded iterator over this fixed view |
| ScanLimit | `(ctx context.Context, minKey, maxKey []byte, limit int) ([]KV, error)` | Read up to limit records from this fixed view |
| Close | `() error` | Release the caller reference to the view |

```go
type Version struct { ... }
type Snapshot struct { ... }
```

### Reader

Read-only handle for database access. Supports point lookups, range scans, and iteration.

Readers are opened from the database that owns the object-store prefix:

```go
func (db *DB) OpenReader(ctx context.Context, opts ReaderOpenOptions) (*Reader, error)
```

Opening a reader loads a view. Reads refresh it when `Views.RefreshAfter` has
elapsed; concurrent refreshes are coalesced. `Refresh` remains available when
the caller needs an immediate visibility check.

| Method | Signature | Description |
|--------|-----------|-------------|
| Refresh | `(ctx context.Context) error` | Reload manifest and invalidate removed SSTs |
| Get | `(ctx context.Context, key []byte) ([]byte, bool, error)` | Retrieve one key from the current view |
| Scan | `(ctx context.Context, minKey, maxKey []byte) ([]KV, error)` | Scan a key range into memory |
| ScanLimit | `(ctx context.Context, minKey, maxKey []byte, limit int) ([]KV, error)` | Scan a bounded number of records |
| NewIterator | `(ctx context.Context, opts IteratorOptions) (*Iterator, error)` | Stream a bounded key range |
| Snapshot | `(ctx context.Context) (*Snapshot, error)` | Load a fresh state and pin it for bounded consistent reads |
| Prefetch | `(ctx context.Context, opts PrefetchOptions) (PrefetchStats, error)` | Warm SST cache for the current manifest view |
| Close | `() error` | Close reader and caches. Existing snapshots become invalid. |
| BlobCacheStats | `() CacheStats` | External-value cache statistics |
| SSTCacheStats | `() CacheStats` | SST cache statistics |
| ManifestPageCacheStats | `() CacheStats` | Manifest commit-page cache statistics |

```go
type ReaderOpenOptions struct {
    CacheDir                 string               // Required disk cache directory.
    SSTCacheSize             int64                // Default: 1GB
    BlobCacheSize            int64                // Default: 1GB
    BlobCacheMaxItemSize     int64                // Max size per cached blob (0 = no limit)
    BlockCacheSize           int64                // Range-read block cache (0 = disabled)
    AllowUnverifiedRangeRead bool                 // Allow range reads without checksum verification
    RangeReadMinSSTSize      int64                // Minimum SST size for range-read optimization
    ValidateSSTChecksum      bool                 // Verify SST checksums on read
    SSTHashVerifier          SSTHashVerifier      // SST signature verifier
    Views                    ReaderViewPolicy     // Manifest freshness policy
    VerifyBlobsOnRead        bool
}

type CacheStats struct {
    Hits       int64
    Misses     int64
    Bytes      int64
    MaxBytes   int64
    EntryCount int
    MaxEntries int
}

func DefaultReaderOpenOptions(cacheDir string) ReaderOpenOptions

type ReaderViewPolicy struct {
    RefreshAfter time.Duration // Default: 1 minute
}
```

`Prefetch` first applies the same freshness policy as a read, then downloads
selected SSTs from that manifest into the local cache. It does not force an
object-store refresh while the loaded view is still fresh.

Example:

```go
r, err := db.OpenReader(ctx, isledb.DefaultReaderOpenOptions("./cache"))
if err != nil {
    return err
}
defer r.Close()

if err := r.Refresh(ctx); err != nil {
    return err
}

value, ok, err := r.Get(ctx, []byte("user:1"))
if err != nil {
    return err
}
_ = value
_ = ok

items, err := r.ScanLimit(ctx, []byte("user:"), []byte("user;"), 100)
if err != nil {
    return err
}
_ = items
```

```go
type KeyRange struct {
    Min []byte // inclusive; nil means beginning
    Max []byte // exclusive; nil means end
}

func PrefixRange(prefix []byte) KeyRange

type PrefetchOptions struct {
    Range       KeyRange // Select SSTs overlapping this half-open range.
    All         bool     // Required for whole-database prefetch.
    MaxSSTs     int      // 0 = no limit
    MaxBytes    int64    // 0 = no limit
    Concurrency int      // 0 = default
}

type PrefetchStats struct {
    MatchedSSTs int
    CachedSSTs  int
    SkippedSSTs int
    BytesRead   int64
}
```

Prefetch example:

```go
if err := r.Refresh(ctx); err != nil {
    return err
}

stats, err := r.Prefetch(ctx, isledb.PrefetchOptions{
    Range:       isledb.PrefixRange([]byte("user:")),
    MaxBytes:    256 << 20,
    Concurrency: 4,
})
if err != nil {
    return err
}
_ = stats
```

#### KV

```go
type KV struct {
    Key   []byte
    Value []byte
}
```

#### Iterator

Bounded range traversal over the database.

| Method | Signature | Description |
|--------|-----------|-------------|
| Next | `() bool` | Advance to next entry |
| Key | `() []byte` | Current key |
| Value | `() []byte` | Current value |
| Valid | `() bool` | Whether iterator is positioned at a valid entry |
| Err | `() error` | Any iteration error |
| SeekGE | `(target []byte) bool` | Seek to first key >= target |
| Close | `() error` | Close iterator |

```go
type IteratorOptions struct {
    MinKey []byte // Inclusive lower bound
    MaxKey []byte // Inclusive upper bound
}
```

### ChangeReader

Reads the optional durable mutation feed without object-store listing. Open it
after `DBOptions.EnableChangeFeed` has been persisted for the database prefix.
Any number of independent change readers may be opened from one `DB`.

```go
func (db *DB) OpenChangeReader(ctx context.Context) (*ChangeReader, error)
```

| Method | Signature | Description |
|--------|-----------|-------------|
| Bounds | `(ctx context.Context) (ChangeBounds, error)` | Return oldest retained and current head cursors |
| Read | `(ctx context.Context, from ChangeCursor, opts ChangeReadOptions) (ChangePage, error)` | Return a bounded ordered page |
| Close | `() error` | Release this reader |

```go
type Change struct {
    Sequence  uint64
    Operation ChangeOperation // ChangePut or ChangeDelete
    Key       []byte
    Value     []byte          // empty for deletes
    ExpiresAt time.Time       // zero when no TTL applies
}

type ChangeBounds struct {
    Oldest ChangeCursor
    Head   ChangeCursor
}

type ChangeReadOptions struct {
    MaxChanges int
    MaxBytes   int64
}

func DefaultChangeReadOptions() ChangeReadOptions
func ParseChangeCursor(value string) (ChangeCursor, error)
func (c ChangeCursor) String() string
func (p ChangePage) CaughtUp() bool
```

The cursor points to the next change, including a position inside a large flush
batch. Save `page.Next.String()` only after processing the returned changes.
Use `bounds.Oldest` for retained replay or `bounds.Head` for future commits.
A zero cursor also starts at the oldest retained change.

`Read` performs no polling. An empty page can still advance `Next` over
manifest entries that contain no user mutations; continue until `CaughtUp`
returns true. When retention has passed a saved cursor, `Read` returns
`ErrChangeCursorExpired`.

Each `ChangeReader` owns its manifest view. A `Read` beginning at a manifest
entry boundary refreshes `manifest/CURRENT`, then reads from that immutable
view. If a returned page stops inside a change batch, continuation reads reuse
that decoded batch and its observed head without another `CURRENT` or
manifest-page read. The next call beginning at an entry boundary refreshes the
view and revalidates the retention floor.

### Maintenance

Owns one fenced maintenance session for a database prefix. It runs compaction,
optional SST retention, optional change-feed retention, and garbage collection
in a fixed serialized order. `DB.OpenMaintenance` rejects a second active
maintenance handle opened from the same `DB`.

| Method | Signature | Description |
|--------|-----------|-------------|
| Run | `(ctx context.Context) error` | Run cycles until cancellation, fencing, or `Close` |
| RunOnce | `(ctx context.Context) (MaintenanceStats, error)` | Perform one serialized cycle |
| Close | `(ctx context.Context) error` | Stop scheduling, wait for active work, and release ownership |

```go
type MaintenanceOptions struct {
    OwnerID            string
    Every              time.Duration
    Compaction         CompactionPolicy
    GarbageCollection  GarbageCollectionPolicy
    Retention          *RetentionPolicy
    OnCycle            func(MaintenanceStats)
    OnError            func(error)
}

type CompactionPolicy struct {
    InputReadParallelism        int
    L0SSTCount                  int
    MaxConsecutiveL0Compactions int
    BaseLevelBytes              int64
    LevelSizeMultiplier         int
    MaxInputSSTs                int
    TargetSSTBytes              int64
    BloomBitsPerKey             int
    BlockBytes                  int
    Compression                 string
    ValidateSSTChecksum         bool
    SSTHashVerifier             SSTHashVerifier
    OnCompactionStart           func(CompactionJob)
    OnCompactionEnd             func(CompactionJob, error)
}

type GarbageCollectionPolicy struct {
    DeleteBatchSize int
}

type CompactionJob struct {
    Type             CompactionJobType
    SourceLevel      uint32
    DestinationLevel uint32
    InputSSTs        []string
    OutputSSTs       []CompactionOutput
    MetadataOnly     bool
}

type CompactionOutput struct {
    ID    string
    Bytes int64
    Level uint32
}

type RetentionPolicy struct {
    Mode               RetentionMode
    KeepFor            time.Duration
    KeepAtLeastSSTs    int
    KeepAtLeastWindows int
    Window             time.Duration
    OnCleanup          func(CleanupStats)
}

```

`DefaultMaintenanceOptions` enables compaction and leaves retention disabled.
Use `DefaultRetentionPolicy` before enabling destructive cleanup.

In `RetentionByAge` mode, `KeepAtLeastSSTs` protects the newest SSTs. In
`RetentionByTimeWindow` mode, `Window` defines the grouping interval and
`KeepAtLeastWindows` protects the newest groups. The two limits are independent;
there is no conversion between SST counts and window counts.

```go
opts := isledb.DefaultMaintenanceOptions()
opts.Every = 5 * time.Second
opts.Compaction.L0SSTCount = 8
opts.Compaction.InputReadParallelism = 4
opts.Compaction.TargetSSTBytes = 64 << 20

retention := isledb.DefaultRetentionPolicy()
retention.KeepFor = 7 * 24 * time.Hour
retention.KeepAtLeastSSTs = 10
opts.Retention = &retention

m, err := db.OpenMaintenance(ctx, opts)
if err != nil {
    return err
}
defer m.Close(ctx)

return m.Run(ctx)
```

For a scheduled job:

```go
stats, err := m.RunOnce(ctx)
```

---

## Integrity

### SSTSignature

```go
type SSTSignature struct {
    Algorithm string
    KeyID     string
    Hash      string
    Signature []byte
}
```

| Field | Type |
|-------|------|
| Algorithm | `string` |
| KeyID | `string` |
| Hash | `string` |
| Signature | `[]byte` |

---

## Interfaces

### SSTHashSigner

Signs SST hashes for integrity verification.

```go
type SSTHashSigner interface {
    Algorithm() string
    KeyID() string
    SignHash(hash []byte) ([]byte, error)
}
```

### SSTHashVerifier

Verifies SST hash signatures.

```go
type SSTHashVerifier interface {
    VerifyHash(hash []byte, sig SSTSignature) error
}
```

## Blob Storage

### blobstore.Store

Abstraction over cloud object storage (S3, GCS, Azure Blob).

```go
// Open with bucket URL
func Open(ctx context.Context, bucketURL, prefix string) (*Store, error)

// Wrap existing bucket
func New(bkt *blob.Bucket, bucketName, prefix string) *Store

// In-memory store for testing
func NewMemory(prefix string) *Store
```

| Method | Signature | Description |
|--------|-----------|-------------|
| Close | `() error` | Close the store |
| Prefix | `() string` | Storage prefix |
| SSTPath | `(id string) string` | Path for SST file |
| ChangeBatchPath | `(id string) string` | Path for change batch file |
| BlobPath | `(blobID string) string` | Path for blob file |
| Read | `(ctx, key) ([]byte, Attributes, error)` | Read object |
| ReadRange | `(ctx, key, offset, length) ([]byte, error)` | Read byte range |
| Write | `(ctx, key, data) (Attributes, error)` | Write object |
| WriteIfMatch | `(ctx, key, data, ifMatch) (Attributes, error)` | CAS write |
| WriteIfNotExist | `(ctx, key, data) (Attributes, error)` | Create-only write |
| Delete | `(ctx, key) error` | Delete object |
| BatchDelete | `(ctx, keys) error` | Batch delete |
| Exists | `(ctx, key) (bool, error)` | Check existence |
| Attributes | `(ctx, key) (Attributes, error)` | Get object attributes |
| List | `(ctx, ListOptions) (*ListResult, error)` | List objects |
| ListSSTFiles | `(ctx) ([]ObjectInfo, error)` | List SST files |
| ListBlobFiles | `(ctx) ([]ObjectInfo, error)` | List blob files |

```go
type Attributes struct {
    Size       int64
    ETag       string
    ModTime    time.Time
    Generation int64 // GCS only
}
```

**Errors:**
- `blobstore.ErrNotFound` - Object does not exist
- `blobstore.ErrPreconditionFailed` - CAS condition not met
- `blobstore.BatchDeleteError` - Partial batch delete failure (`.Failed` map)
