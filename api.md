# isledb API Reference

## Quick Start

```go
import (
    "context"
    "log"
    "time"

    "github.com/ankur-anand/isledb"
    "github.com/ankur-anand/isledb/blobstore"
)

ctx := context.Background()

// 1. Open blob storage
store, err := blobstore.Open(ctx, "s3://my-bucket?region=us-east-1", "mydb")
if err != nil {
    log.Fatal(err)
}
defer store.Close()

// 2. Open database
db, err := isledb.OpenDB(ctx, store, isledb.DBOptions{})
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// 3. Write data. Flush is the synchronous visibility boundary.
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

// 4. Read data. Refresh discovers newly committed SSTs.
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
reader, one maintenance owner, and shared manifest state.

```go
func OpenDB(ctx context.Context, store *blobstore.Store, opts DBOptions) (*DB, error)
```

| Method | Signature |
|--------|-----------|
| OpenWriter | `(ctx context.Context, opts WriterOptions) (*Writer, error)` |
| OpenReader | `(ctx context.Context, opts ReaderOpenOptions) (*Reader, error)` |
| OpenMaintenance | `(ctx context.Context, opts MaintenanceOptions) (*Maintenance, error)` |
| Close | `() error` |

```go
type DBOptions struct {
    ManifestStorage manifest.Storage // Optional custom manifest storage backend
    GCCursorStorage manifest.GCCursorStorage // Optional custom retirement cursor backend
}
```

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
- `DeleteWithTTL` stores an expiring tombstone. `ttl <= 0` means the tombstone does not expire.
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
| DeleteWithTTL | `(ctx context.Context, key []byte, ttl time.Duration) error` | Buffer a tombstone with TTL |
| Flush | `(ctx context.Context) error` | Publish all currently buffered writes |
| Close | `(ctx context.Context) error` | Stop background flushing and publish pending writes |

```go
type WriterOptions struct {
    OwnerID      string
    Memtable    WriterMemtableOptions
    Flush       WriterFlushOptions
    SST         WriterSSTOptions
    Values      config.ValueStorageConfig
    ChangeFeed  ChangeFeedOptions
    OnFlushError func(error)
    Metrics     *WriterMetrics
}

type ChangeFeedOptions struct {
    Enabled bool // Write seq-ordered mutation batches under changes/.
}

type WriterMemtableOptions struct {
    TargetBytes int64 // Approximate active memtable size before rotation. Zero selects the default.
    MaxPendingMemtables int // Max queued or flushing memtables. Zero selects the default.
}

type WriterFlushOptions struct {
    Interval time.Duration // Background flush cadence. Zero disables auto-flush.
}

type WriterSSTOptions struct {
    BloomBitsPerKey int    // Zero selects the default.
    BlockBytes      int    // Zero selects the default.
    Compression     string // "none", "snappy", or "zstd"; empty selects the default.
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
if err := w.DeleteWithTTL(ctx, []byte("lock:1"), 5*time.Second); err != nil {
    return err
}
return w.Flush(ctx)
```

---

### Snapshot

Immutable read handle over one loaded reader state. A snapshot does not refresh.
It keeps reading the same visible state even if its parent `Reader` is refreshed
later. Snapshots expire after `ReaderOpenOptions.Views.SnapshotMaxAge`; their
iterators expire after the smaller of the snapshot deadline and
`IteratorMaxAge`.

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
| Manifest | `() *Manifest` | Return cloned manifest snapshot |
| Close | `() error` | Close reader and caches. Existing snapshots become invalid. |
| BlobCacheStats | `() internal.BlobCacheStats` | Blob cache statistics |
| SSTCacheStats | `() SSTCacheStats` | SST cache statistics |
| ManifestPageCacheStats | `() cachestore.ManifestPageCacheStats` | Manifest commit-page cache statistics |

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
    Views                    ReaderViewPolicy     // Refresh and retained-view lifetime policy
    BlobReadOptions          config.BlobReadOptions
    ManifestStorage          manifest.Storage     // Optional custom manifest storage
}

func DefaultReaderOpenOptions(cacheDir string) ReaderOpenOptions

type ReaderViewPolicy struct {
    RefreshAfter   time.Duration // Default: 1 minute
    SnapshotMaxAge time.Duration // Default: 5 minutes
    IteratorMaxAge time.Duration // Default: 2 minutes
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
    ChangeFeedRetention *ChangeFeedRetentionPolicy
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
    GracePeriod     time.Duration
}

type CompactionJob struct {
    Type             CompactionJobType
    SourceLevel      uint32
    DestinationLevel uint32
    InputSSTs        []string
    OutputSSTs       []SSTMeta
    MetadataOnly     bool
}

type RetentionPolicy struct {
    Mode               RetentionMode
    KeepFor            time.Duration
    KeepAtLeastSSTs    int
    KeepAtLeastWindows int
    Window             time.Duration
    OnCleanup          func(CleanupStats)
}

type ChangeFeedRetentionPolicy struct {
    KeepFor                    time.Duration
    KeepAtLeastManifestEntries uint64
    DeleteBatchSize            int
    DeleteGracePeriod          time.Duration
    OnCleanup                  func(ChangeFeedCleanupStats)
}
```

`DefaultMaintenanceOptions` enables compaction and leaves both retention
policies nil. Use `DefaultRetentionPolicy` or
`DefaultChangeFeedRetentionPolicy` before enabling destructive cleanup.

In `RetentionByAge` mode, `KeepAtLeastSSTs` protects the newest SSTs. In
`RetentionByTimeWindow` mode, `Window` defines the grouping interval and
`KeepAtLeastWindows` protects the newest groups. The two limits are independent;
there is no conversion between SST counts and window counts.

`KeepAtLeastManifestEntries` protects the newest manifest entries from
change-feed retirement. This includes entries without a change batch because
the retention floor advances through the ordered manifest log.

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

## Manifest Types

Re-exported from `manifest` package for convenience.

### Manifest

LSM tree state snapshot.

```go
type Manifest = manifest.Manifest
```

| Field | Type | Description |
|-------|------|-------------|
| Version | `int` | Schema version |
| NextEpoch | `uint64` | Next epoch number |
| LogSeq | `uint64` | Current log sequence |
| WriterFence | `*FenceToken` | Writer ownership claim |
| CompactorFence | `*FenceToken` | Compactor ownership claim |
| L0SSTs | `[]SSTMeta` | Overlapping level-0 SSTs, newest first |
| Levels | `[]Level` | Non-overlapping levels ordered by level number |

| Method | Signature |
|--------|-----------|
| Clone | `() *Manifest` |
| L0SSTCount | `() int` |
| LookupSST | `(id string) *SSTMeta` |
| Level | `(number uint32) *Level` |
| ValidateLevels | `() error` |
| AllSSTIDs | `() []string` |
| MaxSeqNum | `() uint64` |

### SSTMeta

```go
type SSTMeta = manifest.SSTMeta
```

| Field | Type | Description |
|-------|------|-------------|
| ID | `string` | SST identifier |
| Epoch | `uint64` | Writer epoch |
| SeqLo | `uint64` | Lowest sequence number |
| SeqHi | `uint64` | Highest sequence number |
| MinKey | `[]byte` | Smallest key |
| MaxKey | `[]byte` | Largest key |
| Size | `int64` | File size in bytes |
| Checksum | `string` | `sha256:<hex>` checksum |
| Signature | `*SSTSignature` | Digital signature |
| Bloom | `BloomMeta` | Bloom filter metadata |
| CreatedAt | `time.Time` | Creation timestamp |
| Level | `uint32` | Logical LSM level (0 = L0) |
| HasBlobRefs | `bool` | Contains external blob references |

### ChangeBatchMeta

Change batches are written only when `WriterOptions.ChangeFeed.Enabled` is true.
Manifest entries are still the source of truth; readers should not discover
change history by listing `changes/`.

```go
type ChangeBatchMeta = manifest.ChangeBatchMeta
```

| Field | Type | Description |
|-------|------|-------------|
| ID | `string` | Change batch identifier |
| Path | `string` | Object path for the binary change batch |
| Epoch | `uint64` | Writer epoch |
| SeqLo | `uint64` | Lowest mutation sequence |
| SeqHi | `uint64` | Highest mutation sequence |
| Count | `uint32` | Number of changes in the batch |
| Size | `int64` | Encoded object size |
| Checksum | `string` | Encoded object checksum |
| CreatedAt | `time.Time` | Creation time |
| Version | `int` | Change batch format version |

### Level

```go
type Level = manifest.Level
```

| Field | Type |
|-------|------|
| Number | `uint32` |
| SSTs | `[]SSTMeta` |

### SSTSignature

```go
type SSTSignature = manifest.SSTSignature
```

| Field | Type |
|-------|------|
| Algorithm | `string` |
| KeyID | `string` |
| Hash | `string` |
| Signature | `[]byte` |

### FenceToken

```go
// from manifest package
type FenceToken struct {
    Epoch     uint64
    Owner     string
    ClaimedAt time.Time
}
```

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

### manifest.GCMarkStorage

Stores GC coordination state (pending SST delete marks and GC checkpoint) with CAS semantics.

```go
type GCMarkStorage interface {
    LoadPendingDeleteMarks(ctx context.Context) ([]byte, string, bool, error)
    StorePendingDeleteMarks(ctx context.Context, data []byte, matchToken string, exists bool) error
    LoadGCCheckpoint(ctx context.Context) ([]byte, string, bool, error)
    StoreGCCheckpoint(ctx context.Context, data []byte, matchToken string, exists bool) error
}
```

---

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

---

## Configuration

### config.ValueStorageConfig

Controls how values are stored (inline vs. blob storage).

```go
type ValueStorageConfig struct {
    ValueOptions
    BlobReadOptions
    BlobGCOptions
}

func DefaultValueStorageConfig() ValueStorageConfig
```

```go
type ValueOptions struct {
    MaxKeySize    int   // Maximum key size
    BlobThreshold int   // Values >= this size go to blob storage
    MaxValueSize  int64 // Maximum value size
}

type BlobReadOptions struct {
    VerifyBlobsOnRead bool // Re-hash blobs on read for integrity
}

// Note: Current Not Implemented Just Defined
type BlobGCOptions struct {
    Enabled  bool
    Interval time.Duration
    MinAge   time.Duration
}
```

---

## Manifest Store

### manifest.Store

Manages manifest snapshots, committed entry pages, CURRENT, and writer/compactor fences.

```go
func NewStore(store *blobstore.Store) *Store
func NewStoreWithStorage(storage Storage) *Store
```

| Method | Signature | Description |
|--------|-----------|-------------|
| ClaimWriter | `(ctx, ownerID) (*FenceToken, error)` | Claim writer fence |
| ClaimCompactor | `(ctx, ownerID) (*FenceToken, error)` | Claim compactor fence |
| CheckWriterFence | `(ctx) error` | Verify writer fence still valid |
| CheckCompactorFence | `(ctx) error` | Verify compactor fence still valid |
| Replay | `(ctx) (*Manifest, error)` | Rebuild manifest from CURRENT, snapshots, and committed entries |
| AppendWriterCommit | `(ctx, WriterCommit) (*ManifestLogEntry, error)` | Idempotently publish one SST and optional change batch using a stable commit ID |
| AppendAddSSTableWithFence | `(ctx, SSTMeta) (*ManifestLogEntry, error)` | Append SST add entry |
| AppendAddSSTableWithChangeBatchWithFence | `(ctx, SSTMeta, *ChangeBatchMeta) (*ManifestLogEntry, error)` | Append paired SST and change-batch entry |
| AppendRemoveSSTablesWithFence | `(ctx, []string, []RetiredObject) (*ManifestLogEntry, error)` | Atomically remove SST metadata and record exact retired objects |
| AppendCompactionWithFence | `(ctx, CompactionLogPayload, []RetiredObject) (*ManifestLogEntry, error)` | Append one bounded adjacent-level compaction entry with explicit source and destination levels |
| ReadRetirementEntries | `(ctx, start uint64, limit int) ([]*ManifestLogEntry, uint64, error)` | Read a bounded retirement-history page |
| AdvanceRetirementLogStart | `(ctx, floor uint64, *FenceToken) (*Current, error)` | Advance the retained retirement floor after cursor commit |
| WriteSnapshot | `(ctx, *Manifest) (string, error)` | Write manifest snapshot |

**Errors:**
- `manifest.ErrFenced` - Epoch superseded by newer owner
- `manifest.ErrFenceConflict` - Concurrent claim detected
- `manifest.ErrInvalidWriterCommit` - Commit identity or SST/change metadata is invalid
- `manifest.ErrWriterCommitConflict` - A commit ID was reused with different metadata
- `manifest.ErrInvalidRetirement` - Removed SSTs and exact retirement records do not match
- `manifest.ErrInvalidManifest` - A replayed or submitted level topology is invalid
