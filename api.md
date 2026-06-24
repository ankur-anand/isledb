# isledb API Reference

## Quick Start

```go
import (
    "github.com/ankur-anand/isledb"
    "github.com/ankur-anand/isledb/blobstore"
)

// 1. Open blob storage
store, err := blobstore.Open(ctx, "s3://my-bucket?region=us-east-1", "mydb")

// 2. Open database
db, err := isledb.OpenDB(ctx, store, isledb.DBOptions{})
defer db.Close()

// 3. Write data
w, err := db.OpenWriter(ctx, isledb.DefaultWriterOptions())
defer w.Close(ctx)
w.Put(ctx, []byte("key"), []byte("value"))
w.Flush(ctx)

// 4. Read data
r, err := isledb.OpenReader(ctx, store, isledb.DefaultReaderOpenOptions())
defer r.Close()
val, found, err := r.Get(ctx, []byte("key"))
```

---

## Core Types

### DB

Entry point for database operations. Manages writers, compactors, and manifest state.

```go
func OpenDB(ctx context.Context, store *blobstore.Store, opts DBOptions) (*DB, error)
```

| Method | Signature |
|--------|-----------|
| OpenWriter | `(ctx context.Context, opts WriterOptions) (*Writer, error)` |
| OpenCompactor | `(ctx context.Context, opts CompactorOptions) (*Compactor, error)` |
| OpenRetentionCompactor | `(ctx context.Context, opts RetentionCompactorOptions) (*RetentionCompactor, error)` |
| OpenChangeFeedCleaner | `(ctx context.Context, opts ChangeFeedCleanerOptions) (*ChangeFeedCleaner, error)` |
| Close | `() error` |

```go
type DBOptions struct {
    ManifestStorage manifest.Storage // Optional custom manifest storage backend
    GCMarkStorage   manifest.GCMarkStorage // Optional custom GC mark storage backend
    KeyPositionExtractor isledb.KeyPositionExtractor // Optional CURRENT position updater for monotonic keyspaces
}
```

`KeyPositionExtractor` is intended for monotonic keyspaces where the
lexicographically largest key is also the latest logical position, such as
8-byte big-endian sequence keys. If it is not configured,
`Reader.MaxCommittedPosition` returns `found=false`.

---

### Writer

Provides write access to the database. Buffers writes in a memtable and flushes to SSTs.

| Method | Signature | Description |
|--------|-----------|-------------|
| Put | `(ctx context.Context, key, value []byte) error` | Write a key-value pair |
| PutWithTTL | `(ctx context.Context, key, value []byte, ttl time.Duration) error` | Write with time-to-live |
| Delete | `(ctx context.Context, key []byte) error` | Mark a key as deleted |
| DeleteWithTTL | `(ctx context.Context, key []byte, ttl time.Duration) error` | Delete with TTL |
| Flush | `(ctx context.Context) error` | Force flush memtable to SST |
| Close | `(ctx context.Context) error` | Close the writer |

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
    TargetBytes int64 // Approximate active memtable size before rotation.
    MaxFrozen   int   // Max full memtables waiting for flush. Zero means unbounded.
}

type WriterFlushOptions struct {
    Interval time.Duration // Background flush cadence. Zero disables auto-flush.
}

type WriterSSTOptions struct {
    BloomBitsPerKey int
    BlockBytes      int
    Compression     string
}

func DefaultWriterOptions() WriterOptions
```

**Errors:**
- `ErrBackpressure` - Writer hit memtable queue limit, caller should retry after a delay.

---

### Snapshot

Immutable read handle over one loaded reader state. A snapshot does not refresh.
It keeps reading the same visible state even if its parent `Reader` is refreshed
later.

| Method | Signature | Description |
|--------|-----------|-------------|
| Version | `() Version` | Return an opaque identifier for the loaded visible state |
| Get | `(ctx context.Context, key []byte) ([]byte, bool, error)` | Retrieve value for key from this fixed view |
| NewIterator | `(ctx context.Context, opts IteratorOptions) (*Iterator, error)` | Create a bounded iterator over this fixed view |
| ScanLimit | `(ctx context.Context, minKey, maxKey []byte, limit int) ([]KV, error)` | Read up to limit records from this fixed view |
| MaxCommittedPosition | `() (uint64, bool)` | Return the head/high-watermark captured with this view |
| LowWatermarkPosition | `() (uint64, bool)` | Return the low watermark captured with this view |
| Close | `() error` | Release the caller reference to the view |

```go
type Version struct { ... }
type Snapshot struct { ... }
```

### Reader

Read-only handle for database access. Supports point lookups, range scans, and iteration.

```go
func OpenReader(ctx context.Context, store *blobstore.Store, opts ReaderOpenOptions) (*Reader, error)
```

| Method | Signature | Description |
|--------|-----------|-------------|
| RefreshAndPrefetchSSTs | `(ctx context.Context) error` | Reload manifest and proactively warm newly visible SSTs into cache |
| Get | `(ctx context.Context, key []byte) ([]byte, bool, error)` | Retrieve value for key |
| MaxCommittedPosition | `(ctx context.Context) (uint64, bool, error)` | Read latest committed position from `CURRENT` for monotonic keyspaces |
| Scan | `(ctx context.Context, minKey, maxKey []byte) ([]KV, error)` | Scan a key range |
| ScanLimit | `(ctx context.Context, minKey, maxKey []byte, limit int) ([]KV, error)` | Scan with result limit |
| NewIterator | `(ctx context.Context, opts IteratorOptions) (*Iterator, error)` | Create bounded iterator |
| Refresh | `(ctx context.Context) error` | Reload manifest and invalidate removed SSTs |
| Snapshot | `() *Snapshot` | Pin the current loaded state for consistent multi-operation reads |
| Manifest | `() *Manifest` | Return cloned manifest snapshot |
| ManifestUnsafe | `() *Manifest` | Return manifest without copying |
| Close | `() error` | Close reader and caches |
| BlobCacheStats | `() internal.BlobCacheStats` | Blob cache statistics |
| SSTCacheStats | `() SSTCacheStats` | SST cache statistics |
| ManifestPageCacheStats | `() cachestore.ManifestPageCacheStats` | Manifest commit-page cache statistics |

```go
type ReaderOpenOptions struct {
    CacheDir                 string               // Required: directory for disk caches
    SSTCacheSize             int64                // Default: 1GB
    BlobCacheSize            int64                // Default: 1GB
    BlobCacheMaxItemSize     int64                // Max size per cached blob (0 = no limit)
    BlockCacheSize           int64                // Range-read block cache (0 = disabled)
    AllowUnverifiedRangeRead bool                 // Allow range reads without checksum verification
    RangeReadMinSSTSize      int64                // Minimum SST size for range-read optimization
    ValidateSSTChecksum      bool                 // Verify SST checksums on read
    SSTHashVerifier          SSTHashVerifier      // SST signature verifier
    BlobReadOptions          config.BlobReadOptions
    ManifestStorage          manifest.Storage     // Optional custom manifest storage
}

func DefaultReaderOpenOptions() ReaderOpenOptions
```

`MaxCommittedPosition` is a fast path backed by `CURRENT`. Use it only for workloads
where max key equals latest logical position. It is not a general-purpose
replacement for manifest replay or arbitrary KV ordering.

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

### Compactor

Merges L0 SSTs into sorted runs and merges consecutive similar runs in the background.

| Method | Signature | Description |
|--------|-----------|-------------|
| Start | `(ctx context.Context) error` | Start background compaction loop |
| Close | `(ctx context.Context) error` | Stop background compaction and close compactor |
| Refresh | `(ctx context.Context) error` | Reload manifest |
| RunOnce | `(ctx context.Context) error` | Perform one scheduler compaction pass |
| IsFenced | `() bool` | Check if compactor is fenced |
| FenceToken | `() *manifest.FenceToken` | Get fence token |

```go
type CompactorOptions struct {
    OwnerID string
    Trigger CompactionTriggerOptions
    Output  CompactionOutputOptions
    Safety  CompactionSafetyOptions
    OnCompactionStart     func(CompactionJob)      // Compaction start callback
    OnCompactionEnd       func(CompactionJob, error) // Compaction end callback
    GCMarkStorage         manifest.GCMarkStorage   // Optional custom GC mark storage backend
}

type CompactionTriggerOptions struct {
    CheckInterval time.Duration // Background scheduler cadence used by Start.
    L0SSTCount    int           // L0 SST count that triggers an L0 merge.
    MinSources    int           // Minimum sorted runs to merge.
    MaxSources    int           // Maximum sorted runs to merge.
    SizeRatio     int           // Size similarity threshold.
}

type CompactionOutputOptions struct {
    TargetSSTBytes int64
    BloomBitsPerKey int
    BlockBytes      int
    Compression     string
}

type CompactionSafetyOptions struct {
    ValidateSSTChecksum bool
    SSTHashVerifier     SSTHashVerifier
}

func DefaultCompactorOptions() CompactorOptions
```

```go
type CompactionJob struct {
    Type      CompactionJobType // CompactionL0Flush or CompactionConsecutiveMerge
    InputSSTs []string
    InputRuns []uint32
    OutputRun *SortedRun
}
```

---

### RetentionCompactor

Deletes old SSTs based on a retention policy (age-based FIFO or time-window segmented).

| Method | Signature | Description |
|--------|-----------|-------------|
| Start | `(ctx context.Context) error` | Start background cleanup loop |
| Close | `(ctx context.Context) error` | Stop background cleanup and close retention compactor |
| Refresh | `(ctx context.Context) error` | Reload manifest |
| RunOnce | `(ctx context.Context) error` | Perform a cleanup cycle |
| IsFenced | `() bool` | Check if fenced |
| Stats | `() RetentionCompactorStats` | Get current statistics |

```go
type RetentionCompactorOptions struct {
    Mode            RetentionCompactorMode // CompactByAge or CompactByTimeWindow
    RetentionPeriod time.Duration          // How long to keep data (default: 7 days)
    RetentionCount  int                    // Minimum SSTs to keep (default: 10)
    CheckInterval   time.Duration          // Cleanup check interval (default: 1 min)
    SegmentDuration time.Duration          // Time window segment size (default: 1 hour)
    OnCleanup       func(CleanupStats)     // Cleanup complete callback
    OnCleanupError  func(error)            // Cleanup error callback
    GCMarkStorage   manifest.GCMarkStorage // Optional custom GC mark storage backend
}

func DefaultRetentionCompactorOptions() RetentionCompactorOptions
```

```go
type CleanupStats struct {
    SSTsDeleted    int
    BytesReclaimed int64
    Duration       time.Duration
}

type RetentionCompactorStats struct {
    Mode            RetentionCompactorMode
    RetentionPeriod time.Duration
    L0SSTCount      int
    SortedRunCount  int
    TotalSize       int64
    OldestSST       time.Time
}
```

---

### ChangeFeedCleaner

Advances the retained change-feed floor and deletes old `changes/*.chg` objects
after a grace period. This is only needed when writers enable change feed.

| Method | Signature | Description |
|--------|-----------|-------------|
| Start | `(ctx context.Context) error` | Start background cleanup loop |
| Close | `(ctx context.Context) error` | Stop background cleanup and close cleaner |
| RunOnce | `(ctx context.Context) error` | Perform one cleanup cycle |

```go
type ChangeFeedCleanerOptions struct {
    RetentionPeriod  time.Duration
    RetentionCount   uint64
    CheckInterval    time.Duration
    SweepBatchSize   int
    SweepGracePeriod time.Duration
    OnCleanup        func(ChangeFeedCleanupStats)
    OnCleanupError   func(error)
}

func DefaultChangeFeedCleanerOptions() ChangeFeedCleanerOptions
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
| L0SSTs | `[]SSTMeta` | Level-0 SSTs |
| SortedRuns | `[]SortedRun` | Sorted runs (level 1+) |
| NextSortedRunID | `uint32` | Next run ID to assign |

| Method | Signature |
|--------|-----------|
| Clone | `() *Manifest` |
| L0SSTCount | `() int` |
| SortedRunCount | `() int` |
| LookupSST | `(id string) *SSTMeta` |
| GetSortedRun | `(id uint32) *SortedRun` |
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
| Level | `int` | LSM level (0 = L0) |
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

### SortedRun

```go
type SortedRun = manifest.SortedRun
```

| Field | Type |
|-------|------|
| ID | `uint32` |
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
| AppendAddSSTableWithFence | `(ctx, SSTMeta) (*ManifestLogEntry, error)` | Append SST add entry |
| AppendAddSSTableWithChangeBatchWithFence | `(ctx, SSTMeta, *ChangeBatchMeta) (*ManifestLogEntry, error)` | Append paired SST and change-batch entry |
| AppendRemoveSSTablesWithFence | `(ctx, []string) (*ManifestLogEntry, error)` | Append SST remove entry |
| AppendCompactionWithFence | `(ctx, CompactionLogPayload) (*ManifestLogEntry, error)` | Append compaction entry |
| WriteSnapshot | `(ctx, *Manifest) (string, error)` | Write manifest snapshot |

**Errors:**
- `manifest.ErrFenced` - Epoch superseded by newer owner
- `manifest.ErrFenceConflict` - Concurrent claim detected
