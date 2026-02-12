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
defer w.Close()
w.Put([]byte("key"), []byte("value"))
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
| Close | `() error` |

```go
type DBOptions struct {
    ManifestStorage manifest.Storage // Optional custom manifest storage backend
    GCMarkStorage   manifest.GCMarkStorage // Optional custom GC mark storage backend
}
```

---

### Writer

Provides write access to the database. Buffers writes in a memtable and flushes to SSTs.

| Method | Signature | Description |
|--------|-----------|-------------|
| Put | `(key, value []byte) error` | Write a key-value pair |
| PutWithTTL | `(key, value []byte, ttl time.Duration) error` | Write with time-to-live |
| Delete | `(key []byte) error` | Mark a key as deleted |
| DeleteWithTTL | `(key []byte, ttl time.Duration) error` | Delete with TTL |
| Flush | `(ctx context.Context) error` | Force flush memtable to SST |
| Close | `() error` | Close the writer |

```go
type WriterOptions struct {
    MemtableSize           int64                    // Memtable size before flush
    FlushInterval          time.Duration            // Auto-flush interval
    BloomBitsPerKey        int                      // Bloom filter bits per key
    BlockSize              int                      // SST block size
    Compression            string                   // Compression algorithm
    MaxImmutableMemtables  int                      // Max pending memtables before backpressure
    OnFlushError           func(error)              // Flush error callback
    ValueStorage           config.ValueStorageConfig // Value storage configuration
    OwnerID                string                   // Writer owner identifier
}

func DefaultWriterOptions() WriterOptions
```

**Errors:**
- `ErrBackpressure` - Writer hit memtable queue limit, caller should retry after a delay.

---

### Reader

Read-only handle for database access. Supports point lookups, range scans, and iteration.

```go
func OpenReader(ctx context.Context, store *blobstore.Store, opts ReaderOpenOptions) (*Reader, error)
```

| Method | Signature | Description |
|--------|-----------|-------------|
| Get | `(ctx context.Context, key []byte) ([]byte, bool, error)` | Retrieve value for key |
| Scan | `(ctx context.Context, minKey, maxKey []byte) ([]KV, error)` | Scan a key range |
| ScanLimit | `(ctx context.Context, minKey, maxKey []byte, limit int) ([]KV, error)` | Scan with result limit |
| NewIterator | `(ctx context.Context, opts IteratorOptions) (*Iterator, error)` | Create bounded iterator |
| Refresh | `(ctx context.Context) error` | Reload manifest and invalidate removed SSTs |
| Manifest | `() *Manifest` | Return cloned manifest snapshot |
| ManifestUnsafe | `() *Manifest` | Return manifest without copying |
| Close | `() error` | Close reader and caches |
| BlobCacheStats | `() internal.BlobCacheStats` | Blob cache statistics |
| SSTCacheStats | `() SSTCacheStats` | SST cache statistics |

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

---

### TailingReader

Reader with periodic background manifest refresh and the ability to tail for new keys.

```go
func OpenTailingReader(ctx context.Context, store *blobstore.Store, opts TailingReaderOpenOptions) (*TailingReader, error)
```

| Method | Signature | Description |
|--------|-----------|-------------|
| Start | `() error` | Start background refresh loop |
| Stop | `()` | Stop background refresh |
| Close | `() error` | Stop and close underlying reader |
| Refresh | `(ctx context.Context) error` | Manually refresh manifest |
| LastRefresh | `() time.Time` | Time of last refresh |
| Get | `(ctx context.Context, key []byte) ([]byte, bool, error)` | Retrieve value |
| Scan | `(ctx context.Context, minKey, maxKey []byte) ([]KV, error)` | Scan key range |
| ScanLimit | `(ctx context.Context, minKey, maxKey []byte, limit int) ([]KV, error)` | Scan with limit |
| NewIterator | `(ctx context.Context, opts IteratorOptions) (*Iterator, error)` | Create iterator |
| Manifest | `() *Manifest` | Get manifest snapshot |
| Reader | `() *Reader` | Access underlying Reader |
| Tail | `(ctx context.Context, opts TailOptions, handler func(KV) error) error` | Continuously scan for new keys |
| TailChannel | `(ctx context.Context, opts TailOptions) (<-chan KV, <-chan error)` | Tail as channels |

```go
type TailingReaderOpenOptions struct {
    RefreshInterval time.Duration
    OnRefresh       func()
    OnRefreshError  func(error)
    ReaderOptions   ReaderOpenOptions
}

func DefaultTailingReaderOpenOptions() TailingReaderOpenOptions
```

```go
type TailOptions struct {
    MinKey        []byte        // Inclusive lower bound
    MaxKey        []byte        // Inclusive upper bound
    StartAfterKey []byte        // Resume from next key after this value
    PollInterval  time.Duration // Check frequency for new keys
}
```

**Errors:**
- `ErrTailingReaderStopped` - Tailing reader has been stopped.

---

### Compactor

Merges L0 SSTs into sorted runs and merges consecutive similar runs in the background.

| Method | Signature | Description |
|--------|-----------|-------------|
| Start | `()` | Start background compaction loop |
| Stop | `()` | Stop background compaction |
| Close | `() error` | Close compactor |
| Refresh | `(ctx context.Context) error` | Reload manifest |
| RunCompaction | `(ctx context.Context) error` | Perform a compaction cycle |
| IsFenced | `() bool` | Check if compactor is fenced |
| FenceToken | `() *manifest.FenceToken` | Get fence token |

```go
type CompactorOptions struct {
    L0CompactionThreshold int                      // L0 SST count to trigger compaction
    MinSources            int                      // Min sorted runs to merge
    MaxSources            int                      // Max sorted runs to merge
    SizeThreshold         int                      // Size similarity threshold
    BloomBitsPerKey       int                      // Bloom filter bits per key
    BlockSize             int                      // SST block size
    Compression           string                   // Compression algorithm
    TargetSSTSize         int64                    // Target SST file size
    ValidateSSTChecksum   bool                     // Verify SST checksums
    SSTHashVerifier       SSTHashVerifier          // SST signature verifier
    CheckInterval         time.Duration            // Compaction check interval
    OnCompactionStart     func(CompactionJob)      // Compaction start callback
    OnCompactionEnd       func(CompactionJob, error) // Compaction end callback
    OwnerID               string                   // Compactor owner identifier
    GCMarkStorage         manifest.GCMarkStorage   // Optional custom GC mark storage backend
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
| Start | `()` | Start background cleanup loop |
| Stop | `()` | Stop background cleanup |
| Close | `() error` | Close retention compactor |
| Refresh | `(ctx context.Context) error` | Reload manifest |
| RunCleanup | `(ctx context.Context) error` | Perform a cleanup cycle |
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

Manages the manifest log and snapshots with fencing support.

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
| Replay | `(ctx) (*Manifest, error)` | Rebuild manifest from log |
| AppendAddSSTableWithFence | `(ctx, SSTMeta) (*ManifestLogEntry, error)` | Append SST add entry |
| AppendRemoveSSTablesWithFence | `(ctx, []string) (*ManifestLogEntry, error)` | Append SST remove entry |
| AppendCompactionWithFence | `(ctx, CompactionLogPayload) (*ManifestLogEntry, error)` | Append compaction entry |
| WriteSnapshot | `(ctx, *Manifest) (string, error)` | Write manifest snapshot |

**Errors:**
- `manifest.ErrFenced` - Epoch superseded by newer owner
- `manifest.ErrFenceConflict` - Concurrent claim detected
