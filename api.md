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
    Prefix     string
    ChangeFeed *ChangeFeedOptions // nil leaves a new feed disabled
    SSTOutput  SSTOutputOptions   // zero fields select production defaults
    Policy     StorePolicy
}

type SSTOutputOptions struct {
    L0        SSTEncodingOptions // writer flush output
    Compacted SSTEncodingOptions // maintenance compaction output
}

type SSTEncodingOptions struct {
    Compression     string // "none", "snappy", or "zstd"
    BlockBytes      int
    BloomBitsPerKey int
}

func DefaultSSTOutputOptions() SSTOutputOptions

type ChangeFeedPayload uint8

const (
    ChangeFeedKeysOnly ChangeFeedPayload = iota + 1
    ChangeFeedFullValues
)

type ChangeFeedOptions struct {
    Payload ChangeFeedPayload
}

type StorePolicy struct {
    MaxPinnedViewAge time.Duration // Default: 1 hour
}
```

`Open` owns the bucket connection and closes it from `DB.Close`. `OpenBucket`
borrows an existing Go Cloud bucket and leaves its lifecycle with the caller.
The change-feed payload is explicit, persisted in the manifest, and cannot be
changed or disabled after enablement. `ChangeFeedKeysOnly` is an invalidation
feed; `ChangeFeedFullValues` is replayable CDC. Enabling an existing database
starts the feed at its current manifest head. Reopening with `ChangeFeed == nil`
adopts an already-persisted configuration.
`MaxPinnedViewAge` is persisted by the first writer. Later writers must present
the same value. Readers and SST garbage collection both derive their safety
deadline from this one store policy.

`SSTOutput` is the single encoding policy used by all writers and maintenance
handles opened from the `DB`. It affects only newly created SSTs and is not
persisted in `manifest/CURRENT`. SST files are self-describing, so readers can
open a mixture of old and new encodings without receiving this configuration.
The current zero-value default is Snappy compression, 4 KiB data blocks, and
10 Bloom-filter bits per key for both L0 and compacted SSTs.

For example, foreground flushes can remain inexpensive while maintenance
produces denser, larger-block SSTs:

```go
SSTOutput: isledb.SSTOutputOptions{
    L0: isledb.SSTEncodingOptions{
        Compression:     "snappy",
        BlockBytes:      4 << 10,
        BloomBitsPerKey: 10,
    },
    Compacted: isledb.SSTEncodingOptions{
        Compression:     "zstd",
        BlockBytes:      16 << 10,
        BloomBitsPerKey: 10,
    },
},
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

type ValueOptions struct {
    MaxKeyBytes   int   // Largest accepted key size. Default: 64 KiB.
    MaxValueBytes int64 // Largest accepted value size. Default: 16 MiB.
}

func DefaultWriterOptions() WriterOptions
```

**Errors:**
- `ErrBackpressure` - writer hit `Memtable.MaxPendingMemtables`; caller should retry after a delay or flush.
- `ErrInvalidWriterOptions` - writer configuration contains a negative size or
  interval, oversized identity, invalid value limit, or a memtable
  configuration that exceeds the arena format limit.
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
| SSTCacheStats | `() CacheStats` | SST cache statistics |
| ManifestPageCacheStats | `() CacheStats` | Manifest commit-page cache statistics |

```go
type ReaderOpenOptions struct {
    CacheDir                 string               // Required disk cache directory.
    SSTCacheSize             int64                // Default: 1GB
    BlockCacheSize           int64                // Range-read block cache (0 = disabled)
    AllowUnverifiedRangeRead bool                 // Allow range reads without checksum verification
    RangeReadMinSSTSize      int64                // Minimum SST size for range-read optimization
    ValidateSSTChecksum      bool                 // Verify SST checksums on read
    SSTHashVerifier          SSTHashVerifier      // SST signature verifier
    Views                    ReaderViewPolicy     // Manifest freshness policy
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
after `DBOptions.ChangeFeed` has been persisted for the database prefix.
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
    Value     []byte          // nil when omitted or for deletes
    HasValue  bool            // true for full-value PUTs, including empty values
    ExpiresAt time.Time       // zero when no TTL applies
}

type ChangeBounds struct {
    Oldest ChangeCursor
    Head   ChangeCursor
    Payload ChangeFeedPayload
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
In keys-only mode, PUT records have `HasValue == false`; a later KV `Get` returns
current state and is not a historical reconstruction of that mutation.

`Read` performs no polling. An empty page can still advance `Next` over
manifest entries that contain no user mutations; continue until `CaughtUp`
returns true. When retention has passed a saved cursor, `Read` returns
`ErrChangeCursorExpired`.

Each `ChangeReader` owns its manifest view. A `Read` beginning at a manifest
entry boundary refreshes `manifest/CURRENT`, then reads from that immutable
view. Change batches are indexed collections of independently compressed and
checksummed blocks. `Read` range-fetches only the blocks needed by
`MaxChanges` and `MaxBytes`; continuation reads can reuse decoded blocks from a
bounded 16 MiB cache without another `CURRENT` or manifest-page read. The next
call beginning at an entry boundary refreshes the view and revalidates the
retention floor.

### Maintenance

Owns one fenced maintenance session for a database prefix. It runs compaction,
optional change-feed retention, checkpoints, and garbage collection through a
bounded fair scheduler. `DB.OpenMaintenance` rejects a second active
maintenance handle opened from the same `DB`.

| Method | Signature | Description |
|--------|-----------|-------------|
| Run | `(ctx context.Context) error` | Run cycles until cancellation, fencing, or `Close` |
| RunOnce | `(ctx context.Context) (MaintenanceStats, error)` | Perform one deterministic control pass and one bounded pass from each reclaimer |
| Close | `(ctx context.Context) error` | Stop scheduling, wait for active work, and release ownership |

```go
type MaintenanceOptions struct {
    IdleInterval        time.Duration
    SSTCompaction       SSTCompactionOptions
    ManifestCheckpoint ManifestCheckpointOptions
    ChangeFeedRetention *ChangeFeedRetentionOptions
    Reclamation         ReclamationOptions
    OnCycle             func(MaintenanceStats)
    OnReclamationCycle  func(ReclamationCycleStats)
    OnError             func(error)
}

type ReclamationOptions struct {
    MaxConcurrentDeletes int
    SST                  DeleterOptions
    ChangeFeed           DeleterOptions
    Manifest             ManifestDeleterOptions
}

type DeleterOptions struct {
    PollInterval      time.Duration
    MaxObjectsPerPass int
}

type ManifestDeleterOptions struct {
    DeleterOptions
    AuditInterval time.Duration
}

type ChangeFeedRetentionOptions struct {
    RetainFor time.Duration
}

type SSTCompactionOptions struct {
    ReadConcurrency     int
    L0TriggerSSTs       int
    BaseLevelBytes      int64
    LevelGrowthFactor   int
    MaxInputSSTsPerJob  int
    MaxInputBytesPerJob int64
    TargetSSTBytes      int64
}

type ManifestCheckpointOptions struct {
    TargetReplayPages uint64
    TargetReplayBytes uint64
}

type MaintenanceStats struct {
    State                   MaintenanceState
    Scheduling              MaintenanceScheduleStats
    SSTCompaction           SSTCompactionStats
    SSTCleanup              SSTCleanupStats
    ChangeFeedRetention     ChangeFeedCleanupStats
    ManifestCheckpoint      ManifestCheckpointStats
    ManifestCleanup         ManifestCleanupStats
    Duration                time.Duration
}

type ReclamationCycleStats struct {
    Family     ReclamationFamily
    SST        SSTCleanupStats
    ChangeFeed ChangeFeedCleanupStats
    Manifest   ManifestCleanupStats
    Duration   time.Duration
}

type ManifestCleanupStats struct {
    Snapshots ManifestSnapshotCleanupStats
    Pages     ManifestPageCleanupStats
}

type SSTCleanupStats struct {
    SSTsPlanned    int
    PlansPrepared  int
    PlansScanned   int
    PlansCompleted int
    DeleteAttempts int
    SSTsDeleted    int
    DeferredPlans  int
    Failures       int
}

type ManifestSnapshotCleanupStats struct {
    SnapshotsMarked  int
    DeleteAttempts   int
    SnapshotsDeleted int
    Protected        int
    Deferred         int
    Failures         int
    MarkersScanned   int
    MarkersCleared   int
    ObjectsScanned   int
    Duration         time.Duration
}

type ManifestPageCleanupStats struct {
    PagesMarked      int
    PagesDeleted     int
    Protected        int
    Deferred         int
    Failures         int
    MarkersScanned   int
    MarkersCleared   int
    ObjectsScanned   int
    DeleteAttempts   int
    ReachabilityGETs int
    Duration         time.Duration
}

type MaintenanceScheduleStats struct {
    Selected              MaintenanceTask
    CompactionSourceLevel uint32
    CompactionWorkUnits   uint32
    CompactionCritical    bool
    CheckpointEligible    bool
    CheckpointUrgent      bool
    ReplayPages           uint64
    ReplayBytes           uint64
}

type MaintenanceState uint8

const (
    MaintenanceIdle MaintenanceState = iota
    MaintenanceWaitingForWriter
)

type SSTCompactionStats struct {
    Jobs        int
    InputSSTs   int
    OutputSSTs  int
    OutputBytes int64
}

type ManifestCheckpointStats struct {
    Staged      bool
    ReplayPages uint64
    ReplayBytes uint64
}

type ChangeFeedCleanupStats struct {
    EntriesRetired  int
    BatchesPlanned  int
    BatchesDeleted  int
    BlockedRetained int
    FailedDeletes   int
    Duration        time.Duration
}
```

`DefaultMaintenanceOptions` enables compaction, checkpoints, and three physical
reclamation lanes. `Maintenance.Run` executes the serialized control lane
independently from SST, change-feed, and manifest-metadata reclaimers. All
three reclaimers share `MaxConcurrentDeletes`, retain provider listing cursors
between bounded passes, and cannot change logical visibility. Scheduler burst
limits and deletion safety margins remain internal. Change-feed retention
remains disabled. Use
`DefaultChangeFeedRetentionOptions` before enabling feed-history cleanup.

Change-feed retention requires an enabled feed and runs only while maintenance
runs. The setting is runtime maintenance configuration, not part of the
persisted immutable payload choice. Omitting it preserves feed history
indefinitely.

`MaintenanceWaitingForWriter` means the cycle staged a command in the
maintenance mailbox. The active writer must publish or reject it before the
next maintenance command can be staged.

Checkpoint snapshot cleanup is automatic. An
applied checkpoint durably marks its previous snapshot as retired before the
mailbox command is cleared; a rejected checkpoint marks its candidate instead.
Deletion waits for the persisted maximum pinned-view age plus an internal
safety margin, rechecks both `CURRENT` and a pending checkpoint, and is bounded
per pass. A periodic bounded audit finds candidates created before a crash or
an ambiguous staging failure.

Manifest pages use per-page quarantine markers. A candidate must be
structurally valid and unreachable from fresh `CURRENT`. Physical deletion
also requires monotonic proof that a paused publication cannot later succeed:
the page's complete sequence range is below the committed retained-entry
floor. The deleter waits through the pinned-view deadline and orphan grace,
reloads `CURRENT`, traverses only the candidate's containing index branch, and
fails closed on corruption or ambiguous overlap.

```go
opts := isledb.DefaultMaintenanceOptions()
opts.IdleInterval = 5 * time.Second
opts.SSTCompaction.L0TriggerSSTs = 8
opts.SSTCompaction.ReadConcurrency = 4
opts.SSTCompaction.MaxInputBytesPerJob = 512 << 20
opts.SSTCompaction.TargetSSTBytes = 64 << 20
opts.ManifestCheckpoint.TargetReplayPages = 64
opts.ManifestCheckpoint.TargetReplayBytes = 32 << 20
opts.Reclamation.MaxConcurrentDeletes = 4
opts.Reclamation.SST.MaxObjectsPerPass = 128
opts.Reclamation.Manifest.AuditInterval = time.Hour

feedRetention := isledb.DefaultChangeFeedRetentionOptions()
feedRetention.RetainFor = 24 * time.Hour
opts.ChangeFeedRetention = &feedRetention

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
