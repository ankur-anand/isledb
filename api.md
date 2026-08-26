# IsleDB Go API Guide

This guide documents the public API most applications use. It focuses on
behavior that affects correctness: durability, visibility, handle ownership,
read-view lifetime, change-feed cursors, and maintenance running in a separate
process.

```go
import "github.com/ankur-anand/isledb"
```

## Contents

- [Mental model](#mental-model)
- [Quick start](#quick-start)
- [Open and close a database](#open-and-close-a-database)
- [SST output policy](#sst-output-policy)
- [Write key-value data](#write-key-value-data)
- [Read key-value data](#read-key-value-data)
- [Enable and consume the change feed](#enable-and-consume-the-change-feed)
- [Run maintenance separately](#run-maintenance-separately)
- [Prometheus metrics](#prometheus-metrics)
- [Error reference](#error-reference)
- [Advanced blobstore package](#advanced-blobstore-package)

## Mental model

An IsleDB database is one object-store bucket or container plus a prefix. The
`DB` value owns the local runtime for that prefix.

| Handle | Per `DB` | Concurrency contract | Purpose |
|---|---:|---|---|
| `Writer` | One | Serialize calls made to one writer | Buffer and publish KV mutations |
| `Reader` | One | Safe for concurrent reads | Serve point and range reads |
| `ChangeReader` | Any number | Safe for concurrent use; callers own cursors | Consume the durable mutation feed |
| `Maintenance` | One | `Close` may run concurrently with `Run` or `RunOnce` | Compact, checkpoint, retain, and reclaim |

Writer and maintenance ownership are also fenced through the object store, so
different processes cannot safely act as the same owner at the same time.
Readers and change readers can scale independently by opening the same database
prefix from additional processes.

The main visibility rule is simple:

```text
Put / Delete
    -> buffered in the writer
    -> Flush, background flush, or Close
    -> committed to the manifest
    -> visible to a newly opened or refreshed reader
```

## Quick start

The example disables timed background flushing and uses an explicit `Flush` as
the durability and visibility boundary.

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/ankur-anand/isledb"
)

func main() {
    if err := run(context.Background()); err != nil {
        log.Fatal(err)
    }
}

func run(ctx context.Context) error {
    db, err := isledb.Open(
        ctx,
        "s3://my-bucket?region=us-east-1",
        isledb.DBOptions{Prefix: "accounts"},
    )
    if err != nil {
        return err
    }
    defer func() { _ = db.Close() }()

    writerOptions := isledb.DefaultWriterOptions()
    writerOptions.Flush.Interval = 0

    writer, err := db.OpenWriter(ctx, writerOptions)
    if err != nil {
        return err
    }
    if err := writer.Put(ctx, []byte("user:1"), []byte("Ankur")); err != nil {
        return err
    }
    if err := writer.Flush(ctx); err != nil {
        return err
    }
    if err := writer.Close(ctx); err != nil {
        return err
    }

    reader, err := db.OpenReader(
        ctx,
        isledb.DefaultReaderOpenOptions("./isledb-cache"),
    )
    if err != nil {
        return err
    }
    defer func() { _ = reader.Close() }()

    value, found, err := reader.Get(ctx, []byte("user:1"))
    if err != nil {
        return err
    }
    if found {
        fmt.Printf("user:1 = %s\n", value)
    }
    return nil
}
```

`Open` uses [Go Cloud bucket URLs](https://gocloud.dev/howto/blob/). Typical
schemes include `s3://`, `gs://`, `azblob://`, and `file://`. Provider
credentials come from the corresponding Go Cloud driver and cloud SDK.

## Open and close a database

```go
func Open(
    ctx context.Context,
    bucketURL string,
    opts DBOptions,
) (*DB, error)

func OpenBucket(
    ctx context.Context,
    bucket *blob.Bucket,
    bucketName string,
    opts DBOptions,
) (*DB, error)
```

`Open` creates and owns the Go Cloud bucket connection. `DB.Close` closes that
connection. `OpenBucket` borrows an existing `*blob.Bucket`; the caller remains
responsible for closing it.

```go
type DBOptions struct {
    Prefix     string
    ChangeFeed *ChangeFeedOptions
    SSTOutput  SSTOutputOptions
    Policy     StorePolicy
}

type StorePolicy struct {
    MaxPinnedViewAge time.Duration
}
```

`Prefix` is the database root inside the bucket. Use a dedicated prefix for
each database.

`MaxPinnedViewAge` is the longest time a loaded manifest view may remain usable.
Zero selects `DefaultMaxPinnedViewAge`, currently one hour. The first writer
persists this policy. Later writers must present the same value or opening the
writer fails with `ErrStorePolicyMismatch`. KV read views and physical
reclamation deadlines—including change-feed reclamation—use this policy to
agree on when an old view can no longer refer to retired objects.

### Database methods

```go
func (db *DB) OpenWriter(ctx context.Context, opts WriterOptions) (*Writer, error)
func (db *DB) OpenReader(ctx context.Context, opts ReaderOpenOptions) (*Reader, error)
func (db *DB) OpenChangeReader(ctx context.Context) (*ChangeReader, error)
func (db *DB) OpenMaintenance(ctx context.Context, opts MaintenanceOptions) (*Maintenance, error)
func (db *DB) Close() error
```

`DB.Close` closes handles still registered with that `DB`. Close application
handles explicitly when their errors matter; use `DB.Close` as the final
process-level cleanup.

## SST output policy

SST encoding is runtime output policy. It affects newly written files and is
not persisted in `manifest/CURRENT`. Existing files are self-describing, so a
reader can read a mixture of encodings.

```go
type SSTOutputOptions struct {
    L0        SSTEncodingOptions
    Compacted SSTEncodingOptions
}

type SSTEncodingOptions struct {
    Compression     string
    BlockBytes      int
    BloomBitsPerKey int
}

func DefaultSSTOutputOptions() SSTOutputOptions
```

Supported compression values are `"none"`, `"snappy"`, and `"zstd"`.
Zero fields select the current defaults:

| SST class | Compression | Data block target | Bloom bits/key |
|---|---|---:|---:|
| Writer L0 output | Snappy | 4 KiB | 10 |
| Compacted output | Snappy | 4 KiB | 10 |

Writer flushes and maintenance may use different settings:

```go
dbOptions := isledb.DBOptions{
    Prefix: "accounts",
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
}
```

Separate writer and maintenance processes each receive their own `DBOptions`.
They may safely use different output settings, although using one deployment
configuration makes performance more predictable.

## Write key-value data

### Writer methods

```go
func (w *Writer) Put(ctx context.Context, key, value []byte) error
func (w *Writer) PutWithTTL(ctx context.Context, key, value []byte, ttl time.Duration) error
func (w *Writer) Delete(ctx context.Context, key []byte) error
func (w *Writer) Flush(ctx context.Context) error
func (w *Writer) Close(ctx context.Context) error
```

- `Put`, `PutWithTTL`, and `Delete` return after buffering the mutation locally.
- `ttl == 0` means no expiration. Negative TTL values are rejected with
  `ErrInvalidMutation`.
- Empty or oversized keys and oversized values also return an error wrapping
  `ErrInvalidMutation`, allowing callers to avoid retrying invalid input.
- Expired values are filtered by readers. TTL expiration is not an immediate
  object deletion operation.
- `Flush` publishes all currently buffered and frozen memtables.
- A successful background flush also publishes buffered data.
- `Close` stops background flushing and flushes pending writes.

One writer uses internal locks for correctness, but public call ordering is not
defined under concurrent use. Serialize `Put`, `PutWithTTL`, `Delete`, `Flush`,
and `Close` for one writer.

### Writer options and defaults

```go
type WriterOptions struct {
    OwnerID       string
    Memtable      WriterMemtableOptions
    Flush         WriterFlushOptions
    Maintenance   WriterMaintenanceOptions
    Values        ValueOptions
    OnFlushError  func(error)
    Metrics       *WriterMetrics
}

type WriterMemtableOptions struct {
    TargetBytes         int64
    MaxPendingMemtables int
}

type WriterFlushOptions struct {
    Interval time.Duration
}

type WriterMaintenanceOptions struct {
    PollInterval time.Duration
}

type ValueOptions struct {
    MaxKeyBytes   int
    MaxValueBytes int64
}

func DefaultWriterOptions() WriterOptions
```

`DefaultWriterOptions` returns:

| Option | Default | Meaning |
|---|---:|---|
| `OwnerID` | Generated | Stable identity stored in the writer fence |
| `Memtable.TargetBytes` | 16 MiB | Approximate active memtable rotation target |
| `Memtable.MaxPendingMemtables` | 4 | Queued or flushing memtables before backpressure |
| `Flush.Interval` | 1 second | Timed background flush cadence |
| `Maintenance.PollInterval` | 1 second | Mailbox polling in a separate writer process |
| `Values.MaxKeyBytes` | 64 KiB | Largest accepted key |
| `Values.MaxValueBytes` | 16 MiB | Largest accepted value |
| `OnFlushError` | `nil` | Optional terminal background-error callback |
| `Metrics` | `nil` | Optional Prometheus observations |

`Flush.Interval` is intentionally different from most zero-valued options:
setting it to zero disables timed background flushing. Call
`DefaultWriterOptions` first when you want all defaults.

When `MaxPendingMemtables` is reached, a mutation that would require another
rotation returns `ErrBackpressure` before accepting the mutation. Retry after a
delay or call `Flush` from the serialized writer owner.

The first background flush error makes the writer terminal. `OnFlushError` is
called once after the background worker stops; later mutations, `Flush`, and
`Close` return `ErrWriterFailed` wrapping the original error. A synchronous
`Flush` error is returned directly and remains retryable.

## Read key-value data

### Open a reader

```go
type ReaderOpenOptions struct {
    CacheDir                 string
    SSTCacheSize             int64
    BlockCacheSize           int64
    BloomCacheSize           int64
    AllowUnverifiedRangeRead bool
    RangeReadMinSSTSize      int64
    ValidateSSTChecksum      bool
    Views                    ReaderViewPolicy
    Metrics                  *ReaderMetrics
}

type ReaderViewPolicy struct {
    RefreshAfter time.Duration
}

func DefaultReaderOpenOptions(cacheDir string) ReaderOpenOptions
```

`CacheDir` is required. `DefaultReaderOpenOptions` returns:

| Option | Default | Meaning |
|---|---:|---|
| `SSTCacheSize` | 1 GiB | Maximum on-disk SST cache size |
| `BlockCacheSize` | 0 | In-memory range-read block cache disabled |
| `BloomCacheSize` | 64 MiB | Maximum accounted size of decoded SST bloom filters |
| `RangeReadMinSSTSize` | 0 | No minimum SST size |
| `ValidateSSTChecksum` | `false` | Do not hash full SST downloads |
| `AllowUnverifiedRangeRead` | `false` | Do not bypass requested full-file validation |
| `Views.RefreshAfter` | 1 minute | Refresh a loaded manifest before a later read |
| `Metrics` | `nil` | Optional Prometheus observations |

Every newly written SST records a SHA-256 checksum. With
`ValidateSSTChecksum`, the reader validates the full SST on its first download.
Range reads require `BlockCacheSize > 0`. If checksum validation is enabled,
the reader uses a full download unless `AllowUnverifiedRangeRead` explicitly
permits range reads without validating the full-file checksum.

### Reader methods

```go
func (r *Reader) Refresh(ctx context.Context) error
func (r *Reader) Get(ctx context.Context, key []byte) ([]byte, bool, error)
func (r *Reader) Scan(ctx context.Context, minKey, maxKey []byte) ([]KV, error)
func (r *Reader) ScanLimit(ctx context.Context, minKey, maxKey []byte, limit int) ([]KV, error)
func (r *Reader) NewIterator(ctx context.Context, opts IteratorOptions) (*Iterator, error)
func (r *Reader) Snapshot(ctx context.Context) (*Snapshot, error)
func (r *Reader) BootstrapView(ctx context.Context) (*BootstrapView, error)
func (r *Reader) Prefetch(ctx context.Context, opts PrefetchOptions) (PrefetchStats, error)
func (r *Reader) SSTCacheStats() CacheStats
func (r *Reader) BloomCacheStats() CacheStats
func (r *Reader) ManifestPageCacheStats() CacheStats
func (r *Reader) Close() error
```

`Get` returns `found == false` for a missing, deleted, or expired key.

Every reader range is half-open: `[minKey, maxKey)`. This includes `Scan`,
`ScanLimit`, `IteratorOptions`, `Snapshot.ScanLimit`, snapshot iterators, and
`PrefetchOptions.Range`. A nil or empty bound leaves that side unbounded.

Because the upper bound is exclusive, `PrefixRange(prefix)` can be passed
directly to any range API without admitting the first key after the prefix.

`Scan` allocates and returns the complete result. `ScanLimit` also materializes
its result, but stops after a positive `limit`. A zero or negative limit has the
current API meaning of no limit. Prefer an iterator when the result can be
large.

```go
type KV struct {
    Key   []byte
    Value []byte
}

type IteratorOptions struct {
    MinKey []byte // Inclusive; nil means beginning
    MaxKey []byte // Exclusive; nil means end
}
```

### Iterate without materializing the range

```go
iter, err := reader.NewIterator(ctx, isledb.IteratorOptions{
    MinKey: []byte("user:"),
    MaxKey: []byte("user;"),
})
if err != nil {
    return err
}
defer func() { _ = iter.Close() }()

for iter.Next() {
    key := iter.Key()
    value := iter.Value()
    _ = key
    _ = value
}
if err := iter.Err(); err != nil {
    return err
}
```

Iterator methods are:

```go
func (it *Iterator) Next() bool
func (it *Iterator) SeekGE(target []byte) bool
func (it *Iterator) Key() []byte
func (it *Iterator) Value() []byte
func (it *Iterator) Valid() bool
func (it *Iterator) Err() error
func (it *Iterator) Close() error
```

### Consistent snapshots

A snapshot pins one loaded manifest view. It does not refresh when its parent
reader refreshes.

```go
type Version struct { /* opaque */ }

func (v Version) String() string
func (v Version) IsZero() bool

func (s *Snapshot) Version() Version
func (s *Snapshot) Get(ctx context.Context, key []byte) ([]byte, bool, error)
func (s *Snapshot) ScanLimit(ctx context.Context, minKey, maxKey []byte, limit int) ([]KV, error)
func (s *Snapshot) NewIterator(ctx context.Context, opts IteratorOptions) (*Iterator, error)
func (s *Snapshot) Close() error
```

Snapshots and their iterators inherit the absolute deadline of the loaded view.
Creating another handle does not extend that deadline. An expired operation
returns `ErrSnapshotExpired`, `ErrIteratorExpired`, or `ErrReadViewExpired`.
Refresh or create a new snapshot instead of retrying against the expired view.

Closing the parent reader invalidates its snapshots and iterators.

### Materialize state and resume the change feed

`BootstrapView` binds a KV snapshot to the exact change-feed boundary from the
same loaded `CURRENT`:

```go
type BootstrapView struct {
    Snapshot *Snapshot
    Cursor   ChangeCursor
    Version  Version
}

func (r *Reader) BootstrapView(ctx context.Context) (*BootstrapView, error)
```

`Snapshot` contains every mutation committed before `Cursor`. `Cursor` is the
first feed position not represented by the snapshot. `Version` is the same
opaque value returned by `Snapshot.Version()` and can be recorded in
checkpoint metadata for diagnostics.

This lets an application scan the snapshot into a local materialized database,
store `Cursor` with that database, and then consume only later changes. Do not
construct this boundary by calling `Snapshot()` and `ChangeReader.Bounds()`
separately: a writer may publish between those calls, producing a cursor newer
than the snapshot and skipping a committed change.

```go
view, err := reader.BootstrapView(ctx)
if err != nil {
    return err
}
defer view.Snapshot.Close()

iterator, err := view.Snapshot.NewIterator(ctx, isledb.IteratorOptions{})
if err != nil {
    return err
}
defer iterator.Close()

for iterator.Next() {
    if err := materialize(iterator.Key(), iterator.Value()); err != nil {
        return err
    }
}
if err := iterator.Err(); err != nil {
    return err
}

if err := saveCheckpointCursor(view.Cursor.String()); err != nil {
    return err
}
```

The change feed must be enabled or `BootstrapView` returns
`ErrChangeFeedDisabled`. The snapshot keeps the normal loaded-view deadline,
so the materialization must finish before it expires. Change-feed retention
must also keep `Cursor` available until the generated checkpoint is installed
and caught up. Like `Snapshot`, this method follows the reader freshness
policy; call `Refresh` first when the checkpoint must start from the latest
published `CURRENT`. Close `view.Snapshot` when materialization finishes.

### Prefetch selected SSTs

`Prefetch` uses the same half-open `KeyRange` contract as scans and iterators.

```go
type KeyRange struct {
    Min []byte // Inclusive; nil means beginning
    Max []byte // Exclusive; nil means end
}

func PrefixRange(prefix []byte) KeyRange

type PrefetchOptions struct {
    Range       KeyRange
    All         bool
    MaxSSTs     int
    MaxBytes    int64
    Concurrency int
}

type PrefetchStats struct {
    MatchedSSTs int
    CachedSSTs  int
    SkippedSSTs int
    BytesRead   int64
}
```

Use `All: true` to opt into prefetching the complete keyspace. A zero
`MaxSSTs` or `MaxBytes` means no limit.

```go
stats, err := reader.Prefetch(ctx, isledb.PrefetchOptions{
    Range:       isledb.PrefixRange([]byte("user:")),
    MaxBytes:    256 << 20,
    Concurrency: 4,
})
```

`Prefetch` applies the normal freshness policy. It does not force a manifest
refresh while the loaded view is still fresh; call `Refresh` first when an
immediate visibility check is required.

### Cache statistics

```go
type CacheStats struct {
	Hits                int64
	Misses              int64
	Bytes               int64
	MaxBytes            int64
	EntryCount          int
	MaxEntries          int
	PinnedBytes         int64
	PinnedEntries       int
	Evictions           int64
	Corruptions         int64
	AdmissionBypasses   int64
	SyncFailures        int64
	PublicationFailures int64
}
```

Byte-bounded caches report `MaxEntries == 0`. Entry-bounded caches report
`MaxBytes == 0`. `SyncFailures` counts verified fills served transiently after
their file sync failed. `PublicationFailures` counts failures while cleaning
capacity victims or publishing an artifact at its final cache path.

## Enable and consume the change feed

The change feed is optional. Enable it while opening the database:

```go
db, err := isledb.Open(ctx, bucketURL, isledb.DBOptions{
    Prefix: "accounts",
    ChangeFeed: &isledb.ChangeFeedOptions{
        Payload: isledb.ChangeFeedFullValues,
    },
})
```

```go
type ChangeFeedPayload uint8

const (
    ChangeFeedKeysOnly ChangeFeedPayload = iota + 1
    ChangeFeedFullValues
)

type ChangeFeedOptions struct {
    Payload ChangeFeedPayload
}
```

- `ChangeFeedKeysOnly` records operation, key, sequence, and expiry metadata.
  It is suitable for invalidation and “fetch current value” consumers.
- `ChangeFeedFullValues` also records PUT values and supports historical CDC
  replay.

The payload mode is persisted and immutable after enablement. Reopening with a
different mode returns `ErrChangeFeedPayloadMismatch`. Reopening with
`ChangeFeed == nil` adopts an already-enabled configuration. Enabling a feed on
an existing database starts it at the current manifest head; it does not invent
changes for older KV history.

### Change-feed types

```go
type ChangeOperation uint8

const (
    ChangePut ChangeOperation = iota + 1
    ChangeDelete
)

type Change struct {
    Sequence  uint64
    Operation ChangeOperation
    Key       []byte
    Value     []byte
    HasValue  bool
    ExpiresAt time.Time
}

type ChangeCursor struct { /* opaque */ }

func ParseChangeCursor(value string) (ChangeCursor, error)
func (c ChangeCursor) String() string
func (c ChangeCursor) IsZero() bool
func (c ChangeCursor) MarshalText() ([]byte, error)
func (c *ChangeCursor) UnmarshalText(text []byte) error

type ChangeBounds struct {
    Oldest  ChangeCursor
    Head    ChangeCursor
    Payload ChangeFeedPayload
}

type ChangePage struct {
    Changes []Change
    Next    ChangeCursor
    Head    ChangeCursor
}

func (p ChangePage) CaughtUp() bool
```

For a PUT in keys-only mode, `HasValue` is false and `Value` is nil. In
full-values mode, `HasValue` distinguishes an omitted value from a present but
empty value. Delete records do not carry values.

### Read bounded pages

```go
type ChangeReadOptions struct {
    MaxChanges int
    MaxBytes   int64
}

func DefaultChangeReadOptions() ChangeReadOptions

func (r *ChangeReader) Bounds(ctx context.Context) (ChangeBounds, error)
func (r *ChangeReader) Read(
    ctx context.Context,
    from ChangeCursor,
    opts ChangeReadOptions,
) (ChangePage, error)
func (r *ChangeReader) Close() error
```

The default page limits are 1,024 changes and 16 MiB. `MaxChanges` is capped at
65,536. `MaxBytes` counts key plus value bytes, not the Go allocation overhead
of the returned `[]Change`. A single change larger than `MaxBytes` is returned
alone so its cursor can make progress. Negative limits return
`ErrInvalidChangeReadOptions`; zero fields select defaults.

A cursor identifies the next change. It is an opaque resume position containing
a manifest-entry position and an index inside that entry's change batch. It is
not the same value as `Change.Sequence`, and the API does not currently seek by
`Change.Sequence`.

### Choose the initial cursor

`ChangeCursor.IsZero()` means that the application has no saved resume
position. It does not mean "start at the latest change." A new consumer must
choose its startup policy explicitly:

```go
bounds, err := reader.Bounds(ctx)
if err != nil {
    return err
}

// Replay every change that is still retained.
replayCursor := bounds.Oldest

// Ignore existing history and consume only changes published after Bounds.
tailCursor := bounds.Head
```

`Oldest` points to the first retained feed position. `Head` points immediately
after everything visible to `Bounds`; an initial read from `Head` normally
returns an empty, caught-up page. A commit that becomes visible after that
boundary is returned by a later `Read`. Passing a zero cursor directly to
`Read` starts from `Oldest`, but resolving `Bounds` makes the startup policy
explicit.

Persist the selected initial cursor before polling. Otherwise, a process that
crashes after choosing `Head` but before saving it may choose a newer head on
restart and silently skip changes. After processing starts, persist
`page.Next`, and only persist it after the complete page has been applied
successfully.

```go
func drainChanges(
    ctx context.Context,
    reader *isledb.ChangeReader,
    savedCursor string,
    startAtHead bool,
    apply func(isledb.Change) error,
    saveCursor func(string) error,
) error {
    cursor, err := isledb.ParseChangeCursor(savedCursor)
    if err != nil {
        return err
    }

    if cursor.IsZero() {
        bounds, err := reader.Bounds(ctx)
        if err != nil {
            return err
        }
        if startAtHead {
            cursor = bounds.Head
        } else {
            cursor = bounds.Oldest
        }
        if err := saveCursor(cursor.String()); err != nil {
            return err
        }
    }

    options := isledb.DefaultChangeReadOptions()
    for {
        page, err := reader.Read(ctx, cursor, options)
        if err != nil {
            return err
        }
        for _, change := range page.Changes {
            if err := apply(change); err != nil {
                return err
            }
        }
        if err := saveCursor(page.Next.String()); err != nil {
            return err
        }
        cursor = page.Next
        if page.CaughtUp() {
            return nil
        }
    }
}
```

`Read` does not poll or wait for future writes. An empty page may still advance
the cursor over manifest entries without user mutations. Continue until
`CaughtUp` is true, then let the application choose its polling interval.

When retention passes a saved cursor, `Read` refreshes its view and returns
`ErrChangeCursorExpired`. This means there is a gap between the saved position
and the oldest retained position. Restarting from `Bounds().Oldest` accepts and
skips that gap. A derived-state consumer can instead rebuild from a KV snapshot
and then resume the feed. A consumer that requires every historical event must
fail and require explicit recovery; a current-state snapshot cannot reconstruct
intermediate updates or deletes.

Change batches contain independently compressed and checksummed blocks. A read
range-fetches only the blocks needed for the requested page and reuses decoded
blocks from a bounded internal cache.

## Run maintenance separately

Maintenance is designed to run outside application reader and writer processes.
All processes open the same bucket and prefix:

```text
writer process       -> buffers writes and publishes manifest changes
reader processes     -> load immutable views and read independently
maintenance process  -> prepares compaction, checkpoints, and retention
reclamation workers  -> delete retired objects at bounded independent rates
```

Compaction, checkpointing, and logical change-feed retention stage fenced
commands. The active writer publishes or rejects those commands through its
normal `CURRENT` update path. A separate writer process discovers commands at
`WriterOptions.Maintenance.PollInterval`, which defaults to one second.

Physical SST, change-feed, snapshot, and manifest-page deletion proceeds in
independently paced reclamation lanes. Slow object deletion does not hold the
serialized control lane open.

### Maintenance API

```go
func (db *DB) OpenMaintenance(
    ctx context.Context,
    opts MaintenanceOptions,
) (*Maintenance, error)

func (m *Maintenance) Run(ctx context.Context) error
func (m *Maintenance) RunOnce(ctx context.Context) (MaintenanceStats, error)
func (m *Maintenance) Close(ctx context.Context) error
```

`Run` continues until its context is cancelled, `Close` is called, or the
maintenance fence is lost. `RunOnce` performs one deterministic control pass
and one bounded pass from each physical reclamation family.

Use a fresh shutdown context when `Run` returns. The run context is commonly
already cancelled:

```go
func runMaintenance(ctx context.Context, db *isledb.DB) error {
    options := isledb.DefaultMaintenanceOptions()

    maintenance, err := db.OpenMaintenance(ctx, options)
    if err != nil {
        return err
    }

    runErr := maintenance.Run(ctx)

    closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()
    closeErr := maintenance.Close(closeCtx)

    if errors.Is(runErr, context.Canceled) {
        runErr = nil
    }
    return errors.Join(runErr, closeErr)
}
```

For a scheduled job:

```go
stats, err := maintenance.RunOnce(ctx)
```

A `RunOnce` that returns `MaintenanceWaitingForWriter` has staged a command.
The writer must poll and publish or reject it. A later maintenance run observes
the receipt and continues. One scheduled invocation is therefore a bounded
unit of progress, not a promise to drain all maintenance backlog.

### Maintenance options and defaults

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

func DefaultMaintenanceOptions() MaintenanceOptions
```

Control-lane defaults:

| Option | Default |
|---|---:|
| `IdleInterval` | 5 seconds |
| `SSTCompaction.ReadConcurrency` | 4 |
| `SSTCompaction.L0TriggerSSTs` | 8 |
| `SSTCompaction.BaseLevelBytes` | 512 MiB |
| `SSTCompaction.LevelGrowthFactor` | 8 |
| `SSTCompaction.MaxInputSSTsPerJob` | 128 |
| `SSTCompaction.MaxInputBytesPerJob` | 512 MiB soft limit |
| `SSTCompaction.TargetSSTBytes` | 64 MiB |
| `ManifestCheckpoint.TargetReplayPages` | 64 pages |
| `ManifestCheckpoint.TargetReplayBytes` | 32 MiB |
| `ChangeFeedRetention` | `nil`, history retained indefinitely |

```go
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
```

One indivisible compaction plan may exceed `MaxInputBytesPerJob`.
`MaxInputSSTsPerJob` cannot exceed 128. Compacted SST encoding comes from
`DBOptions.SSTOutput.Compacted`.

Physical reclamation defaults:

| Lane | Poll interval | Maximum objects/pass |
|---|---:|---:|
| SST | 1 second | 128 |
| Change feed | 5 seconds | 128 |
| Manifest snapshots and pages | 1 minute | 128 |

The shared delete concurrency defaults to four. Manifest orphan auditing
defaults to once per hour.

```go
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
```

`MaxObjectsPerPass` bounds normal work. One already-bounded immutable SST
retirement plan may be completed atomically even when it exceeds the remaining
per-pass budget.

### Enable change-feed retention

Feed retention remains disabled until `ChangeFeedRetention` is non-nil.

```go
type ChangeFeedRetentionOptions struct {
    RetainFor time.Duration
}

func DefaultChangeFeedRetentionOptions() ChangeFeedRetentionOptions

options := isledb.DefaultMaintenanceOptions()
retention := isledb.DefaultChangeFeedRetentionOptions() // seven days
retention.RetainFor = 15 * 24 * time.Hour
options.ChangeFeedRetention = &retention
```

`RetainFor` is a minimum age, not an exact deletion time. Logical retention,
writer publication, pinned-view safety, and bounded physical deletion may keep
objects longer. Omitting retention preserves change history indefinitely.

### Maintenance state and statistics

```go
type MaintenanceState uint8

const (
    MaintenanceIdle MaintenanceState = iota
    MaintenanceWaitingForWriter
)

type MaintenanceTask uint8

const (
    MaintenanceTaskNone MaintenanceTask = iota
    MaintenanceTaskSSTCompaction
    MaintenanceTaskManifestCheckpoint
)

type ReclamationFamily string

const (
    ReclamationSST        ReclamationFamily = "sst"
    ReclamationChangeFeed ReclamationFamily = "change_feed"
    ReclamationManifest   ReclamationFamily = "manifest"
)
```

`MaintenanceState`, `MaintenanceTask`, `ChangeOperation`, and
`ChangeFeedPayload` implement `String`.

```go
type MaintenanceStats struct {
    State               MaintenanceState
    Scheduling          MaintenanceScheduleStats
    SSTCompaction       SSTCompactionStats
    SSTCleanup          SSTCleanupStats
    ChangeFeedRetention ChangeFeedCleanupStats
    ManifestCheckpoint  ManifestCheckpointStats
    ManifestCleanup     ManifestCleanupStats
    Duration            time.Duration
}

type ReclamationCycleStats struct {
    Family     ReclamationFamily
    SST        SSTCleanupStats
    ChangeFeed ChangeFeedCleanupStats
    Manifest   ManifestCleanupStats
    Duration   time.Duration
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

type SSTCompactionStats struct {
    Jobs        int
    InputSSTs   int
    OutputSSTs  int
    OutputBytes int64
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

type ManifestCleanupStats struct {
    Snapshots ManifestSnapshotCleanupStats
    Pages     ManifestPageCleanupStats
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
```

Use `OnCycle` for serialized control work and `OnReclamationCycle` for the
independent physical lanes.

## Prometheus metrics

```go
func DefaultWriterMetrics(constLabels prometheus.Labels) *WriterMetrics
func DefaultReaderMetrics(constLabels prometheus.Labels) *ReaderMetrics
```

Assign the returned values to `WriterOptions.Metrics` or
`ReaderOpenOptions.Metrics`. The constructors create Prometheus counters and
histograms but do not register them. Register the exported collectors with the
application's `prometheus.Registerer`.

Writer metrics cover puts, deletes, backpressure, flush count, errors, latency,
and bytes. Reader metrics cover refreshes, point reads, scans, SST cache use,
downloads, and range reads.

## Error reference

Use `errors.Is` for exported sentinel errors:

```go
if errors.Is(err, isledb.ErrBackpressure) {
    // Retry according to the application's admission policy.
}
```

### Database and configuration

| Error | Meaning |
|---|---|
| `ErrInvalidDBOptions` | Invalid store policy, feed mode, SST encoding, or `OpenBucket` input |
| `ErrWriterAlreadyOpen` | This `DB` already owns a writer |
| `ErrReaderAlreadyOpen` | This `DB` already owns a KV reader |
| `ErrChangeFeedDisabled` | `OpenChangeReader` was called for a disabled feed |
| `ErrChangeFeedPayloadMismatch` | Requested payload differs from persisted mode |
| `ErrStorePolicyMismatch` | Writer policy differs from persisted store policy |
| `ErrCommitIndeterminate` | An uncertain writer commit can no longer be proven because its manifest evidence was retired |

### Writer

| Error | Meaning |
|---|---|
| `ErrBackpressure` | Pending memtable limit reached before accepting the mutation |
| `ErrInvalidMutation` | Empty or oversized key, oversized value, or negative TTL |
| `ErrInvalidWriterOptions` | Invalid limits, interval, identity, or arena configuration |
| `ErrWriterClosed` | Operation attempted after writer close |
| `ErrWriterFailed` | Terminal background flush failure; wraps the cause |
| `ErrNilContext` | A nil context was supplied |

### Reader and snapshots

| Error | Meaning |
|---|---|
| `ErrInvalidReaderOptions` | Negative manifest refresh interval |
| `ErrReaderClosed` | Operation attempted after reader close |
| `ErrReadViewExpired` | The reader's loaded manifest view reached its store deadline |
| `ErrSnapshotClosed` | Operation attempted on a closed snapshot |
| `ErrSnapshotExpired` | Snapshot reached its inherited view deadline |
| `ErrIteratorExpired` | Iterator reached its inherited view deadline |

### Change feed

| Error | Meaning |
|---|---|
| `ErrChangeReaderClosed` | Operation attempted after close |
| `ErrInvalidChangeCursor` | Cursor text is malformed or unsupported |
| `ErrChangeCursorExpired` | Retention passed the requested cursor |
| `ErrInvalidChangeReadOptions` | Negative page limit |
| `ErrCorruptChangeFeed` | Manifest feed metadata is inconsistent |
| `ErrCorruptChangeBatch` | Change-batch index, block, or checksum is invalid |

### Maintenance

| Error | Meaning |
|---|---|
| `ErrMaintenanceAlreadyOpen` | This `DB` already owns maintenance |
| `ErrMaintenanceClosed` | Operation attempted after close |
| `ErrMaintenanceRunning` | `Run` or `RunOnce` already owns the run gate |
| `ErrInvalidMaintenanceOptions` | Invalid interval, concurrency, or work bound |

Provider and context errors can also be returned and wrapped. Always preserve
the complete error for logging.

## Advanced: `blobstore` package

Most applications should use `isledb.Open` or `isledb.OpenBucket`. The
`github.com/ankur-anand/isledb/blobstore` package is for storage adapters,
integration tests, and operational tooling.

```go
func blobstore.Open(ctx context.Context, bucketURL, prefix string) (*blobstore.Store, error)
func blobstore.New(bucket *blob.Bucket, bucketName, prefix string) *blobstore.Store
func blobstore.NewMemory(prefix string) *blobstore.Store
```

Object operations:

```go
func (s *Store) Read(ctx context.Context, key string) ([]byte, Attributes, error)
func (s *Store) ReadStream(ctx context.Context, key string) (*blob.Reader, error)
func (s *Store) ReadRange(ctx context.Context, key string, offset, length int64) ([]byte, error)
func (s *Store) ReadRangeStream(ctx context.Context, key string, offset, length int64) (*blob.Reader, error)
func (s *Store) Attributes(ctx context.Context, key string) (Attributes, error)
func (s *Store) Exists(ctx context.Context, key string) (bool, error)
func (s *Store) Write(ctx context.Context, key string, data []byte) (Attributes, error)
func (s *Store) WriteReader(ctx context.Context, key string, r io.Reader, opts *blob.WriterOptions) (Attributes, error)
func (s *Store) WriteIfMatch(ctx context.Context, key string, data []byte, etag string) (Attributes, error)
func (s *Store) WriteIfNotExist(ctx context.Context, key string, data []byte) (Attributes, error)
func (s *Store) Delete(ctx context.Context, key string) error
func (s *Store) BatchDelete(ctx context.Context, keys []string) error
```

Listing operations:

```go
type ListOptions struct {
    Prefix    string
    Delimiter string
}

type ObjectInfo struct {
    Key   string
    Size  int64
    IsDir bool
}

type ListResult struct {
    Objects []ObjectInfo
}

func (s *Store) List(ctx context.Context, opts ListOptions) (*ListResult, error)
func (s *Store) NewListIterator(opts ListOptions) *ListIterator
func (it *ListIterator) Next(ctx context.Context) (ObjectInfo, error)
func (s *Store) Walk(ctx context.Context, opts ListOptions, visit func(ObjectInfo) (bool, error)) error
```

`List` materializes every matching object. Prefer `NewListIterator` or `Walk`
for bounded operational scans.

```go
type Attributes struct {
    Size       int64
    ETag       string
    ModTime    time.Time
    Generation int64
}

type BatchDeleteError struct {
    Failed map[string]error
}
```

Blobstore sentinel errors are `blobstore.ErrNotFound`,
`blobstore.ErrPreconditionFailed`, and `blobstore.ErrBucketNameRequired`.
`Delete` is idempotent for a missing object. `BatchDeleteError` identifies
individual failed keys.

## Related documentation

- [Project overview](README.md)
- [Object-store schema](docs/object-store-schema.md)
