# IsleDB Go API Guide

This guide covers the public API most applications use, with the behavior
that affects correctness: when writes are durable, when readers see them, how
long a read view lasts, how change-feed cursors work, and how maintenance runs
in its own process.

```go
import "github.com/ankur-anand/isledb"
```

## Contents

- [Overview](#overview)
- [Quick start](#quick-start)
- [Open and close a database](#open-and-close-a-database)
- [Write data](#write-data)
  - [Is my write durable?](#is-my-write-durable)
  - [Writer lifecycle](#writer-lifecycle)
  - [Failures and retries](#failures-and-retries)
  - [Shutting down](#shutting-down)
  - [Writer options](#writer-options)
- [Read data](#read-data)
  - [Freshness and outages](#freshness-and-outages)
  - [Prepare views before reads switch to them](#prepare-views-before-reads-switch-to-them)
- [Change feed](#change-feed)
- [Run maintenance](#run-maintenance)
- [Prometheus metrics](#prometheus-metrics)
- [Error reference](#error-reference)
- [Appendix: how a reader reads SSTs](#appendix-how-a-reader-reads-ssts)
- [Appendix: SST output policy](#appendix-sst-output-policy)
- [Appendix: compaction limits](#appendix-compaction-limits)
- [Appendix: maintenance statistics](#appendix-maintenance-statistics)
- [Appendix: the blobstore package](#appendix-the-blobstore-package)

## Overview

An IsleDB database is one object-store bucket (or container) plus a prefix.
A `DB` value is the local runtime for that prefix, and it hands out four kinds
of handle:

| Handle | Per `DB` | Safe for concurrent use | Purpose |
|---|---:|---|---|
| `Writer` | One | Yes | Buffer writes and commit them |
| `Reader` | One | Yes | Point reads, scans, iterators, snapshots |
| `ChangeReader` | Any number | Yes; each caller owns its cursors | Consume the feed of committed writes |
| `Maintenance` | One | `Close` may run alongside `Run` or `RunOnce` | Compact, checkpoint, retain, reclaim |

Writer and maintenance ownership are also fenced through the object store, so
two processes can never act as the same owner at once. Readers and change
readers scale freely: open the same prefix from as many processes as you
like.

How a write becomes visible:

```text
Put / Delete
    -> buffered in the writer
    -> Flush, background flush, Drain or Close
    -> committed to the manifest
    -> visible to a newly opened or refreshed reader
```

## Quick start

This example turns off timed background flushing, so the explicit `Flush` is
the moment the write becomes durable and visible.

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
    if _, err := writer.Put(ctx, []byte("user:1"), []byte("Ankur")); err != nil {
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

`Open` takes a [Go Cloud bucket URL](https://gocloud.dev/howto/blob/). The
production schemes are `s3://`, `gs://` and `azblob://` (S3-compatible stores
work through `s3://`). Credentials come from the matching Go Cloud driver and
cloud SDK.

`file://` and `mem://` buckets are for development and tests. They behave
correctly within one process, but across processes their conditional writes
are not atomic: never point two processes at the same `file://` directory,
and never use either in production.

## Open and close a database

```go
func Open(ctx context.Context, bucketURL string, opts DBOptions) (*DB, error)
func OpenBucket(ctx context.Context, bucket *blob.Bucket, bucketName string, opts DBOptions) (*DB, error)

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

- `Open` creates the bucket connection and `DB.Close` closes it. `OpenBucket`
  borrows a `*blob.Bucket` you already have; closing it stays your job.
- `Prefix` is the database's root inside the bucket. Give each database its
  own prefix.
- `ChangeFeed` turns on the [change feed](#change-feed).
- `SSTOutput` sets how new SST files are encoded; see
  [SST output policy](#appendix-sst-output-policy).
- `MaxPinnedViewAge` is the longest a loaded manifest view stays usable. Zero
  selects `DefaultMaxPinnedViewAge`, one hour. The first writer stores this
  policy; a later writer with a different value fails to open with
  `ErrStorePolicyMismatch`. Readers and the deletion of retired objects
  (including change-feed batches) both use it, so an old view never refers to
  an object that has been deleted.

```go
func (db *DB) OpenWriter(ctx context.Context, opts WriterOptions) (*Writer, error)
func (db *DB) OpenReader(ctx context.Context, opts ReaderOpenOptions) (*Reader, error)
func (db *DB) OpenChangeReader(ctx context.Context) (*ChangeReader, error)
func (db *DB) OpenMaintenance(ctx context.Context, opts MaintenanceOptions) (*Maintenance, error)
func (db *DB) Close() error
```

`DB.Close` closes any handles still open on that `DB`. It is process-level
cleanup: close your handles yourself when their errors matter. An open writer
is closed with a fixed 30-second deadline and no retry, so if writes may be
in flight, run `Drain` and `Writer.Close` first; see
[Shutting down](#shutting-down).

## Write data

```go
func (w *Writer) Put(ctx context.Context, key, value []byte) (uint64, error)
func (w *Writer) PutWithTTL(ctx context.Context, key, value []byte, ttl time.Duration) (uint64, error)
func (w *Writer) Delete(ctx context.Context, key []byte) (uint64, error)
func (w *Writer) Flush(ctx context.Context) error
func (w *Writer) WaitCommitted(ctx context.Context, seq uint64) error
func (w *Writer) CommittedSequence() uint64
func (w *Writer) AcceptedSequence() uint64
func (w *Writer) State() WriterState
func (w *Writer) StopWrites()
func (w *Writer) Drain(ctx context.Context) error
func (w *Writer) Close(ctx context.Context) error
```

| Method | What it does |
|---|---|
| `Put`, `PutWithTTL`, `Delete` | Buffer the write in memory and return its sequence number |
| `Flush` | Commit everything buffered so far, and wait for it |
| `WaitCommitted` | Wait until a given sequence is committed |
| `CommittedSequence` | The highest committed sequence, without waiting |
| `AcceptedSequence` | The highest sequence handed out, committed or not |
| `State` | One consistent snapshot of the writer, for probes and dashboards |
| `StopWrites` | Refuse new writes from now on; keep committing the ones accepted |
| `Drain` | `StopWrites`, then commit everything accepted, retrying until ctx ends |
| `Close` | Make one last commit attempt and finish the writer |

Every method is safe to call from any goroutine.

**Sequences.** Each write gets the next sequence number, in the order writes
are accepted. Sequences continue across writers, and a change-feed consumer
sees the same number as `Change.Sequence`.

**Validation.** An empty or oversized key, an oversized value, or a negative
TTL returns an error wrapping `ErrInvalidMutation`, so you know not to retry
it. A TTL of zero means no expiration.

**TTLs.** Readers hide expired values. Expiry does not delete anything
immediately; compaction removes expired data later.

### Is my write durable?

A write is durable once it is committed to object storage. To acknowledge a
client only then:

```go
seq, err := writer.Put(ctx, key, value)
if err != nil {
    return err
}
if err := writer.WaitCommitted(ctx, seq); err != nil {
    return err // see the table below
}
// The write is in object storage; a reader's Refresh now returns it.
```

`WaitCommitted(ctx, seq)` waits until that write, and every one before it, is
committed. It does not flush by itself: writes commit with the next
background flush, `Flush`, `Drain` or `Close`, so many waiters share one
commit, and with healthy storage the wait is at most one flush interval.
Without a flush interval, call `Flush`.

| `WaitCommitted` returns | The write |
|---|---|
| `nil` | is committed |
| `ErrFenced` | was not committed and never will be: another writer took over |
| a context error | is not known yet: it is still being retried and may land |
| `ErrWriterClosed` | is not known: `Close` finished the writer before confirming it, and its last attempt may have landed |

Treat an unknown write as unknown, not lost, and make retries idempotent (for
example, `Put` the same key and value again).

### Writer lifecycle

`State().Status` reports where the writer is:

| Status | Entered by | `Put` / `Delete` | `Flush` | Commits |
|---|---|---|---|---|
| `WriterOpen` | `OpenWriter` | accepted | works | yes |
| `WriterStopped` | `StopWrites`, `Drain`, or `Close` starting | `ErrWritesStopped` | works | yes, including background flushes |
| `WriterClosed` | `Close` finishing | `ErrWriterClosed` | `ErrWriterClosed` | no |
| `WriterFenced` | another writer taking over | `ErrFenced` | `ErrFenced` | no |

Status only moves forward. `WriterStopped` is one-way: there is no reopening.
Storage errors never end a writer; only `Close` and losing the fence do.

`State` returns everything in one consistent snapshot:

```go
type WriterState struct {
    Status            WriterStatus // WriterOpen, WriterStopped, WriterClosed, WriterFenced
    Accepted          uint64       // highest sequence handed out
    Committed         uint64       // highest sequence in object storage
    PendingMemtables  int          // memtables not yet committed, the active one included
    PendingBytes      int64        // their approximate in-memory size, before compression
    OldestUncommitted time.Time    // when the oldest uncommitted write arrived; zero if none
}
```

Use it for readiness probes (stop routing writes once the status is not
`WriterOpen`), to show drain progress, and to slow producers before
`PendingMemtables` reaches `MaxPendingMemtables`. It can be out of date as
soon as it returns.

### Failures and retries

**One committer.** A single goroutine inside the writer does every commit:
background flushes, `Flush`, `Drain`, `Close`, and applying maintenance
commands. `Flush` asks it for a commit and waits. Your context bounds only the
wait, not the commit: a `Flush` that times out or is cancelled returns at
once, and the commit carries on.

**A failed commit is retried, never dropped.** The writes stay queued.
Background flushes retry with a delay that doubles from the flush interval up
to 30 seconds. A failed `Flush` returns its error; calling it again retries
at once. Every attempt first checks whether the previous one landed before
its response was lost, so a commit never lands twice.

**Every attempt has a deadline** of 30 seconds plus one second per MiB to
upload, so a hung storage request costs one attempt, not the writer. Each
attempt in a row that times out doubles the next one's deadline, up to 10
minutes, so a slow link still gets a commit through; a successful commit
resets it. A timed-out attempt is reported, and returned by a `Flush` waiting
on it, as `ErrCommitTimeout`. That error does not wrap
`context.DeadlineExceeded`: it is the writer's deadline, not yours.

**Backpressure.** While commits keep failing, writes pile up in memory. Once
`MaxPendingMemtables` frozen memtables are waiting, a write that needs
another one returns `ErrBackpressure` without being accepted. Retry after a
delay, or call `Flush`.

**Reporting.** `OnFlushError` receives every commit and maintenance failure,
such as an expired credential, an unavailable bucket or a timed-out attempt:
the first failure of a run, then at most once a minute while failures
continue. Commit failures and maintenance failures are separate runs, and
each ends, with a log line, at its next success. Without `OnFlushError`, the
writer logs a warning instead. Your own cancellation or deadline in `Flush`
is returned to you, not reported.

`OnFlushError` runs on its own goroutine. It may call `Close`, and it may run
after `Close` returns, so it must not use anything you tear down on close.

**Some failures need a person.** Missing permissions, a deleted bucket, a
manifest entry the storage rejects, or `ErrCommitIndeterminate` will not fix
themselves. The writer still never gives up: it retries every 30 seconds, and
`Put` keeps succeeding until `ErrBackpressure`. Alert on `OnFlushError`, or
on `isledb_writer_oldest_uncommitted_timestamp_seconds` (see
[metrics](#prometheus-metrics)), fix the cause, or close the writer.

**Maintenance commands** are applied by the same committer, in order with
data commits. A failure to apply one is reported and retried; it never holds
up data commits. See [Run maintenance](#run-maintenance).

### Shutting down

`Drain` stops new writes and commits everything accepted, retrying while
there is time; `Close` then finishes the writer:

```go
func shutdownWriter(ctx context.Context, w *isledb.Writer) error {
    // 1. Take no more writes, and commit every accepted one. Put and Delete
    //    now return ErrWritesStopped; failed attempts are retried until ctx.
    drainErr := w.Drain(ctx)

    // 2. Finish the writer: one attempt, then everything it runs stops.
    closeCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
    defer cancel()
    return errors.Join(drainErr, w.Close(closeCtx))
}
```

- **Spend most of the shutdown window in `Drain`.** It returns nil once every
  write the writer accepted is in object storage; `ErrFenced` at once if
  another writer took over; or the context's error, with the last commit
  failure, when time runs out. It works without a flush interval.
- **Rolling your own:** `Drain` is `StopWrites` plus a retry loop. Call
  `StopWrites` first, then wait for `WaitCommitted(ctx, w.AcceptedSequence())`,
  calling `Flush` if there is no flush interval. Without `StopWrites`, other
  goroutines can keep calling `Put` while you flush, and `Flush` only covers
  writes accepted before it was called.
- **Give `Close` its own deadline,** long enough for one commit attempt (30
  seconds plus one second per MiB pending). After a successful `Drain` it has
  nothing left to commit.
- **`Close` always finishes the writer,** whether or not its attempt
  succeeds; after it returns, nothing of the writer is running. It returns
  nil when everything is committed. Otherwise its error names the sequence
  range not known to be committed: those writes are unknown, not confirmed
  lost, and the range tells you what to check or replay.
- **Do not pass `Close` a context that has already ended:** it then finishes
  the writer without attempting a commit.

`DB.Close` is cleanup, not the durability step. It closes a writer still open
with a fixed 30-second deadline and no retry, and returns the writer's error.
That is enough when nothing is pending; otherwise call `Drain` and
`Writer.Close` yourself first.

To keep a crash cheap, set `Flush.Interval` so at most one interval of writes
is unflushed.

### Writer options

```go
type WriterOptions struct {
    OwnerID      string
    Memtable     WriterMemtableOptions
    Flush        WriterFlushOptions
    Maintenance  WriterMaintenanceOptions
    Values       ValueOptions
    OnFlushError func(error)
    Metrics      *WriterMetrics
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

| Option | Default | Meaning |
|---|---:|---|
| `OwnerID` | generated | Stable identity stored in the writer fence |
| `Memtable.TargetBytes` | 16 MiB | Size at which the active memtable is frozen and queued |
| `Memtable.MaxPendingMemtables` | 4 | Frozen memtables waiting to commit before `ErrBackpressure` |
| `Flush.Interval` | 1 second | Background commit cadence; **zero turns it off** |
| `Maintenance.PollInterval` | 1 second | How often the writer checks for maintenance commands |
| `Values.MaxKeyBytes` | 64 KiB | Largest accepted key |
| `Values.MaxValueBytes` | 16 MiB | Largest accepted value |
| `OnFlushError` | `nil` | Callback for commit and maintenance failures; see [Failures and retries](#failures-and-retries) |
| `Metrics` | `nil` | Optional Prometheus observations |

`Flush.Interval` is the one option where zero does not mean "default": it
disables background flushing. Start from `DefaultWriterOptions` when you want
every default.

## Read data

```go
type ReaderOpenOptions struct {
    CacheDir       string
    DiskCacheSize  int64
    BlockCacheSize int64
    BloomCacheSize int64
    Views          ReaderViewPolicy
    Metrics        *ReaderMetrics
}

type ReaderViewPolicy struct {
    RefreshAfter time.Duration
    Manual       bool
    MaxLag       time.Duration
}

func DefaultReaderOpenOptions(cacheDir string) ReaderOpenOptions
```

`CacheDir` is required, and only one live reader process may use it. Zero
sizes select the defaults; negative sizes are rejected.

| Option | Default | Meaning |
|---|---:|---|
| `DiskCacheSize` | 8 GiB | Bytes kept on disk: SST metadata, Bloom filters and SST data |
| `BlockCacheSize` | 256 MiB | Decoded SST blocks kept in memory |
| `BloomCacheSize` | 64 MiB | Parsed Bloom filters kept in memory |
| `Views.RefreshAfter` | 1 minute | How often the view is refreshed in the background (at least 1 second) |
| `Views.Manual` | `false` | Leave publishing newer views to the application; see [Prepare views before reads switch to them](#prepare-views-before-reads-switch-to-them) |
| `Views.MaxLag` | 5 minutes, or a quarter of `MaxPinnedViewAge` if shorter | In manual mode, how long the published view may stay outdated before the reader publishes the newest view itself. A value set explicitly must be below half the store's `MaxPinnedViewAge`, or open fails with `ErrInvalidReaderOptions`; if the store's first writer later sets a shorter age, half of it is used and a warning logged once |
| `Metrics` | `nil` | Optional Prometheus observations |

```go
func (r *Reader) Get(ctx context.Context, key []byte) ([]byte, bool, error)
func (r *Reader) Scan(ctx context.Context, minKey, maxKey []byte) ([]KV, error)
func (r *Reader) ScanLimit(ctx context.Context, minKey, maxKey []byte, limit int) ([]KV, error)
func (r *Reader) NewIterator(ctx context.Context, opts IteratorOptions) (*Iterator, error)
func (r *Reader) Snapshot(ctx context.Context) (*Snapshot, error)
func (r *Reader) BootstrapView(ctx context.Context) (*BootstrapView, error)
func (r *Reader) Prefetch(ctx context.Context, opts PrefetchOptions) (PrefetchStats, error)
func (r *Reader) Refresh(ctx context.Context) error
func (r *Reader) ViewPosition() ViewPosition
func (r *Reader) NextView(ctx context.Context) (*NextView, error)
func (r *Reader) DiskCacheStats() DiskCacheStats
func (r *Reader) BlockCacheStats() CacheStats
func (r *Reader) OpenSSTCacheStats() CacheStats
func (r *Reader) BloomCacheStats() CacheStats
func (r *Reader) ManifestPageCacheStats() CacheStats
func (r *Reader) Close() error

type KV struct {
    Key   []byte
    Value []byte
}
```

- `Get` returns `found == false` for a key that is missing, deleted or
  expired.
- **Every range is half-open, `[minKey, maxKey)`:** `Scan`, `ScanLimit`,
  `IteratorOptions`, `Snapshot.ScanLimit`, snapshot iterators and
  `PrefetchOptions.Range`. A nil or empty bound leaves that side open. Because
  the upper bound is exclusive, `PrefixRange(prefix)` can be passed to any
  range API as is.
- `Scan` builds the whole result in memory. `ScanLimit` stops after `limit`
  results; zero or a negative limit means no limit. For large ranges, use an
  iterator.
- `Refresh` loads the latest commits now; see
  [Freshness and outages](#freshness-and-outages).
- `Prefetch` downloads only into the disk cache's free space, so it never
  evicts what is already cached. SSTs that do not fit are counted in
  `PrefetchStats.SkippedSSTs` and load on demand.
- When a refresh retires SSTs, after a compaction, the reader deletes them
  from its disk cache in the background, at most 1,000 files a second so the
  deletes do not slow lookups, keeping the space for data a view can read.
  `isledb_reader_dead_ssts_pending` shows how many are still queued.

### Iterate without loading the whole range

```go
iter, err := reader.NewIterator(ctx, isledb.IteratorOptions{
    MinKey: []byte("user:"), // inclusive; nil means the beginning
    MaxKey: []byte("user;"), // exclusive; nil means the end
})
if err != nil {
    return err
}
defer func() { _ = iter.Close() }()

for iter.Next() {
    key, value := iter.Key(), iter.Value()
    _, _ = key, value
}
if err := iter.Err(); err != nil {
    return err
}
```

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

A snapshot pins one loaded view. It does not move when its reader refreshes.

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

A snapshot and its iterators share the view's fixed deadline
(`MaxPinnedViewAge` after it was loaded); opening another handle does not
extend it. Past it, operations return `ErrSnapshotExpired`,
`ErrIteratorExpired` or `ErrReadViewExpired`: take a new snapshot rather than
retrying. Closing the reader invalidates its snapshots and iterators.

A snapshot of an older view keeps reading correctly after the reader moves
on: object storage keeps its SSTs for `MaxPinnedViewAge`. Once a refresh
retires those SSTs, the reader deletes them from its disk cache, so the
snapshot may fetch them again from object storage. A long `BootstrapView`
load running across a compaction is where that shows.

### Load state, then follow the change feed

`BootstrapView` gives you a snapshot and the exact change-feed position that
follows it, both from the same manifest:

```go
type BootstrapView struct {
    Snapshot *Snapshot
    Cursor   ChangeCursor
    Version  Version
}
```

The snapshot holds every write committed before `Cursor`; `Cursor` is the
first feed position the snapshot does not contain. `Version` matches
`Snapshot.Version()` and is handy in checkpoint metadata. Build a local copy
from the snapshot, save `Cursor` with it, and then consume only later
changes:

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

- Do not build this pair from `Snapshot()` and `ChangeReader.Bounds()`
  separately: a commit landing between the two calls gives a cursor newer
  than the snapshot, and you would skip that change.
- The change feed must be enabled, or this returns `ErrChangeFeedDisabled`.
- Finish before the snapshot's deadline, and keep change-feed retention long
  enough that `Cursor` is still there when you start consuming.
- Like `Snapshot`, it uses the reader's current view; call `Refresh` first if
  you need the very latest commit.

### Prefetch SSTs to disk

```go
type KeyRange struct {
    Min []byte // inclusive; nil means the beginning
    Max []byte // exclusive; nil means the end
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

```go
stats, err := reader.Prefetch(ctx, isledb.PrefetchOptions{
    Range:       isledb.PrefixRange([]byte("user:")),
    MaxBytes:    256 << 20,
    Concurrency: 4,
})
```

`Prefetch` downloads the selected SSTs (metadata, Bloom filter and data) into
the disk cache, fetching only what is missing and sharing requests with
concurrent reads.

- Use `All: true` to prefetch the whole keyspace.
- Zero `MaxSSTs` or `MaxBytes` means no limit beyond the disk cache itself.
  `MaxBytes` bounds what this call downloads, each SST's Bloom filter
  included, so repeated calls warm a large range in steps.
- It skips SSTs already on disk and selects only what fits in the disk
  cache's free space, each SST counted with its Bloom filter, so repeating a
  prefetch larger than the cache keeps what the last one cached. SSTs left
  out count in `SkippedSSTs`.
- `CachedSSTs` counts selected SSTs wholly on disk when it returns;
  `BytesRead` counts bytes it fetched.
- It uses the reader's current view and does not force a refresh.
- A reader runs one prefetch at a time, `NextView.Prefetch` included; another
  waits for it, or for its context to end. Each then sees the space the last
  one used, so two never fill the same free space.

### Freshness and outages

A reader answers from a loaded view of the manifest. The view is refreshed in
the background every `Views.RefreshAfter`; reads never start a refresh or wait
for one.

| Situation | What reads do |
|---|---|
| Refreshes succeed | The view is never older than `RefreshAfter`. |
| A refresh fails, or takes over 30 seconds | Reads keep using the loaded view, and count in `stale_reads_total`. The next refresh is tried about 30 seconds later (15 to 45, or `RefreshAfter` if that is shorter). A warning is logged at most once a minute, and an info line when refreshes recover. |
| The view is older than `MaxPinnedViewAge` | A read waits for a refresh and fails if it fails. Background refreshes keep trying, so reads work again as soon as storage answers. |

So during an outage reads can be up to `MaxPinnedViewAge` old, and they stay
correct: every SST a view names is kept until the view expires.

**Spread-out refreshes.** Each reader picks a random offset within
`RefreshAfter` when it opens and refreshes exactly once per interval at that
offset. Readers started together by a deploy stay spread evenly; neither a
forced `Refresh` nor a failure moves the schedule, and retries after an
outage stay on the reader's own offset. N readers read `CURRENT` a steady
N / `RefreshAfter` times a second. The one exception is opening: each reader
loads its first view as it opens.

An idle reader still reads `CURRENT` once per interval.

**`Refresh`** always reloads, waits and returns any failure, so a caller that
needs the latest commits learns when it cannot have them. It reflects every
commit made before the call, even when it joins a refresh already in
progress.

Internals of how a reader fetches and caches data are in
[the appendix](#appendix-how-a-reader-reads-ssts).

### Prepare views before reads switch to them

A compaction rewrites data into new SSTs. When reads switch to a view naming
them, the first lookups of hot keys each wait for object storage. In manual
mode the application takes each newer view the reader loads, prepares it, for
example by downloading its new SSTs, and only then switches reads to it.

```go
type ViewPosition uint64

type SSTInfo struct {
    ID             string
    Level          int
    Size           int64 // in the manifest
    BloomSize      int64 // stored after Size in the same object
    MinKey, MaxKey []byte
}

func (v *NextView) Previous() ViewPosition
func (v *NextView) Next() ViewPosition
func (v *NextView) Version() Version
func (v *NextView) Added() []SSTInfo
func (v *NextView) Removed() []SSTInfo
func (v *NextView) Snapshot() (*Snapshot, error)
func (v *NextView) Prefetch(ctx context.Context, opts PrefetchOptions) (PrefetchStats, error)
func (v *NextView) Publish() error
func (v *NextView) Discard()
```

```go
opts := isledb.DefaultReaderOpenOptions("/var/cache/isledb")
opts.Views.Manual = true
reader, err := db.OpenReader(ctx, opts)
if err != nil {
    return err
}
for {
    next, err := reader.NextView(ctx) // waits for the reader's next newer load
    if err != nil {
        return err // ctx ended or the reader closed
    }
    if _, err := next.Prefetch(ctx, isledb.PrefetchOptions{All: true}); err != nil {
        log.Printf("warming view %d: %v", next.Next(), err) // still correct, only colder
    }
    switch err := next.Publish(); {
    case err == nil, errors.Is(err, isledb.ErrViewChanged), errors.Is(err, isledb.ErrNextViewExpired):
        // Published, or another view was; take the next one.
    default:
        return err
    }
}
```

- **The reader keeps loading.** Every `RefreshAfter` it loads the manifest
  and renews an unchanged view, as in the default mode. A newer view is kept
  unpublished for the application instead of replacing the published one.
- **`NextView`** returns a loaded view newer than the published one, waiting
  for the reader's next such load if none is pending. Each load is handed out
  once: after a view is taken, published or not, the next call waits for a
  later load, at most `RefreshAfter` away, so two goroutines calling it get
  different loads. Outside manual mode it returns `ErrNotManual`.
- **Positions.** A `ViewPosition` is the manifest log position a view
  reflects; two views at the same position hold the same data.
  `Reader.ViewPosition()` returns the published one without a lock, so state
  prepared per view can be checked on every read. `Previous` is the published
  position when the view was handed out, `Next` the view's own.
- **`Added` and `Removed`** list the SSTs the view names that the published
  one does not, and the reverse. An SST moved between levels is in neither.
  Caching an SST downloads `Size + BloomSize` bytes.
- **`Prefetch`** downloads the `Added` SSTs, as `Reader.Prefetch` does, into
  the disk cache's free space only, so it never evicts what reads are using.
  Give the disk cache room for the largest compaction's output beside the
  live data, up to twice the store when a compaction rewrites all of it, or
  warming covers only part of a view and the rest loads on demand.
- **`Snapshot`** reads the view before it is published. It never refreshes.
- **`Publish`** is a compare-and-swap: it switches reads to the view only if
  the published view is still at `Previous`. Otherwise it returns
  `ErrViewChanged`. A view past its readable life, `MaxPinnedViewAge` from
  when it was loaded, returns `ErrNextViewExpired`. After `Publish` or
  `Discard`, every method that acts returns `ErrNextViewDone`. `Discard`
  releases nothing, so it is optional.
- **`Refresh` publishes at once** in manual mode too, for read-your-writes.
  A pending `Publish` then gets `ErrViewChanged`.
- **The safety net.** If the published view has been outdated for `MaxLag`,
  the reader publishes the newest view it loaded itself, so a stuck
  application loop costs freshness, never availability. If the published
  view has already expired when a newer one loads, the reader publishes at
  once.
- **The cost is visibility.** A change reaches reads after the next refresh
  plus the time to prepare it, at most `MaxLag`.

## Change feed

The change feed is optional. Turn it on when opening the database:

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

- `ChangeFeedKeysOnly` records the operation, key, sequence and expiry. It
  suits invalidation and "fetch the current value" consumers.
- `ChangeFeedFullValues` also records PUT values, for replaying history.

The payload mode is stored when the feed is enabled and cannot change.
Reopening with a different mode returns `ErrChangeFeedPayloadMismatch`;
reopening with `ChangeFeed == nil` adopts the stored mode. Enabling the feed
on an existing database starts it at the current head; older history is not
added.

### Types

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

In keys-only mode, a PUT has `HasValue == false` and a nil `Value`. In
full-values mode, `HasValue` tells an empty value apart from a missing one.
Deletes never carry a value.

### Read pages

```go
type ChangeReadOptions struct {
    MaxChanges int
    MaxBytes   int64
}

func DefaultChangeReadOptions() ChangeReadOptions

func (r *ChangeReader) Bounds(ctx context.Context) (ChangeBounds, error)
func (r *ChangeReader) Read(ctx context.Context, from ChangeCursor, opts ChangeReadOptions) (ChangePage, error)
func (r *ChangeReader) Close() error
```

- Default page limits: 1,024 changes and 16 MiB. `MaxChanges` is capped at
  65,536. `MaxBytes` counts key and value bytes, not Go allocation overhead.
  A single change larger than `MaxBytes` is returned alone, so the cursor
  always moves.
- Zero fields select defaults; negative ones return
  `ErrInvalidChangeReadOptions`.
- A cursor is an opaque resume position: a manifest entry plus an index
  within its change batch. It is not `Change.Sequence`, and you cannot seek by
  sequence.
- `Read` does not wait for new writes. A page may be empty yet still move the
  cursor past manifest entries with no user writes. Keep reading until
  `CaughtUp()` is true, then poll at your own interval.
- Change batches are split into compressed, checksummed blocks; a read fetches
  only the blocks its page needs and reuses decoded blocks from a bounded
  cache.

### Choose where to start

A zero cursor means "no saved position", not "start at the latest change".
Pick the starting policy explicitly:

```go
bounds, err := reader.Bounds(ctx)
if err != nil {
    return err
}

// Replay every change still retained.
replayCursor := bounds.Oldest

// Skip existing history; consume only changes published after Bounds.
tailCursor := bounds.Head
```

`Oldest` is the first retained position. `Head` is just past everything
`Bounds` saw, so a first read from it is usually empty and caught up. (A zero
cursor passed to `Read` starts at `Oldest`, but calling `Bounds` makes the
choice explicit.)

**Save the starting cursor before you poll.** Otherwise a process that picks
`Head`, crashes before saving it, and restarts would pick a newer head and
silently skip changes. After that, save `page.Next` only once the whole page
has been applied.

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

### When a cursor expires

If retention deletes changes past a saved cursor, `Read` refreshes its view
and returns `ErrChangeCursorExpired`: there is a gap between your position
and the oldest retained one. You can:

- restart from `Bounds().Oldest`, accepting the gap;
- rebuild derived state from a KV snapshot, then resume the feed (see
  [BootstrapView](#load-state-then-follow-the-change-feed));
- or, if you need every historical event, stop and recover explicitly; a
  current-state snapshot cannot recreate intermediate updates or deletes.

## Run maintenance

Maintenance compacts, checkpoints, applies change-feed retention and deletes
retired objects. It is designed to run in its own process, against the same
bucket and prefix:

```text
writer process       -> buffers writes and commits them
reader processes     -> load views and read independently
maintenance process  -> prepares compaction, checkpoints and retention
reclamation workers  -> delete retired objects at their own bounded pace
```

**How its work is published.** Maintenance never edits the manifest itself.
It does the heavy work (for example, writing compacted SSTs), then stages a
command in a small mailbox object, `maintenance/HEAD`. The active writer
checks the mailbox in the background every
`WriterOptions.Maintenance.PollInterval` (one second by default, each check
bounded to 5 seconds) and applies the command with its next commit, in order
with data commits. `Flush` never waits on the mailbox, so a slow or hung
mailbox cannot delay commits. `Close` checks it once more so a command staged
just before shutdown is not left behind, and a command superseded by a later
one is skipped. Without a flush interval, a fetched command is applied at the
next `Flush`, `Drain` or `Close`.

**Deletion runs separately.** SSTs, change batches, manifest snapshots and
manifest pages are deleted in independent lanes at their own pace, so slow
deletes never block compaction.

```go
func (m *Maintenance) Run(ctx context.Context) error
func (m *Maintenance) RunOnce(ctx context.Context) (MaintenanceStats, error)
func (m *Maintenance) Close(ctx context.Context) error
```

`Run` continues until its context ends, `Close` is called, or another
process takes over maintenance. Close with a fresh context, since the run
context is usually already cancelled:

```go
func runMaintenance(ctx context.Context, db *isledb.DB) error {
    maintenance, err := db.OpenMaintenance(ctx, isledb.DefaultMaintenanceOptions())
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

For a scheduled job, call `RunOnce` instead:

```go
stats, err := maintenance.RunOnce(ctx)
```

`RunOnce` makes one control pass and one bounded pass of each deletion lane.
If it returns `MaintenanceWaitingForWriter`, it has staged a command that the
writer must apply; the next run picks up from there. One scheduled run is a
bounded step of progress, not a promise to clear the whole backlog.

### Maintenance options

```go
type MaintenanceOptions struct {
    IdleInterval        time.Duration
    SSTCompaction       SSTCompactionOptions
    ManifestCheckpoint  ManifestCheckpointOptions
    ChangeFeedRetention *ChangeFeedRetentionOptions
    Reclamation         ReclamationOptions
    OnCycle             func(MaintenanceStats)
    OnReclamationCycle  func(ReclamationCycleStats)
    OnError             func(error)
}

type SSTCompactionOptions struct {
    ReadConcurrency   int
    ScratchDir        string
    L0TriggerSSTs     int
    BaseLevelBytes    int64
    LevelGrowthFactor int
    TargetSSTBytes    int64
}

type ManifestCheckpointOptions struct {
    TargetReplayPages uint64
    TargetReplayBytes uint64
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

func DefaultMaintenanceOptions() MaintenanceOptions
```

| Option | Default |
|---|---:|
| `IdleInterval` | 5 seconds |
| `SSTCompaction.ReadConcurrency` | 4 |
| `SSTCompaction.ScratchDir` | user cache directory, falling back to a per-user temporary directory |
| `SSTCompaction.L0TriggerSSTs` | 8 |
| `SSTCompaction.BaseLevelBytes` | 512 MiB |
| `SSTCompaction.LevelGrowthFactor` | 8 |
| `SSTCompaction.TargetSSTBytes` | 64 MiB |
| `ManifestCheckpoint.TargetReplayPages` | 64 pages |
| `ManifestCheckpoint.TargetReplayBytes` | 32 MiB |
| `ChangeFeedRetention` | `nil`: history kept forever |
| `Reclamation.MaxConcurrentDeletes` | 4, shared by all lanes |

| Deletion lane | Poll interval | Most objects per pass |
|---|---:|---:|
| SST | 1 second | 128 |
| Change feed | 5 seconds | 128 |
| Manifest snapshots and pages | 1 minute | 128 |

The manifest lane also audits for orphaned objects once an hour
(`AuditInterval`). `MaxObjectsPerPass` bounds normal work, but one SST
retirement plan, already bounded, may finish in a single pass even if it goes
over.

**Scratch space.** Compaction streams its inputs to `ScratchDir`, so a job is
not limited by memory. Each store, role and fence epoch gets a private
session directory below it. A clean close removes the session; a later epoch
tries to remove abandoned ones, and a failed cleanup never stops maintenance
from opening. IsleDB never deletes the configured directory itself or
anything below it that it did not create.

Compacted SSTs are encoded with `DBOptions.SSTOutput.Compacted`. The
compaction planner's limits are described in
[the appendix](#appendix-compaction-limits).

`OnCycle` receives statistics for each control pass and `OnReclamationCycle`
for each deletion pass; the types are in
[the appendix](#appendix-maintenance-statistics).

### Change-feed retention

Retention is off until `ChangeFeedRetention` is set:

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

`RetainFor` is a minimum age, not an exact deletion time: publishing,
view-safety rules and paced deletion can keep objects longer.

## Prometheus metrics

```go
func DefaultWriterMetrics(constLabels prometheus.Labels) *WriterMetrics
func DefaultReaderMetrics(constLabels prometheus.Labels) *ReaderMetrics
```

Set the result as `WriterOptions.Metrics` or `ReaderOpenOptions.Metrics`. The
constructors create the collectors but do not register them; register them
with your `prometheus.Registerer`.

**Writer metrics** cover puts, deletes, backpressure, flush counts, errors,
latency and bytes, plus:

- `isledb_writer_committed_sequence`: the highest sequence committed and
  visible to readers. Set at open from the manifest and after each commit, so
  it never counts writes still in memory and continues across a failover.
- `isledb_writer_oldest_uncommitted_timestamp_seconds`: the Unix time the
  oldest uncommitted write was accepted, or 0 when everything is committed.
  Alert when commits keep failing:

```promql
time() - (isledb_writer_oldest_uncommitted_timestamp_seconds > 0) > 120
```

**Change-feed lag.** Every `Change` carries the writer's sequence, so a
consumer that exports its last applied `Change.Sequence` gets its lag from
Prometheus with no extra reads. With several databases, label each one's
metrics through `constLabels` and match on it:

```promql
clamp_min(
  max by (db) (isledb_writer_committed_sequence)
    - on(db) group_right isledb_follower_applied_sequence,
  0)
```

`clamp_min` hides brief negative values caused by scrape timing. Lag counts
changes committed but not yet applied; writes still buffered in the writer
are not committed and not counted.

**Reader metrics** cover refreshes, point reads, scans and SST range reads,
plus:

- `stale_reads_total`: reads answered from a view whose refresh failed (see
  [Freshness and outages](#freshness-and-outages));
- `view_loaded_timestamp_seconds`: when the current view was loaded;
  `time() - isledb_reader_view_loaded_timestamp_seconds` is its age;
- `dead_ssts_pending`: SSTs a publish retired that are not yet deleted from
  the disk cache. It rises after a compaction and drains; one that keeps
  rising means deletes fall behind compactions.

In [manual mode](#prepare-views-before-reads-switch-to-them):

- `next_view_publishes_total{result}`: `NextView.Publish` calls by result,
  `published`, `changed` or `expired`. Calls on a view already done, or on a
  closed reader, are not counted.
- `view_outdated_since_timestamp_seconds`: when the published view became
  outdated, the load time of a newer view not yet published, or 0 when it is
  the newest loaded. Alert when preparation falls behind:

```promql
time() - (isledb_reader_view_outdated_since_timestamp_seconds > 0) > 60
```

- `view_safety_publishes_total`: views the reader published itself after
  `MaxLag`. A rising count means the application's loop is stuck or too
  slow.

## Error reference

Check sentinel errors with `errors.Is`:

```go
if errors.Is(err, isledb.ErrBackpressure) {
    // Retry according to the application's admission policy.
}
```

Storage and context errors can also be returned, wrapped. Log the complete
error.

### Database and configuration

| Error | Meaning |
|---|---|
| `ErrInvalidDBOptions` | Invalid store policy, feed mode, SST encoding, or `OpenBucket` input |
| `ErrWriterAlreadyOpen` | This `DB` already has a writer |
| `ErrReaderAlreadyOpen` | This `DB` already has a KV reader |
| `ErrChangeFeedDisabled` | The change feed is not enabled |
| `ErrChangeFeedPayloadMismatch` | Requested payload differs from the stored mode |
| `ErrStorePolicyMismatch` | Writer policy differs from the stored store policy |
| `ErrCommitIndeterminate` | An uncertain commit can no longer be proven, because the manifest entries that would show it were retired |

### Writer

| Error | Meaning |
|---|---|
| `ErrBackpressure` | Too many memtables waiting to commit; the write was not accepted |
| `ErrInvalidMutation` | Empty or oversized key, oversized value, or negative TTL |
| `ErrInvalidWriterOptions` | Invalid limits, interval, identity, or memory settings |
| `ErrWritesStopped` | A write after `StopWrites`, `Drain` or while `Close` runs; accepted writes are still committing |
| `ErrWriterClosed` | The writer was closed |
| `ErrCommitTimeout` | A commit attempt ran out of its own deadline; the writes stay queued and are retried |
| `ErrFenced` | Another writer took over; this one commits nothing more |
| `ErrNilContext` | A nil context was passed |

### Reader and snapshots

| Error | Meaning |
|---|---|
| `ErrInvalidReaderOptions` | Negative or sub-second refresh interval, negative `MaxLag`, or `MaxLag` not below half the store's `MaxPinnedViewAge` |
| `ErrReaderClosed` | The reader was closed |
| `ErrNotManual` | `NextView` on a reader not in manual mode |
| `ErrViewChanged` | `Publish` found another view published since the view was handed out |
| `ErrNextViewExpired` | The next view passed `MaxPinnedViewAge` from its load before it was published |
| `ErrNextViewDone` | The next view was already published or discarded |
| `ErrReadViewExpired` | The reader's view passed `MaxPinnedViewAge` |
| `ErrSnapshotClosed` | The snapshot was closed |
| `ErrSnapshotExpired` | The snapshot's view passed its deadline |
| `ErrIteratorExpired` | The iterator's view passed its deadline |

### Change feed

| Error | Meaning |
|---|---|
| `ErrChangeReaderClosed` | The change reader was closed |
| `ErrInvalidChangeCursor` | Cursor text is malformed or unsupported |
| `ErrChangeCursorExpired` | Retention deleted changes past the cursor |
| `ErrInvalidChangeReadOptions` | Negative page limit |
| `ErrCorruptChangeFeed` | Feed metadata in the manifest is inconsistent |
| `ErrCorruptChangeBatch` | A change batch's index, block or checksum is invalid |

### Maintenance

| Error | Meaning |
|---|---|
| `ErrMaintenanceAlreadyOpen` | This `DB` already has a maintenance handle |
| `ErrMaintenanceClosed` | Maintenance was closed |
| `ErrMaintenanceRunning` | `Run` or `RunOnce` is already running |
| `ErrInvalidMaintenanceOptions` | Invalid interval, concurrency, or work bound |

## Appendix: how a reader reads SSTs

Every SST is read from object storage by byte range, through four caches:

| Cache | Holds | Bounded by |
|---|---|---|
| Open SSTs | Parsed SST readers, up to 1,024, least recently used out first | count |
| Block cache | Decoded index and data blocks | `BlockCacheSize` |
| Bloom cache | Parsed Bloom filters | `BloomCacheSize` |
| Disk cache, under `CacheDir` | SST metadata and Bloom filters; whole small SSTs and 128 KiB chunks of larger ones. One budget; data is evicted first, never metadata to make room for data | `DiskCacheSize` |

**Fetching.**

- An SST of up to 4 MiB is fetched whole, with its Bloom filter, in one
  request, and its SHA-256 is checked against the manifest.
- A larger SST's metadata and Bloom filter are fetched in one request each;
  its data in aligned 128 KiB chunks. A lookup fetches the chunk holding its
  block.
- A scan reads ahead: each request fetches twice as many chunks as the last,
  up to 4 MiB, and a seek elsewhere starts small again.
- No request repeats bytes already on disk or being fetched, and concurrent
  reads of the same part share one request.
- Data blocks of large SSTs are checked by their own checksums, which catch
  damaged bytes but not a different, internally valid object stored under the
  SST's name.

**What gets cached.** A point lookup caches the blocks it reads, in memory
and on disk. A scan, and each iterator seek, caches only its first 64 KiB of
keys and values in each SST, so short reads (a page, a prefix, a seek) are
warm when repeated. Reading further uses buffers of its own, still drawing on
what is cached: a long scan adds at most about 64 KiB per SST to memory and
two chunks to disk, and never evicts what lookups reuse.

**Speed and eviction.** A read of an already-open SST parses no metadata; a
warm lookup takes about 1 µs. Each cache evicts least recently used first.
Nothing is dropped when an SST leaves the manifest, so a snapshot still
reading it stays warm. A cached part that proves damaged is dropped and
fetched again; a lookup that meets damaged cache bytes retries once from
object storage instead of failing.

**The disk cache** survives restarts and holds no files open: each read
opens, reads and closes, so the budget is exact.

- A fetched part is written before the read returns, to a temporary file
  renamed into place without `fsync`. On a local SSD that adds about 2–4% to
  a cold read and nothing to a warm one.
- Each file name carries its size, so startup only lists directories: about
  0.1 s for a 10 GiB cache, a few seconds at 100 GiB.
- After a crash, startup drops unfinished writes. A file shorter than its
  name says fails the read that reaches past its end and is dropped then;
  checksums catch damaged bytes.
- Fetched bytes are never held in memory waiting to be written; if the disk
  cannot store a part, a later read fetches it again.
- Opening logs a warning when the filesystem cannot hold `DiskCacheSize`.

**Block cache memory.** The block cache is Pebble's. With cgo it lives outside
the Go heap: it counts toward resident memory but not toward `GOMEMLIMIT` or
heap profiles. Without cgo (`CGO_ENABLED=0`) it is ordinary Go heap, which the
garbage collector lets grow to about twice the live heap under the default
`GOGC`; set `GOMEMLIMIT` to bound the process.

### Cache statistics

```go
type CacheStats struct {
    Hits        int64
    Misses      int64
    Bytes       int64
    MaxBytes    int64
    EntryCount  int
    MaxEntries  int
    Evictions   int64
    Corruptions int64
    Bypasses    int64
    Failures    int64
}

type DiskCacheStats struct {
    Meta     CacheStats // SST metadata and Bloom filters
    Data     CacheStats // whole small SSTs and chunks of larger SSTs' data
    SSTDrops int64      // reads that failed on damaged bytes and dropped the SST
}
```

- `BlockCacheStats` counts hits and misses on index and data blocks. The
  metaindex and properties blocks, read on every SST open and never cached,
  are not counted. `OpenSSTCacheStats` reports how many reads found their SST
  already open.
- Byte-bounded caches report `MaxEntries == 0`; the open-SST cache, bounded by
  count, reports `MaxBytes == 0`.
- The disk cache has one budget, `DiskCacheSize`, shared by metadata and
  data: `Meta.MaxBytes` and `Data.MaxBytes` both report it, and
  `Meta.Bytes + Data.Bytes` is what is in use. Data is evicted first, and data
  is never stored by evicting metadata.
- For the disk cache: `Corruptions` counts entries it found damaged itself (a
  wrong size, or a Bloom filter failing its checksum); `Bypasses` entries not
  stored, because they are larger than the whole cache or are data that would
  fit only by evicting metadata; `Failures` entries that could not be written.
  Bypassed and failed entries are still served, just not kept, so a disk that
  keeps failing means each later read fetches them again.
- `SSTDrops` counts reads that failed on what looked like damaged bytes; each
  drops the SST from every layer, so the next read fetches it again. It counts
  drops, not distinct SSTs: concurrent readers of one damaged SST each count,
  a lookup whose retry also fails counts twice, and an SST read in chunks that
  is damaged in object storage, or fails to open the same way every time,
  counts on every read. A small SST damaged in object storage fails its
  whole-object checksum when fetched, so it fails reads without counting. A
  steadily rising count points at an SST in one of those states.

## Appendix: SST output policy

How new SST files are encoded is a runtime setting. It applies to files
written from now on and is not stored in the manifest; every file describes
its own encoding, so readers handle a mix.

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

Compression is `"none"`, `"snappy"` or `"zstd"`. Zero fields select the
defaults:

| SST class | Compression | Data block target | Bloom bits/key |
|---|---|---:|---:|
| Writer output (L0) | Snappy | 4 KiB | 10 |
| Compacted output | Snappy | 4 KiB | 10 |

The writer and maintenance can use different settings:

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

Separate writer and maintenance processes each pass their own `DBOptions`.
Different settings are safe, though one shared configuration makes
performance easier to predict.

## Appendix: compaction limits

One manifest entry can remove at most 128 objects and add at most 128. This is
a fixed format rule, not a tuning option. The planner picks the widest source
batch whose whole overlap with the next level fits. If even one source SST
overlaps too many files below it, the planner first compacts that destination
level further down and retries the original move on a later cycle.

If that drain reaches the bottom of the tree, IsleDB creates one deeper
level. For sorted levels this is usually a metadata-only move: the SSTs keep
their IDs and bytes, and only their level changes. It is a correctness escape
hatch for the 128-object limit, not normal level sizing. Doing it repeatedly
can deepen reads and delay merging tombstones and overwritten values, so it is
counted in `SSTCompactionStats.ForcedLevelCreations`, with
`DeepestForcedLevel` recording the deepest level reached. Ordinary creation of
a new bottom level by size is not counted. As with every compaction, the
statistic counts a staged job; the new layout is visible only once the writer
applies it.

## Appendix: maintenance statistics

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

`MaintenanceState`, `MaintenanceTask`, `ChangeOperation`, `ChangeFeedPayload`
and `WriterStatus` implement `String`.

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
    Jobs                 int
    InputSSTs            int
    OutputSSTs           int
    ForcedLevelCreations int
    DeepestForcedLevel   uint32
    OutputBytes          int64
    MovedJobs            int
    MovedBytes           int64
    ReadBytes            int64
    RewrittenBytes       int64
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

## Appendix: the blobstore package

Most applications should use `isledb.Open` or `isledb.OpenBucket`. The
`github.com/ankur-anand/isledb/blobstore` package is for storage adapters,
integration tests and operational tools.

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

`List` loads every matching object into memory; prefer `NewListIterator` or
`Walk` for large listings.

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

- Sentinel errors: `blobstore.ErrNotFound`, `blobstore.ErrPreconditionFailed`
  and `blobstore.ErrBucketNameRequired`.
- `Delete` succeeds for an object that does not exist.
- `BatchDeleteError` names each key that failed.
- For backends without native conditional writes (the in-memory and file
  stores), `Read` returns the MD5 of the content as the ETag, which is what
  `WriteIfMatch` compares against.

## Related documentation

- [Project overview](README.md)
- [Object-store schema](docs/object-store-schema.md)
