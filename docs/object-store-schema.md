# IsleDB object-store layout

IsleDB stores one database below one object-store prefix. For example, a
database opened with the prefix `demo/p000` owns the objects below
`demo/p000/`.

Applications use the IsleDB API rather than reading these objects directly.
This document is for operators who need to understand backups, permissions,
storage usage, retention, and recovery.

## Layout at a glance

```text
demo/p000/
  manifest/
    CURRENT
    snapshots/
      <id>.manifest.zst
    pages/
      l00/
        <page-id>.page.zst
      l01/
        <page-id>.page.zst
      ...
    gc/
      sst/plans/
        <plan-id>.json
      sst/ready/
        <not-before>-<plan-id>.json
      change-feed/plans/
        <plan-id>.json
      change-feed/ready/
        <not-before>-<plan-id>.json
      snapshots/
        <snapshot-path-hash>.json
      page-marks/
        <page-path-hash>.json
      pages/plans/
        <plan-id>.json
      pages/ready/
        <not-before>-<plan-id>.json
  maintenance/
    HEAD
  sstable/
    <bucket>/
      <sst-id>
  changes/
    <bucket>/
      <change-batch-id>
```

`changes/` is present only when the change feed is enabled. Empty directories
usually do not exist because object stores expose keys, not real directories.

| Object family | Purpose | Lifecycle |
| --- | --- | --- |
| `manifest/CURRENT` | Authoritative database head and visibility boundary | Updated with conditional writes; never deleted during normal operation |
| `manifest/snapshots/` | Complete metadata checkpoints used to bound database-open work | Immutable; old snapshots are reclaimed by maintenance |
| `manifest/pages/` | Immutable pages of committed manifest history | Immutable; unreachable pages are reclaimed by maintenance |
| `maintenance/HEAD` | Coordination mailbox between maintenance and the writer | Bounded mutable object; never swept as ordinary data |
| `sstable/` | Immutable key-value data files | Reclaimed after compaction or explicit history removal makes them unreachable |
| `changes/` | Immutable ordered mutation batches | Reclaimed according to configured change-feed retention |
| `manifest/gc/` | Durable, bounded proof and work records for safe physical deletion | Created and removed by maintenance |

## What makes data visible

Object existence does not make data visible. `manifest/CURRENT` is the only
authoritative database head.

A normal write is published in this order:

1. The writer uploads a new SST object.
2. If the change feed is enabled, it uploads the matching change-batch object.
3. The writer conditionally updates `manifest/CURRENT` to reference the new
   objects.
4. Readers see the write after loading that committed `CURRENT` generation.

The SST and change batch can therefore exist before they are visible. A failed
or interrupted publication can also leave an unreferenced immutable object.
Readers ignore such objects because they do not discover data by listing
`sstable/` or `changes/`.

The same rule applies to metadata. A snapshot or page becomes usable only when
the committed head references it.

## `manifest/CURRENT`

`manifest/CURRENT` is a small JSON object and the root of the database. It
contains:

- the object-layout version and manifest format;
- the next database epoch and manifest sequence;
- an optional snapshot reference;
- recent manifest entries and references to older manifest pages;
- writer and maintenance fencing state;
- the change-feed mode and retained feed floor, when enabled;
- the maximum lifetime of a reader's pinned view;
- bounded receipts used to make writer and maintenance retries idempotent.

The current format is:

```text
layout_version: 2
format: isledb-manifest-v2
```

`MaxPinnedViewAge` is a persisted store policy. It defaults to one hour and
defines how long an already-loaded reader view can remain usable. Later writers
must use the same value. This setting is a physical-deletion safety window; it
does not determine how much change-feed history is retained.

IsleDB updates this object with provider conditional-write semantics. A writer
must not replace it with an unconditional PUT, and operators must not edit it
by hand.

### Immutable object references

References from `CURRENT`, manifest pages, and maintenance commands identify an
immutable metadata object using:

| Field | Meaning |
| --- | --- |
| `path` | Exact object key, including the database prefix |
| `encoded_bytes` | Exact stored-object length |
| `checksum` | `sha256:` followed by 64 lowercase hexadecimal digits |
| `created_at` | UTC creation timestamp |

Readers verify the encoded length and SHA-256 before decoding a referenced
snapshot or manifest page.

Because references store exact object keys, a raw bucket copy should be
restored under the same database prefix. Copying the objects to a different
prefix does not rewrite embedded references.

## Manifest snapshots and pages

Snapshots and pages let IsleDB open large databases without placing the full
manifest history in `CURRENT`.

```text
manifest/snapshots/<id>.manifest.zst
manifest/pages/l<level>/h<seq-hi>-l<seq-lo>-<id>.page.zst
```

Both object types are immutable. They use a versioned `ISLM` envelope around a
zstd-compressed JSON payload. The envelope distinguishes snapshots from pages
and records the uncompressed size before decompression begins.

The current decoder limits are:

| Object | Maximum uncompressed payload |
| --- | ---: |
| Manifest page | 32 MiB |
| Manifest snapshot | 512 MiB |

Level `l00` pages contain committed manifest entries. Higher levels contain
references to lower-level pages. This hierarchy keeps `CURRENT` bounded as
history grows.

Page keys put the zero-padded `SeqHi` first because physical reclamation asks
whether a complete page ends below the retained manifest floor. Listing one
level therefore visits reclaimable ranges before protected ranges. The key is
only a conservative listing hint: maintenance downloads and validates every
page selected for deletion before removing it.

These objects are implementation-managed metadata. Operators should copy them
during backup, but should not decode, rewrite, rename, or delete them manually.

## SST objects

```text
sstable/<bucket>/<sst-id>
```

SSTs are immutable binary files containing keys, values, tombstones, sequence
numbers, and TTL metadata. Values are stored directly in SSTs; IsleDB does not
create a separate large-value blob object family.

The three-character hexadecimal bucket is derived deterministically from the
SST ID. Sharding prevents every SST from sharing one flat object-store prefix.
The manifest records the SST ID, and IsleDB computes the exact key without
listing the directory.

Depending on reader configuration, an SST may be downloaded into the local
cache or read through bounded range requests. The on-object checksum can be
validated when an SST is downloaded in full. Existing SSTs remain
self-describing and readable when compression or block-size settings change for
new output.

Compaction writes replacement SSTs before publishing the new topology through
`CURRENT`. Replaced SSTs remain physically present until the pinned-view safety
window has passed and maintenance deletes them.

## Change-feed objects

```text
changes/<bucket>/<change-batch-id>
```

Change batches exist only when the database is opened with change-feed support.
Each writer flush publishes one mutation batch alongside its SST. The batch
preserves mutation order, PUT and DELETE operations, and TTL metadata.

The configured payload mode controls PUT values:

| Mode | Stored in the change batch |
| --- | --- |
| `keys_only` | Key, operation, sequence, and TTL metadata; PUT value omitted |
| `full_values` | Key, operation, sequence, TTL metadata, and complete PUT value |

The payload mode is persisted in `CURRENT` and cannot be changed after the feed
has been enabled for that database prefix.

Change batches use independently compressed zstd blocks. A block normally
closes at 512 records or 1 MiB of uncompressed record data, whichever comes
first. A single record larger than the byte target is stored as one oversized
block so the record remains intact.

An index and trailer at the end of the object let `ChangeReader` range-read only
the blocks needed for the requested page. Each decoded block has its own
SHA-256 integrity check. Normal feed reads use the exact path recorded in the
manifest and do not list `changes/`.

Change-feed retention first advances the logical feed floor in `CURRENT`.
Physical deletion happens later, after existing pinned readers have had time to
expire. A cursor below the committed feed floor returns
`ErrChangeCursorExpired` even if an old batch object still exists physically.

## Maintenance coordination

```text
maintenance/HEAD
```

`maintenance/HEAD` is a bounded JSON mailbox. A maintenance process can prepare
compaction, checkpoint, and retention work, but only the fenced writer publishes
the resulting database state through `manifest/CURRENT`.

The mailbox holds at most one pending command. The writer applies or rejects
that command and records the result in `CURRENT` in the same conditional update
as the command's effect. Maintenance then reconciles the result and clears the
mailbox.

Readers never need `maintenance/HEAD`. Users normally interact with this flow
through the public maintenance API described in the [API guide](../api.md), not
through the JSON object.

## Garbage-collection records

The JSON objects below `manifest/gc/` are durable deletion work, not user data
and not temporary files:

```text
manifest/gc/sst/plans/<plan-id>.json
manifest/gc/sst/ready/<not-before>-<plan-id>.json
manifest/gc/change-feed/plans/<plan-id>.json
manifest/gc/change-feed/ready/<not-before>-<plan-id>.json
manifest/gc/snapshots/<snapshot-path-hash>.json
manifest/gc/page-marks/<page-path-hash>.json
manifest/gc/pages/plans/<plan-id>.json
manifest/gc/pages/ready/<not-before>-<plan-id>.json
```

SST, change-feed, and normal manifest-page handoffs use two immutable records. The hash-keyed `plans/`
record is the canonical retry anchor: reconciliation of the same receipt adopts
the first durable deadline instead of creating new work. The `ready/` record
contains the same validated plan under a fixed-width UTC `NotBefore` prefix.
Lexicographic listing therefore visits due plans first and stops before fetching
the first future plan. Both records are removed after target deletion succeeds.

SST and change-feed plans contain bounded lists of exact deletion targets.
Manifest-page plans contain a retained sequence floor and maximum page level,
so one constant-sized record can retire any number of pages folded by a
checkpoint. Reclamation re-reads `CURRENT`, rejects a regressed floor, validates
each selected page object, and only then deletes it. `page-marks/` remains the
low-frequency orphan-audit fallback for pages that were uploaded but never
became visible. All records carry safety deadlines so slow physical deletion
cannot race readers holding an older, still-valid view.

Each reclaim lane progresses independently. A slow SST delete does not prevent
change-feed or metadata cleanup from making progress, and deletion does not
block ordinary reads and writes. Publishing a new deletion plan signals its
lane without mutating lane-owned iterator state, so it also cannot block
compaction or checkpoint reconciliation behind object-store deletion. Empty
lanes and repeated failures back off to a bounded safety scan; a newly durable
plan wakes its lane promptly.

Do not delete GC records manually. Removing a record before its targets are
deleted can leave unreachable data in the bucket indefinitely.

## Backup and restore

A database backup must include the complete database prefix, including
`manifest/`, `maintenance/`, `sstable/`, `changes/`, and `manifest/gc/`.

For a simple consistent backup:

1. Stop every writer using the database prefix.
2. Stop maintenance and physical reclamation.
3. Copy the complete prefix.
4. Restore all objects under the same prefix.

Provider snapshots or versioned-bucket tooling can avoid a long application
pause, but the captured view must contain `CURRENT` and every immutable object
referenced by that generation. Copying only `CURRENT` is not a backup.

Provider object versioning, soft-delete, and incomplete multipart uploads are
outside IsleDB's logical object graph. Configure provider lifecycle rules for
those provider-owned versions or uploads separately.

## Operational rules

- Give normal readers GET access to manifest metadata, SSTs, and change batches.
- Give the writer read and conditional-write access to `manifest/CURRENT`, read
  access to `maintenance/HEAD`, and create access to SST, change-batch, and
  manifest-page objects.
- Give maintenance read access to the database, conditional-write access to
  `maintenance/HEAD`, and the list, create, and delete permissions required by
  its configured compaction and reclamation work.
- Do not apply a generic age-based delete rule to live IsleDB prefixes.
- Do not rename, rewrite, or manually remove immutable objects.
- Treat a missing object referenced by `CURRENT` as corruption or premature
  external deletion; an unreferenced object is not automatically corruption.
- Keep independent databases under independent prefixes.

Logical retention is controlled through IsleDB. Physical object count can lag
logical retention because deletion deliberately waits for pinned readers and
proceeds in bounded batches.
