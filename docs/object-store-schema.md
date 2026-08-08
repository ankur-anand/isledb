# Object Store Schema

`isledb` stores all durable state under one object-store prefix.

If the prefix is `demo/p000`, the object layout can include:

```text
demo/p000/
  manifest/
    CURRENT
    snapshots/
      <id>.manifest
    pages/
      l00/
        <page-id>.json
      l01/
        <page-id>.json
    gc/
      pending-sst/
        pending.json
      checkpoint.json
  maintenance/
    HEAD
  sstable/
    <bucket>/
      <sst-id>
  changes/
    <bucket>/
      <change-batch-id>
  blobs/
    <prefix>/
      <blob-id>.blob
```

## Serialization Notes

- `manifest/CURRENT`, `maintenance/HEAD`, `manifest/snapshots/*.manifest`,
  `manifest/pages/**/*.json`, and `manifest/gc/*.json` are UTF-8 JSON.
- `sstable/*`, `changes/*`, and `blobs/*` are binary objects, not JSON.
- `changes/*` stores mutation batches opened by the public `ChangeReader`.
- `MinKey`, `MaxKey`, and any other `[]byte` fields are base64-encoded by Go's JSON encoder.
- Manifest log `role` is `0` for writer-owned publication. Applied maintenance
  entries are also writer-owned because only the writer updates `CURRENT`.
- Top-level manifest fields use explicit `snake_case` JSON tags.
- Nested `SSTMeta`, `BloomMeta`, and `SSTSignature` fields currently serialize with Go's default exported field names, so the on-disk JSON uses keys like `ID`, `SeqLo`, `MinKey`, `Bloom`, and `HasBlobRefs`. `Level` uses explicit `number` and `ssts` fields.

## Directory View

This is a representative JSON-style view of the object families under one prefix:

```json
{
  "demo/p000": {
    "manifest": {
      "CURRENT": "{...json...}",
      "snapshots": {
        "<id>.manifest": "{...json...}"
      },
      "pages": {
        "l00": {
          "<page-id>.json": "{...json...}"
        },
        "l01": {
          "<page-id>.json": "{...json...}"
        }
      },
      "gc": {
        "pending-sst": {
          "pending.json": "{...json...}"
        },
        "checkpoint.json": "{...json...}"
      }
    },
    "maintenance": {
      "HEAD": "{...json...}"
    },
    "sstable": {
      "<bucket>": {
        "<sst-id>": "<binary>"
      }
    },
    "changes": {
      "<bucket>": {
        "<change-batch-id>": "<binary>"
      }
    },
    "blobs": {
      "<prefix>": {
        "<blob-id>.blob": "<binary>"
      }
    }
  }
}
```

## `manifest/CURRENT`

Hot control record and visibility boundary. It points to the current snapshot, the committed sequence window, bounded active entries, and immutable commit-page refs.

Path:

```text
demo/p000/manifest/CURRENT
```

Example:

```json
{
  "layout_version": 1,
  "format": "isledb-manifest-v1",
  "snapshot": "demo/p000/manifest/snapshots/0ujsszwN8NRY24YaXiTIE2VWDTS.manifest",
  "log_seq_start": 412,
  "change_feed_enabled": true,
  "change_feed_log_start": 412,
  "next_seq": 428,
  "next_epoch": 19,
  "index_frontier": [
    {
      "level": 0,
      "seq_lo": 412,
      "seq_hi": 419,
      "path": "demo/p000/manifest/pages/l00/412-419-2YBx.json",
      "count": 8,
      "checksum": "sha256:abc",
      "created_at": "2026-04-15T10:14:11Z"
    }
  ],
  "active_entries": [
    {
      "id": "2YBxg5dN8nH4A4Z6Q8v6V8sC7rT",
      "commit_id": "39xAPN6YtrMhPX69wjUb4V3S3xA",
      "seq": 420,
      "role": 0,
      "epoch": 18,
      "ts": "2026-04-15T10:14:11Z",
      "op": "add_sstable",
      "change_batch": {
        "id": "18-9001-9256-1776257651000000000.chg",
        "path": "demo/p000/changes/9f3/18-9001-9256-1776257651000000000.chg",
        "epoch": 18,
        "seq_lo": 9001,
        "seq_hi": 9256,
        "count": 256,
        "block_count": 1,
        "size": 118420,
        "raw_size": 276520,
        "checksum": "sha256:def",
        "index_checksum": "sha256:abc",
        "version": 1,
        "compression": "zstd"
      }
    }
  ],
  "writer_fence": {
    "epoch": 18,
    "owner": "writer-p000",
    "claimed_at": "2026-04-15T10:12:01Z"
  },
  "last_writer_commit": {
    "commit_id": "39xAPN6YtrMhPX69wjUb4V3S3xA",
    "fingerprint": "sha256:8e5d8f3b9f2c7ad9b143f602b54abdc3c5166c118ad6e52f63caed592a9b45fe",
    "entry_id": "2YBxg5dN8nH4A4Z6Q8v6V8sC7rT",
    "manifest_seq": 420,
    "writer_epoch": 18,
    "seq_lo": 9001,
    "seq_hi": 9256,
    "committed_at": "2026-04-15T10:14:11Z"
  },
  "maintenance_receipt": {
    "command_id": "2YCueqzBfVBMErstfm3QxW8PbXQ",
    "epoch": 7,
    "generation": 42,
    "status": "applied",
    "applied_at": "2026-04-15T10:14:12Z"
  }
}
```

`last_writer_commit` is a bounded receipt for the newest writer commit. A
writer keeps one stable `commit_id` while retrying a memtable publication. If a
`CURRENT` CAS succeeds but its response is lost, the retry compares the receipt
and metadata fingerprint and returns the already committed entry instead of
appending a duplicate. Maintenance publication preserves this receipt.

`maintenance_receipt` identifies the latest command applied from
`maintenance/HEAD`. The command effect and receipt share one `CURRENT` CAS.

## `maintenance/HEAD`

Bounded maintenance mailbox. Maintenance owns this CAS object; the writer
polls it on the flush path. It contains at most one pending command.

```json
{
  "layout_version": 1,
  "epoch": 7,
  "owner_id": "maintenance-p000",
  "claimed_at": "2026-04-15T10:13:11Z",
  "generation": 42,
  "pending": {
    "id": "2YCueqzBfVBMErstfm3QxW8PbXQ",
    "epoch": 7,
    "generation": 42,
    "kind": "compaction",
    "created_at": "2026-04-15T10:14:12Z",
    "compaction": {
      "payload": {
        "remove_sstable_ids": ["sst-a100", "sst-a101"],
        "source_level": 0,
        "destination_level": 1,
        "add_sstables": []
      }
    }
  }
}
```

Readers never fetch this object. See
[Maintenance Publication](maintenance-publication.md) for ownership and crash
recovery.

## `manifest/snapshots/<id>.manifest`

Optional full manifest snapshot describing the complete visible SST topology at a point in time.

Path:

```text
demo/p000/manifest/snapshots/0ujsszwN8NRY24YaXiTIE2VWDTS.manifest
```

Example:

```json
{
  "version": 1,
  "next_epoch": 19,
  "log_seq": 411,
  "writer_fence": {
    "epoch": 18,
    "owner": "writer-p000",
    "claimed_at": "2026-04-15T10:12:01Z"
  },
  "l0_ssts": [
    {
      "ID": "sst-a100",
      "Epoch": 18,
      "SeqLo": 400,
      "SeqHi": 405,
      "MinKey": "AAAAAAAAAWg=",
      "MaxKey": "AAAAAAAAAW0=",
      "Size": 1048576,
      "Checksum": "sha256:abc",
      "Signature": null,
      "Bloom": {
        "BitsPerKey": 0,
        "K": 0,
        "Offset": 0,
        "Length": 0
      },
      "CreatedAt": "2026-04-15T10:14:01Z",
      "Level": 0,
      "HasBlobRefs": false
    }
  ],
  "levels": [
    {
      "number": 1,
      "ssts": [
        {
          "ID": "sst-b010",
          "Epoch": 17,
          "SeqLo": 290,
          "SeqHi": 320,
          "MinKey": "AAAAAAAAAPA=",
          "MaxKey": "AAAAAAAAARc=",
          "Size": 8388608,
          "Checksum": "sha256:def",
          "Signature": null,
          "Bloom": {
            "BitsPerKey": 0,
            "K": 0,
            "Offset": 0,
            "Length": 0
          },
          "CreatedAt": "2026-04-15T09:58:00Z",
          "Level": 1,
          "HasBlobRefs": false
        }
      ]
    }
  ]
}
```

Notes:

- `MinKey` and `MaxKey` are raw key bytes encoded as base64 in JSON.
- If your workload uses monotonic 8-byte big-endian keys, those bytes can be decoded into numeric positions or offsets.

## `manifest/pages/l<level>/<id>.json`

Immutable committed manifest pages. A page is visible only when
`manifest/CURRENT` references it through `index_frontier`. Candidate pages left
behind by a failed CURRENT CAS are ignored by readers and can be cleaned by GC.

Level `0` pages contain actual `ManifestLogEntry` objects. Higher levels contain
`PageRef` children that point at lower-level pages. This keeps `CURRENT` bounded
while allowing the committed history to grow. A level `0` page normally holds
up to the active-entry limit, but it may contain fewer entries when `CURRENT`
crosses its byte limit. If one entry is larger than that limit, the complete
active tail, including that entry, is stored in a level `0` page rather than
directly in `CURRENT`.

Path:

```text
demo/p000/manifest/pages/l00/412-419-2YBx.json
demo/p000/manifest/pages/l01/412-1435-7Kq9.json
```

Level 0 page example:

```json
{
  "layout_version": 1,
  "page_type": "commit_l00",
  "level": 0,
  "seq_lo": 412,
  "seq_hi": 419,
  "count": 8,
  "entries": [
    {
      "id": "2YBxg5dN8nH4A4Z6Q8v6V8sC7rT",
      "seq": 412,
      "role": 0,
      "epoch": 18,
      "ts": "2026-04-15T10:14:11Z",
      "op": "add_sstable"
    }
  ],
  "created_at": "2026-04-15T10:14:11Z"
}
```

Index page example:

```json
{
  "layout_version": 1,
  "page_type": "commit_index",
  "level": 1,
  "seq_lo": 412,
  "seq_hi": 1435,
  "count": 1024,
  "children": [
    {
      "level": 0,
      "seq_lo": 412,
      "seq_hi": 419,
      "path": "demo/p000/manifest/pages/l00/412-419-2YBx.json",
      "count": 8,
      "checksum": "sha256:abc",
      "created_at": "2026-04-15T10:14:11Z"
    }
  ],
  "created_at": "2026-04-15T10:30:00Z"
}
```

Common `ManifestLogEntry` header fields:

```json
{
  "id": "2YBxg5dN8nH4A4Z6Q8v6V8sC7rT",
  "seq": 412,
  "role": 0,
  "epoch": 18,
  "ts": "2026-04-15T10:14:11Z",
  "op": "add_sstable"
}
```

## `manifest/gc/pending-sst/pending.json`

Pending-delete mark set for SST cleanup.

Path:

```text
demo/p000/manifest/gc/pending-sst/pending.json
```

Example:

```json
{
  "version": 1,
  "marks": [
    {
      "version": 1,
      "sst_id": "sst-old-001",
      "first_seen_unreferenced_at": "2026-04-15T09:40:00Z",
      "last_seen_unreferenced_at": "2026-04-15T09:40:00Z",
      "first_seen_seq": 414,
      "last_seen_seq": 414,
      "first_reason": "retention_fifo",
      "last_reason": "retention_fifo",
      "has_blob_refs": false,
      "due_at": "2026-04-15T09:50:00Z"
    }
  ]
}
```

## `manifest/gc/checkpoint.json`

Replay checkpoint for GC mark catch-up.

Path:

```text
demo/p000/manifest/gc/checkpoint.json
```

Example:

```json
{
  "version": 1,
  "last_applied_seq": 428,
  "last_seen_log_seq_start": 412,
  "updated_at": "2026-04-15T10:15:00Z"
}
```

## `sstable/<bucket>/<sst-id>`

Immutable SST data file. This object is binary, not JSON.

The bucket is deterministically derived from the SST ID using IsleDB's
`blobstore.SSTBucket` function. Readers do not list `sstable/` to find files;
they read the manifest, get `SSTMeta.ID`, compute `SSTPath(ID)`, and range-read
the SST object directly.

Path:

```text
demo/p000/sstable/9e6/seg1.sst
```

JSON-style descriptor:

```json
{
  "path": "demo/p000/sstable/9e6/seg1.sst",
  "encoding": "binary",
  "written_by": [
    "writer",
    "compactor"
  ],
  "read_by": [
    "reader",
    "compactor",
    "retention_compactor"
  ],
  "contents": "immutable sstable bytes"
}
```

## `changes/<bucket>/<change-batch-id>`

Immutable, seq-ordered mutation batch opened by `ChangeReader`. This object is
an indexed binary file, not JSON. It preserves puts, deletes, TTL metadata, and
complete values in mutation order. External values are embedded so retained
change history does not depend on the lifetime of a separate blob object.

Indexed format version 1 layout:

```text
independent zstd frame: records 0-511
independent zstd frame: records 512-1023
...
fixed-size block index entries
96-byte trailer
```

A block closes at 512 records or 1 MiB of uncompressed record data, whichever
comes first; one oversized record remains independently decodable. Each index
entry stores the first record index, record count, first sequence, object
offset, compressed size, raw size, and SHA-256 of the raw block. The trailer
stores the batch identity and a SHA-256 of the complete block index; the same
index checksum is anchored in the committed manifest metadata.

`ChangeReader` first range-reads the index and trailer, then coalesces the
contiguous block frames needed by `MaxChanges` and `MaxBytes` into a range GET.
It verifies the index and every decompressed block without downloading or
decoding unrelated blocks.

The bucket is deterministically derived from the change-batch ID. Readers use
the exact path committed in the manifest; normal feed reads never list this
prefix.

Path:

```text
demo/p000/changes/9f3/18-412-417-1776257651000000000.chg
```

Visibility rule:

```text
The SST and change batch upload concurrently and may exist before commit.
They become visible only when manifest/CURRENT commits the add_sstable entry.
```

## `blobs/<prefix>/<blob-id>.blob`

External value object for out-of-line large values. This object is binary, not JSON.

Path:

```text
demo/p000/blobs/ab/ab34ef...c1.blob
```

JSON-style descriptor:

```json
{
  "path": "demo/p000/blobs/ab/ab34ef...c1.blob",
  "encoding": "binary",
  "written_by": [
    "writer"
  ],
  "read_by": [
    "reader"
  ],
  "contents": "blob value bytes"
}
```
