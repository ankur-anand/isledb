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

- `manifest/CURRENT`, `manifest/snapshots/*.manifest`, `manifest/pages/**/*.json`, and `manifest/gc/*.json` are UTF-8 JSON.
- `sstable/*`, `changes/*`, and `blobs/*` are binary objects, not JSON.
- `MinKey`, `MaxKey`, and any other `[]byte` fields are base64-encoded by Go's JSON encoder.
- Manifest log `role` is numeric:
  - `0` = writer
  - `1` = compactor
- Top-level manifest fields use explicit `snake_case` JSON tags.
- Nested `SSTMeta`, `SortedRun`, `BloomMeta`, and `SSTSignature` fields currently serialize with Go's default exported field names, so the on-disk JSON uses keys like `ID`, `SeqLo`, `MinKey`, `Bloom`, and `HasBlobRefs`.

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

Hot control record and visibility boundary. It points to the current snapshot, the committed sequence window, bounded active entries, and immutable commit-page refs. It can also carry fast-path metadata such as `max_committed_lsn` and `low_watermark_lsn`.

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
      "seq": 420,
      "role": 0,
      "epoch": 18,
      "ts": "2026-04-15T10:14:11Z",
      "op": "add_sstable"
    }
  ],
  "max_committed_lsn": 381,
  "low_watermark_lsn": 240,
  "writer_fence": {
    "epoch": 18,
    "owner": "writer-p000",
    "claimed_at": "2026-04-15T10:12:01Z"
  },
  "compactor_fence": {
    "epoch": 7,
    "owner": "compactor-p000",
    "claimed_at": "2026-04-15T10:13:11Z"
  }
}
```

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
  "compactor_fence": {
    "epoch": 7,
    "owner": "compactor-p000",
    "claimed_at": "2026-04-15T10:13:11Z"
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
  "sorted_runs": [
    {
      "id": 12,
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
  ],
  "next_sorted_run_id": 13,
  "compaction_config": {
    "l0_compaction_threshold": 8,
    "min_sources": 4,
    "max_sources": 8,
    "size_threshold": 4
  }
}
```

Notes:

- `MinKey` and `MaxKey` are raw key bytes encoded as base64 in JSON.
- If your workload uses monotonic 8-byte big-endian keys, those bytes can be decoded into numeric LSNs or offsets.

## `manifest/pages/l<level>/<id>.json`

Immutable committed manifest pages. A page is visible only when
`manifest/CURRENT` references it through `index_frontier`. Candidate pages left
behind by a failed CURRENT CAS are ignored by readers and can be cleaned by GC.

Level `0` pages contain actual `ManifestLogEntry` objects. Higher levels contain
`PageRef` children that point at lower-level pages. This keeps `CURRENT` bounded
while allowing the committed history to grow.

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

Immutable, seq-ordered mutation batch written alongside a memtable flush. This
object is binary, not JSON. It preserves puts, deletes, TTL metadata, inline
values, and blob references in row-sequence order.

The bucket is deterministically derived from the change-batch ID using
`blobstore.ChangeBatchBucket`. Readers should not list `changes/` to discover
history. They read manifest metadata, then open the exact path from
`ChangeBatchMeta.Path`.

Path:

```text
demo/p000/changes/9f3/18-412-417-1776257651000000000.chg
```

Visibility rule:

```text
SST object + change batch object may exist before commit.
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
