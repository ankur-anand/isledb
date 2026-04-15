# Object Store Schema

`isledb` stores all durable state under one object-store prefix.

If the prefix is `demo/p000`, the object layout can include:

```text
demo/p000/
  manifest/
    CURRENT
    snapshots/
      <id>.manifest
    log/
      <seq>.json
    gc/
      pending-sst/
        pending.json
      checkpoint.json
  sstable/
    <sst-id>
  blobs/
    <prefix>/
      <blob-id>.blob
```

## Serialization Notes

- `manifest/CURRENT`, `manifest/snapshots/*.manifest`, `manifest/log/*.json`, and `manifest/gc/*.json` are UTF-8 JSON.
- `sstable/*` and `blobs/*` are binary objects, not JSON.
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
      "log": {
        "<seq>.json": "{...json...}"
      },
      "gc": {
        "pending-sst": {
          "pending.json": "{...json...}"
        },
        "checkpoint.json": "{...json...}"
      }
    },
    "sstable": {
      "<sst-id>": "<binary>"
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

Hot control record. It points to the current snapshot and replay window and can carry fast-path metadata such as `max_committed_lsn` and `low_watermark_lsn`.

Path:

```text
demo/p000/manifest/CURRENT
```

Example:

```json
{
  "snapshot": "demo/p000/manifest/snapshots/0ujsszwN8NRY24YaXiTIE2VWDTS.manifest",
  "log_seq_start": 412,
  "next_seq": 428,
  "next_epoch": 19,
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

## `manifest/log/<seq>.json`

Incremental manifest mutation after the snapshot boundary. Each file holds one `ManifestLogEntry`.

Path:

```text
demo/p000/manifest/log/00000000000000000412.json
```

Common header fields:

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

### `op = "add_sstable"`

```json
{
  "id": "2YBxg5dN8nH4A4Z6Q8v6V8sC7rT",
  "seq": 412,
  "role": 0,
  "epoch": 18,
  "ts": "2026-04-15T10:14:11Z",
  "op": "add_sstable",
  "sstable": {
    "ID": "sst-a101",
    "Epoch": 18,
    "SeqLo": 412,
    "SeqHi": 417,
    "MinKey": "AAAAAAAAAXQ=",
    "MaxKey": "AAAAAAAAAXk=",
    "Size": 1048576,
    "Checksum": "sha256:ghi",
    "Signature": null,
    "Bloom": {
      "BitsPerKey": 0,
      "K": 0,
      "Offset": 0,
      "Length": 0
    },
    "CreatedAt": "2026-04-15T10:14:11Z",
    "Level": 0,
    "HasBlobRefs": false
  }
}
```

### `op = "remove_sstables"`

```json
{
  "id": "2YBxhK8H0x8wyKj9kM8vT4W8f2B",
  "seq": 414,
  "role": 1,
  "epoch": 7,
  "ts": "2026-04-15T10:16:00Z",
  "op": "remove_sstables",
  "remove_sstable_ids": [
    "sst-old-001",
    "sst-old-002"
  ]
}
```

### `op = "compaction"`

```json
{
  "id": "2YBxiwH4H6U4m2Q1Qx4T2m2qT3p",
  "seq": 413,
  "role": 1,
  "epoch": 7,
  "ts": "2026-04-15T10:14:20Z",
  "op": "compaction",
  "compaction": {
    "remove_sstable_ids": [
      "sst-a100",
      "sst-a101"
    ],
    "add_sstables": [],
    "add_sorted_run": {
      "id": 13,
      "ssts": [
        {
          "ID": "sst-c001",
          "Epoch": 18,
          "SeqLo": 400,
          "SeqHi": 417,
          "MinKey": "AAAAAAAAAWg=",
          "MaxKey": "AAAAAAAAAXk=",
          "Size": 2097152,
          "Checksum": "sha256:jkl",
          "Signature": null,
          "Bloom": {
            "BitsPerKey": 0,
            "K": 0,
            "Offset": 0,
            "Length": 0
          },
          "CreatedAt": "2026-04-15T10:14:20Z",
          "Level": 1,
          "HasBlobRefs": false
        }
      ]
    }
  }
}
```

### `op = "fence_claim"`

```json
{
  "id": "2YBxk6fW9l5cgY2sS5pRrQp1M8a",
  "seq": 415,
  "role": 0,
  "epoch": 19,
  "ts": "2026-04-15T10:17:00Z",
  "op": "fence_claim",
  "fence_claim": {
    "role": 0,
    "epoch": 19,
    "owner": "writer-p000",
    "claimed_at": "2026-04-15T10:17:00Z"
  }
}
```

### `op = "checkpoint"`

This variant embeds a full manifest payload under `checkpoint`.

```json
{
  "id": "2YBxl9Zl7o3Wk4L5jG6nP2mV8Qd",
  "seq": 416,
  "role": 0,
  "epoch": 19,
  "ts": "2026-04-15T10:18:00Z",
  "op": "checkpoint",
  "checkpoint": {
    "version": 1,
    "next_epoch": 19,
    "log_seq": 416,
    "l0_ssts": [],
    "sorted_runs": [],
    "next_sorted_run_id": 13,
    "compaction_config": {
      "l0_compaction_threshold": 8,
      "min_sources": 4,
      "max_sources": 8,
      "size_threshold": 4
    }
  }
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

## `sstable/<sst-id>`

Immutable SST data file. This object is binary, not JSON.

Path:

```text
demo/p000/sstable/sst-a100
```

JSON-style descriptor:

```json
{
  "path": "demo/p000/sstable/sst-a100",
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
