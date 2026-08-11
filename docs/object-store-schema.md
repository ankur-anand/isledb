# Object Store Schema

`isledb` stores all durable state under one object-store prefix.

If the prefix is `demo/p000`, the object layout can include:

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
    gc/
      sst/ready/
        <plan-id>.json
      change-feed/ready/
        <plan-id>.json
      snapshots/
        <snapshot-path-hash>.json
      pages/
        <page-path-hash>.json
  maintenance/
    HEAD
  sstable/
    <bucket>/
      <sst-id>
  changes/
    <bucket>/
      <change-batch-id>
```

## Serialization Notes

- `manifest/CURRENT`, `maintenance/HEAD`, and objects below `manifest/gc/` are UTF-8 JSON.
- `manifest/snapshots/*.manifest.zst` and `manifest/pages/**/*.page.zst` use the
  versioned `ISLM` binary envelope. Their payload is UTF-8 JSON compressed with
  zstd. References carry the exact encoded size and SHA-256 of the complete
  stored envelope; its header carries the bounded raw size.
- `sstable/*` and `changes/*` are binary objects, not JSON.
- `changes/*` stores mutation batches opened by the public `ChangeReader`.
- `MinKey`, `MaxKey`, and any other `[]byte` fields are base64-encoded by Go's JSON encoder.
- Manifest log `role` is `0` for writer-owned publication. Applied maintenance
  entries are also writer-owned because only the writer updates `CURRENT`.
- Top-level manifest fields use explicit `snake_case` JSON tags.
- Manifest fields use explicit `snake_case` JSON tags. `Level` uses explicit
  `number` and `ssts` fields.

The fixed 16-byte `ISLM` envelope is:

| Offset | Bytes | Field |
| --- | ---: | --- |
| 0 | 4 | Magic `ISLM` |
| 4 | 1 | Envelope version (`1`) |
| 5 | 1 | Object kind (`1` snapshot, `2` page) |
| 6 | 1 | Codec (`1` zstd) |
| 7 | 1 | Reserved flags (`0`) |
| 8 | 8 | Uncompressed JSON length, unsigned big-endian |

Readers verify the reference's encoded size and SHA-256 before decompressing,
then enforce the declared raw length as the decoder's output limit.

## Directory View

This is a representative JSON-style view of the object families under one prefix:

```json
{
  "demo/p000": {
    "manifest": {
      "CURRENT": "{...json...}",
      "snapshots": {
        "<id>.manifest.zst": "<ISLM envelope + zstd JSON>"
      },
      "pages": {
        "l00": {
          "<page-id>.page.zst": "<ISLM envelope + zstd JSON>"
        },
        "l01": {
          "<page-id>.page.zst": "<ISLM envelope + zstd JSON>"
        }
      },
      "gc": {
        "sst": { "ready": {
          "<plan-id>.json": "{...json...}"
        }},
        "change-feed": { "ready": {
          "<plan-id>.json": "{...json...}"
        }},
        "snapshots": {
          "<snapshot-path-hash>.json": "{...json...}"
        },
        "pages": {
          "<page-path-hash>.json": "{...json...}"
        }
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
  "layout_version": 2,
  "format": "isledb-manifest-v2",
  "snapshot": {
    "path": "demo/p000/manifest/snapshots/0ujsszwN8NRY24YaXiTIE2VWDTS.manifest.zst",
    "encoded_bytes": 65168,
    "checksum": "sha256:abc",
    "created_at": "2026-04-15T10:14:11Z"
  },
  "log_seq_start": 412,
  "change_feed_enabled": true,
  "change_feed_payload": "full_values",
  "change_feed_log_start": 412,
  "next_seq": 428,
  "next_epoch": 19,
  "index_frontier": [
    {
      "level": 0,
      "seq_lo": 412,
      "seq_hi": 419,
      "path": "demo/p000/manifest/pages/l00/412-419-2YBx.page.zst",
      "count": 8,
      "encoded_bytes": 6350,
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
        "compression": "zstd",
        "payload": "full_values"
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
  "layout_version": 2,
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

## `manifest/snapshots/<id>.manifest.zst`

Optional full manifest snapshot describing the complete visible SST topology at a point in time.

Path:

```text
demo/p000/manifest/snapshots/0ujsszwN8NRY24YaXiTIE2VWDTS.manifest.zst
```

Decompressed JSON payload example:

```json
{
  "version": 2,
  "next_epoch": 19,
  "log_seq": 411,
  "writer_fence": {
    "epoch": 18,
    "owner": "writer-p000",
    "claimed_at": "2026-04-15T10:12:01Z"
  },
  "l0_ssts": [
    {
      "id": "sst-a100",
      "epoch": 18,
      "seq_lo": 400,
      "seq_hi": 405,
      "min_key": "AAAAAAAAAWg=",
      "max_key": "AAAAAAAAAW0=",
      "size": 1048576,
      "checksum": "sha256:abc",
      "bloom": {
        "bits_per_key": 0,
        "k": 0,
        "offset": 0,
        "length": 0
      },
      "created_at": "2026-04-15T10:14:01Z",
      "level": 0
    }
  ],
  "levels": [
    {
      "number": 1,
      "ssts": [
        {
          "id": "sst-b010",
          "epoch": 17,
          "seq_lo": 290,
          "seq_hi": 320,
          "min_key": "AAAAAAAAAPA=",
          "max_key": "AAAAAAAAARc=",
          "size": 8388608,
          "checksum": "sha256:def",
          "bloom": {
            "bits_per_key": 0,
            "k": 0,
            "offset": 0,
            "length": 0
          },
          "created_at": "2026-04-15T09:58:00Z",
          "level": 1
        }
      ]
    }
  ]
}
```

Notes:

- `MinKey` and `MaxKey` are raw key bytes encoded as base64 in JSON.
- If your workload uses monotonic 8-byte big-endian keys, those bytes can be decoded into numeric positions or offsets.

## `manifest/pages/l<level>/<id>.page.zst`

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
demo/p000/manifest/pages/l00/412-419-2YBx.page.zst
demo/p000/manifest/pages/l01/412-1435-7Kq9.page.zst
```

Decompressed level 0 page example:

```json
{
  "layout_version": 2,
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
  "layout_version": 2,
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
      "path": "demo/p000/manifest/pages/l00/412-419-2YBx.page.zst",
      "count": 8,
      "encoded_bytes": 6350,
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

## `manifest/gc/sst/ready/<plan-id>.json`

Immutable, bounded deletion plan for SSTs already retired by committed
manifest entries.

Path:

```text
demo/p000/manifest/gc/sst/ready/9db6....json
```

Example:

```json
{
  "version": 1,
  "kind": "sst_retirement",
  "plan_id": "9db6...",
  "checksum": "sha256:abc",
  "source": {
    "command_id": "compact-414",
    "epoch": 8,
    "generation": 41
  },
  "applied_at": "2026-04-15T10:14:11Z",
  "observed_at": "2026-04-15T10:14:12Z",
  "pinned_view_age_nanos": 3600000000000,
  "safety_margin_nanos": 60000000000,
  "not_before": "2026-04-15T11:15:12Z",
  "target_count": 1,
  "target_bytes": 67108864,
  "targets": [
    {
      "id": "sst-old-001",
      "key": "demo/p000/sstable/a3/sst-old-001",
      "size": 67108864
    }
  ]
}
```

## `manifest/gc/change-feed/ready/<plan-id>.json`

Immutable checksummed plan containing a bounded, sequence-ordered list of exact
change-batch targets, the committed feed floor, byte accounting, and an
absolute deletion deadline. Before publication, those targets live in the
existing `maintenance/HEAD` floor command. After the writer receipt proves the
floor was applied, maintenance writes this plan with a deadline that honors
`MaxPinnedViewAge`. Physical deletion reloads `CURRENT`, requires the complete
target floor, and removes the plan only after every target delete succeeds.

## `manifest/gc/snapshots/<snapshot-path-hash>.json`

Per-snapshot retirement marker. It records the exact snapshot path and object
identity, retirement reason, and an absolute `not_before` deadline derived
from the persisted pinned-view policy. The manifest reclaimer reloads CURRENT
and the pending checkpoint before deleting the snapshot and then its marker.

## `manifest/gc/pages/<page-path-hash>.json`

Per-page quarantine marker containing the complete checksummed `PageRef`, the
observation time, retained floor, reason, and absolute deadline. A due delete
reloads CURRENT, follows only the candidate's containing index branch, and
requires the page to remain unreachable and completely below the committed
replay floor. Corrupt or ambiguous candidates are retained.

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
mutation order. A `full_values` feed embeds complete values, including values
stored externally by the KV path. A `keys_only` feed stores PUT keys and
operation metadata with an explicit value-omitted flag.

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
stores the batch identity, payload policy, and a SHA-256 of the complete block
index; the same payload policy and index checksum are anchored in the committed
manifest metadata.

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
