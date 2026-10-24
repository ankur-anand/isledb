# stress

`stress` tests an isledb version end to end. It starts that version's
`isledb writer`, `isledb reader` and `isledb maintenance` against a bucket,
writes to the writer and reads from the readers over their HTTP APIs, checks
every answer a reader gives against what was written, and measures what
storage saw.

It imports nothing of isledb, so it tests any version from v0.17.0 on, the
first with the `isledb` service: a release tag, any git ref, or the working
tree.

```
cd tools/stress
go run . run -spec specs/local-smoke.yaml -isledb .          # the working tree
go run . run -spec specs/local-smoke.yaml -isledb v0.20.0    # a release
go run . compare results/*/summary.json
```

## What it checks

Every read is checked exactly, not against a range of acceptable answers:

- The load sends batches from a schedule that says, for every batch, which
  keys it writes and what. Values carry their own index, version, length and
  checksum.
- Each committed batch is recorded with the revision the writer answered.
  Every read answers with the revision it was served at, so the state of
  each key at that revision is known: the last batch writing it at or below
  that revision, or absent.
- A read answered above what the load has settled, with a batch still in
  flight that might be below it, waits until it settles. Nothing is skipped;
  a read that never settles is a failure.
- A scan page must hold exactly the keys present in its range at its
  revision: none missing, none extra, in order. Keys are a permutation of
  their index, so the keys in any range can be listed.
- A write whose outcome is unknown, because the writer died or storage lost
  the answer, is asked about, or sent again; either state is then accepted
  below the revision it finally committed at.

Failures are `missing`, `stale`, `future`, `resurrected` (a deleted key came
back), `corrupt`, `unexpected`, `order` and `unsettled`. Each is a line of
`failures.jsonl` with the key, the revision, what was seen, what was allowed
and the batches that wrote the key. A run with any failure exits non-zero.

## What it measures

- **Write amplification** from storage traffic: SST bytes uploaded through
  each role's latency proxy, divided by the key and value bytes written. It
  counts the first save of each byte, as RocksDB does. `writer` and
  `compaction` split it by who uploaded.
- **Latency** of commits, gets and scans, from the client side.
- **Writes refused** (`503`), never saved, and of unknown outcome.
- **Each role's metrics**, whatever the version exports, summed over their
  labels, in every sample.

Without a latency proxy (a `file://` bucket, or no `storage.latency`), there
are no storage counts, so no write amplification.

## A run's directory

Each run gets `results/<name>-<version>-<time>/`:

| File | What |
|---|---|
| `run.json` | the spec, the isledb build, the host and the key count |
| `series.jsonl` | a row of every measurement each `sample_every` |
| `summary.json` | each phase; the measurements at each checkpoint, settled and at the end |
| `failures.jsonl` | every wrong answer |
| `ledger` | every batch's outcome and revision |
| `logs/` | the output of every isledb process and proxy |
| `roles/` | the configuration each role ran with |

## Specs

```yaml
name: real6
workload:
  shape: random          # or sequential
  values: mixed          # or fixed:N
  target: 20GiB          # or keys: N
  batch_keys: 16
  delete_permille: 0     # share each overwrite round deletes
  rate: 12MiB            # bytes a second; 0 unlimited
  workers: 128           # batches in flight; each waits for its commit
readers:
  - {name: uniform, rate: 200, workers: 16, scan_ratio: 0.05, scan_limit: 100, dist: uniform}
storage:
  bucket_url: s3://isledb?endpoint={endpoint}&region=us-east-1&use_path_style=true
  endpoint: http://127.0.0.1:19000
  latency: {min: 15ms, max: 40ms}
isledb:
  readers: 1
  config:                # each role's section of the isledb configuration
    writer: {memtable: {target_bytes: 3MiB}, flush: {interval: 250ms}}
    maintenance: {compaction: {l0_to_base_ratio: 4, l0_max_depth: 64, base_level_bytes: 4GiB}}
phases:
  - load:                # the insert round
      events: [{after: 2m, role: writer, kill: true, down: 5s}]
      faults: [{after: 1m, proxy: writer, set: {methods: [PUT], kinds: [manifest], drop_rate: 0.05}}]
  - update: {rounds: 1}  # overwrite rounds
  - settle: {quiet: 5m, max: 1h}
  - restart: {role: maintenance, isledb: main}
  - verify: {parts: 32}
checkpoints: [1GiB, 5GiB, 10GiB, 20GiB]
```

- `isledb.config` is passed to each role as its configuration. The runner
  sets `prefix`, the addresses, the directories and auth itself. A version
  that doesn't know a setting refuses it, and the run stops before it starts.
- `events` stop or kill a role while a phase writes, and restart it, maybe
  at another version. `restart` does the same as a phase of its own.
- `faults` set what a role's latency proxy injects while the phase runs:
  `error_rate` refuses requests, `drop_rate` forwards them and loses the
  answer, `extra_delay` slows them; `methods` and `kinds` (`sst`,
  `manifest`, `current`, `other`) choose which. They are cleared when the
  phase ends.

`specs/` has `local-smoke` (a directory bucket, a minute), `silo-smoke`,
`silo-faults`, and `real-defaults` and `real6`: the 20 GiB, 12 MiB/s run at
the default compaction settings and with fewer rewrites.

## Storage

`deploy/up.sh` starts SILO, an S3-compatible server, in Docker, with its data
under `STRESS_DATA`, and creates the bucket:

```
STRESS_DATA=/nvme/stress deploy/up.sh
export AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin AWS_REGION=us-east-1
go run . run -spec specs/real-defaults.yaml -isledb v0.20.0
```

Each run uses its own prefix in the bucket, so runs never see each other's
data. The proxies are built from `tools/latproxy` in the repository.
