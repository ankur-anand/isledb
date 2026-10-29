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
go run . run -spec specs/local-smoke.yaml                                  # the working tree
go run . run -spec specs/default.yaml -target 20GiB -isledb v0.20.0        # a release
go run . run -spec specs/bulk-load.yaml -target 1TiB -isledb origin/main -fetch
go run . compare results/*/summary.json
```

A spec is a profile: the workload, the readers and the isledb settings. The
size comes from `-target`, so one profile runs at every size the same way,
and `-rate` changes the write rate.

## Which isledb

`-isledb` names the build under test:

| Value | Build |
|---|---|
| `.`, the default | the working tree as it is, uncommitted changes included, built every run |
| a path to a file | that binary |
| any git ref: `v0.20.0`, `main`, `origin/main`, a commit | built from that commit in a temporary worktree, which leaves your checkout alone, and kept in `results/.bin/` by commit for the next run |

Refs are resolved in the repository `-repo` names, by default the one the
command runs in, without fetching anything: `main` is your local branch.
With `-fetch`, origin's branches are fetched first, so `origin/main` is
GitHub's, and a tag is the commit origin has it at, fetched without changing
any local tag; a local tag that points elsewhere is reported. `run.json`
records the ref asked for, the commit built, and what the binary reports.

## What it checks

Every read is checked exactly, not against a range of acceptable answers:

- The load sends batches from a schedule that says, for every batch, which
  keys it writes and what. Values carry their own index, version, length and
  checksum.
- Round 0 inserts every key. Each update round writes a seeded share of the
  keys that exist (`update_permille`, all of them by default), or more of
  those in a hot key range, deletes a share of what it writes, and inserts
  new keys (`insert_permille`). Whether a round writes a key is a hash of
  the round and the key, so the last round to write any key, and the batch
  that did, can always be computed.
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
- A write whose outcome is unknown is asked about until the writer settles
  it. One lost with its connection, which has no write ID to ask about, is
  sent again, and either state is accepted below the revision it finally
  committed at; the next round waits a minute after it, so a late commit
  of the lost attempt can't land on top of the next round's write.
- A verify first waits until every reader answers at the newest revision
  written, so it checks everything, not a view a moment old.

Failures are `missing`, `stale`, `future`, `resurrected` (a deleted key came
back), `corrupt`, `unexpected`, `order` and `unsettled`. Each is a line of
`failures.jsonl` with the key, the revision, what was seen, what was allowed
and the batches that wrote the key. A run with any failure exits non-zero.

## What it measures

- **Write amplification** from storage traffic: SST bytes uploaded through
  each role's latency proxy, divided by the key and value bytes written. It
  counts the first save of each byte, as RocksDB does. `writer` and
  `compaction` split it by who uploaded.
- **Latency** of commits, gets and scans, from the client side. Reads are
  paced open loop: each is due at its time whether or not the workers keep
  up. Response time counts from when a read was due, the wait for a free
  worker included; service time from when it was sent. A read due longer
  ago than `max_lag` (5s) when a worker frees up is dropped and counted, so
  the samples show reads offered against achieved, and how busy the workers
  were.
- **Writes refused** (`503`), never saved, and of unknown outcome.
- **Space amplification**: the SST bytes in the bucket, from a listing every
  `storage.list_every` (5m) and at the end, divided by the key and value
  bytes the database holds. Listing works for `file://` buckets, and for
  `s3://` with `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` set; it goes
  straight to storage, so it isn't counted as a role's traffic.
- **Each role's metrics**, whatever the version exports, summed over their
  labels, in every sample.

Without a latency proxy (a `file://` bucket, or no `storage.latency`), there
are no storage counts, so no write amplification.

Every sample also logs a line: phase, bytes written, rate, time left, write
and space amplification, disk use, refused writes, commit p99 and the
verification counts.

## A run's directory

Each run gets `results/<name>-<size>-<version>-<time>/`, such as
`default-20GiB-v0.20.0-20261026T060000Z`:

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
name: bulk-load
workload:
  shape: random          # or sequential
  values: mixed          # or fixed:N
  target: 20GiB          # or keys: N
  batch_keys: 16
  update_permille: 20    # share of keys each update round writes; all when unset
  delete_permille: 100   # share of those it deletes instead
  insert_permille: 10    # new keys each update round adds, of the first round's
  hot: {from_permille: 0, to_permille: 100, update_permille: 500}   # a key range written more
  rate: 12MiB            # bytes a second; 0 unlimited
  workers: 128           # batches in flight; each waits for its commit
readers:
  - {name: uniform, rate: 200, workers: 16, scan_ratio: 0.05, scan_min: 1, scan_limit: 100, dist: uniform}
  - {name: newest, rate: 100, max_lag: 5s, dist: latest, latest_window: 1000000}   # or zipf, with zipf_s
storage:
  bucket_url: s3://isledb?endpoint={endpoint}&region=us-east-1&use_path_style=true
  endpoint: http://127.0.0.1:19000
  latency: {min: 15ms, max: 40ms}
  disk: {path: /data, max_used_pct: 85}   # hold writes while the disk is fuller
  list_every: 5m
isledb:
  readers: 1
  config:                # each role's section of the isledb configuration
    writer: {memtable: {target_bytes: 3MiB}, flush: {interval: 250ms}}
    maintenance: {compaction: {l0_to_base_ratio: 4, l0_max_depth: 64, base_level_bytes: 4GiB, target_sst_bytes: 256MiB}}
phases:
  - load:                # the insert round
      events: [{after: 2m, role: writer, kill: true, down: 5s}]
      faults: [{after: 1m, proxy: writer, set: {methods: [PUT], kinds: [manifest], drop_rate: 0.05}}]
  - update: {rounds: 10} # update rounds
  - settle: {quiet: 5m, max: 3h}
  - restart: {role: maintenance, isledb: main}
  - verify: {parts: 32}
sample_every: 30s
```

- `checkpoints` are where the summary records every measurement. Left out,
  they are the steps of one ladder up to what the run writes: 1, 5, 10, 20,
  50, 100, 250 and 500 GiB, 1, 2 and 5 TiB. Runs of any size share them, so
  `compare` lines a 20 GiB run up with the first steps of a 1 TiB one.
- `storage.disk` holds new writes while the disk at `path` is fuller than
  `max_used_pct`, and lets them go once it is 2 points below; maintenance
  keeps compacting and deleting meanwhile. Holds show in the samples and
  the log. Point it at the disk storage lives on.

- `update_permille`, `delete_permille`, `insert_permille` and `hot` shape
  the update rounds. Left out, every round rewrites every key, as before.
  `hot` ranges are shares of the keyspace from its start, so with `random`
  keys they are a real key range, as a tenant's prefix would be.
- A reader group asks each scan for `scan_limit` keys, or a number between
  `scan_min` and `scan_limit`. `dist: latest` reads the `latest_window`
  newest keys, inserts after round 0 included; `zipf` makes the oldest keys
  hot.

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
- Role names and fault settings are checked when the spec loads, and every
  version a restart names is built before the run starts, so a mistake
  fails at once, not hours in.

`specs/` has:

| Spec | What |
|---|---|
| `default` | 12 MiB/s of small writes against SILO, at the version's default settings |
| `bulk-load` | `default` at 24 MiB/s with fewer rewrites: L0 collects longer, a 4 GiB L1, 256 MiB files. Needs a version with `maintenance.compaction` |
| `local-smoke` | a directory bucket, about a minute: a quick check of the harness and a build |
| `silo-smoke` | `default` at 512 MiB, a few minutes |
| `silo-faults` | lost answers and refusals from storage while writing |
| `mixed` | a production-like mix: each round updates 5% of the keys, more in a hot tenth of the keyspace, deletes some, inserts 2% more; point reads, scans of 1 to 100 keys and reads of the newest keys |

`default` and `bulk-load` only insert: every key is written once, then
the run settles and verifies. The smoke and fault specs add one update round
that rewrites every key. `mixed` builds its dataset, settles and verifies
it, runs its mix of updates, deletes and inserts, then settles and verifies
again, so a result is never read off work compaction still owes.

## Storage and monitoring

`deploy/up.sh` starts, in Docker, on a Linux host:

- **SILO**, an S3-compatible server, with its data under `STRESS_DATA`, and
  creates the bucket;
- **Prometheus**, which scrapes every run in progress and SILO, and
  evaluates the alerts in `deploy/prometheus/alerts.yml`;
- **Grafana**, with the stress dashboard.

```
STRESS_DATA=/data/isledb-stress SILO_CPUS=6 deploy/up.sh
export AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin AWS_REGION=us-east-1
go run . run -spec specs/bulk-load.yaml -target 50GiB \
    -isledb origin/main -fetch -out /data/isledb-stress/results
```

`SILO_CPUS` caps SILO's CPUs, 3 by default: too few, and SILO, not isledb,
limits the run. `MONITORING=0` starts SILO only; Prometheus and Grafana
need Linux host networking, so on other systems SILO starts alone.
`deploy/down.sh` stops everything and keeps the data.

**Nothing is reachable from another host.** SILO, Prometheus (`9090`),
Grafana (`3300`), the runner's `/metrics` (`9400`), the latency proxies and
every isledb role listen on 127.0.0.1 only, and the runner refuses a
`-metrics-listen` address that isn't loopback. From another machine, open
an SSH tunnel and browse `http://127.0.0.1:3300`:

```
ssh -N -L 3300:127.0.0.1:3300 -L 9090:127.0.0.1:9090 <host>
```

**How Prometheus finds a run.** The isledb roles get new ports every run,
so while a run is going it writes its scrape targets, the runner and each
role, to `<-out>/.prometheus/<run>.json`, labelled with `run`, `role` and
`version`, and removes the file when it ends. Prometheus reads that
directory, so run with `-out` set to `STRESS_RESULTS`, by default
`$STRESS_DATA/results`. Prometheus keeps 30 days under `STRESS_DATA/prometheus`.

**What the runner exports**, at `127.0.0.1:9400/metrics`: `stress_*`
metrics for progress, write outcomes, commit and read latency, verification,
storage traffic by proxy, write and space amplification, the bucket and the
disk guard, plus `stress_phase` and `stress_run_info`. Prometheus also
scrapes every role's own metrics, with their labels and histograms whole,
and SILO's.

**The dashboard** (`isledb stress`, pick the run at the top) shows the run,
writes, reads, correctness, compaction, storage, SILO, and each process's
CPU and memory. Grafana is read-only; change the dashboard in
`deploy/grafana/dashboard.py` and run it to rewrite the JSON. L0 files and
depth, level sizes against their targets, and compaction times come from
maintenance's own metrics, so they show only for versions that export them;
for older ones those panels stay empty.

There is no Alertmanager: firing alerts show on the dashboard and at
Prometheus's `/alerts`.

Each run uses its own prefix in the bucket, so runs never see each other's
data. The proxies are built from `tools/latproxy` in the repository.
