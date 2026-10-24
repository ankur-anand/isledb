# Metrics reference

Every IsleDB service serves Prometheus metrics at `/metrics` on its admin
address, without authentication. This page lists them all, by service, with
what to alert on. Each service also exports the standard Go process metrics.

Counters end in `_total`. Durations are in seconds, and times are Unix
timestamps in seconds. Every service with an API reports its requests in the
same two metrics:

| Metric | Shows |
|---|---|
| `isledb_<service>_http_requests_total{route,code}` | Requests, by endpoint and status code. |
| `isledb_<service>_http_request_seconds{route}` | How long requests took to answer, by endpoint. |

## Writer

| Metric | Shows |
|---|---|
| `isledb_writer_status{status}` | `1` for the writer's current state: `open`, `stopped`, `closed` or `fenced` (another writer took over). |
| `isledb_writer_epoch` | This writer's takeover count: each writer that takes over the database has a higher one. |
| `isledb_writer_puts_total`, `isledb_writer_deletes_total` | Writes and deletes accepted. |
| `isledb_writer_backpressure_total` | Writes refused because too much data was waiting to be saved. |
| `isledb_writer_accepted_sequence` | The sequence number of the newest write accepted, saved or not. |
| `isledb_writer_committed_sequence` | The sequence number of the newest write saved to storage. |
| `isledb_writer_pending_bytes`, `isledb_writer_pending_memtables` | Data accepted but not yet saved. |
| `isledb_writer_oldest_uncommitted_timestamp_seconds` | When the oldest write still waiting to be saved arrived; `0` when nothing is waiting. |
| `isledb_writer_commits_total`, `isledb_writer_commit_errors_total` | Saves to storage, and saves that failed and are being retried. |
| `isledb_writer_commit_seconds` | How long a save takes. |
| `isledb_writer_commit_bytes_total` | Bytes saved to storage. |
| `isledb_writer_http_inflight_body_bytes` | Size of the requests being read and applied right now. |
| `isledb_writer_l0_merges_total`, `isledb_writer_l0_merge_errors_total` | The writer combines its recent saves into larger files: combined files uploaded, and attempts that failed and are retried. |
| `isledb_writer_l0_merge_bytes_total` | Bytes of combined files uploaded. |
| `isledb_writer_l0_merge_held_bytes` | Recent saves held in memory to combine. |
| `isledb_writer_l0_merge_skipped_bytes_total` | Saves not held for combining because the memory for it was full. |
| `isledb_writer_l0_merges_published_total`, `isledb_writer_l0_merges_refused_total` | Combined files that replaced the saves they came from, and those refused because maintenance had already changed those saves. |
| `isledb_writer_l0_merges_dropped_total` | Combining work given up because maintenance took the files first. |

**Alert on:**

- **Saves failing:** `commit_errors_total` rising while
  `time() - oldest_uncommitted_timestamp_seconds` grows. Usually a storage
  permission problem or an outage. The writer keeps retrying and loses
  nothing it accepted, but clients start getting `504`.
- **Overloaded:** `backpressure_total` rising, or many `503`s. Writes arrive
  faster than they can be saved: raise `memtable.max_pending`, or check how
  long saves take (`commit_seconds`).
- **Replaced:** `status{status="fenced"}` is `1`. Another writer took over;
  see [Run one writer per database](isledb-writer.md#run-one-writer-per-database).

## Reader

| Metric | Shows |
|---|---|
| `isledb_reader_view_position` | The revision reads are answered from. Compare it across replicas, or with the writer's commits, to see how far behind a replica is. |
| `isledb_reader_view_loaded_timestamp_seconds` | When the reader last loaded new data; `time()` minus it is how old the data being served is. |
| `isledb_reader_view_outdated_since_timestamp_seconds` | For a reader that publishes new data only when told to: since when it has held back newer data; `0` otherwise. |
| `isledb_reader_view_safety_publishes_total` | Times such a reader published newer data itself, after holding it back too long. |
| `isledb_reader_refresh_total`, `isledb_reader_refresh_errors_total` | Loads of new data, and loads that failed. |
| `isledb_reader_refresh_latency_seconds` | How long a load of new data takes. |
| `isledb_reader_stale_reads_total` | Reads answered while loading new data was failing. |
| `isledb_reader_get_total`, `isledb_reader_get_hits_total`, `isledb_reader_get_misses_total`, `isledb_reader_get_errors_total` | Key reads: all, found, not found, and failed. |
| `isledb_reader_get_latency_seconds` | How long a key read takes. |
| `isledb_reader_scan_total`, `isledb_reader_scan_errors_total`, `isledb_reader_scan_results_total`, `isledb_reader_scan_latency_seconds` | Scans: count, failures, keys returned, and duration. |
| `isledb_reader_scan_limit_total`, `isledb_reader_scan_limit_errors_total`, `isledb_reader_scan_limit_results_total`, `isledb_reader_scan_limit_latency_seconds` | Scans with a limit, such as one page of `GET /v1/kv`: the same four. |
| `isledb_reader_http_reads_in_progress` | Reads being served now. |
| `isledb_reader_sst_range_read_total`, `isledb_reader_sst_range_read_errors_total` | Reads of data files from storage, and those that failed. |
| `isledb_reader_sst_range_read_bytes_total`, `isledb_reader_sst_range_read_latency_seconds` | Bytes those reads fetched, and how long they took. |
| `isledb_reader_bloom_filter_skips_total` | Key reads that checked a file without its filter, which lets a read skip files that can't hold the key, because the filter was still loading. |
| `isledb_reader_bloom_filter_errors_total` | Filters that failed to load; reads go on without them. |
| `isledb_reader_dead_ssts_pending` | Replaced data files not yet removed from the disk cache. |
| `isledb_reader_read_cache_reads_total` | Reads the read cache answered. |
| `isledb_reader_read_cache_fallbacks_total{reason}` | Reads meant for the read cache that went to storage instead, and why. |
| `isledb_reader_read_cache_doorbells_total{result}` | Waits on the read cache for new writes, by result: `rang` (new data), `quiet` or `error`. |
| `isledb_reader_read_cache_current_total{outcome}` | Loads of new data asked of the read cache, by outcome: `relayed`, `too_old`, `behind` or `failed`. |
| `isledb_reader_read_cache_current_age_seconds` | How old the read cache's copy of the newest version was, when last asked. |

**Alert on:**

- **Falling behind:** the data served is older than you accept, for example
  `time() - isledb_reader_view_loaded_timestamp_seconds > 30`. It grows
  whenever loading new data fails; `refresh_errors_total` says why, usually a
  storage permission problem or an outage. The reader keeps serving what it
  has, and stays ready, until that data reaches the database's maximum view
  age (one hour by default).
- **Read cache not used:** `read_cache_fallbacks_total` rising with every
  read. The cache's URL, token or certificate is wrong; reads still work, from
  storage.
- **Overloaded:** many `503`s with `backpressure`. Add replicas, or raise
  `reads.max_concurrent` if memory allows.

## Maintenance

| Metric | Shows |
|---|---|
| `isledb_maintenance_running` | `1` while maintenance works on the database; `0` after another maintenance process took it over. |
| `isledb_maintenance_cycles_total`, `isledb_maintenance_cycle_errors_total` | Rounds of work, and rounds that failed and are retried. |
| `isledb_maintenance_last_cycle_timestamp_seconds` | When the last round completed. |
| `isledb_maintenance_waiting_for_writer` | `1` while finished work waits for the writer to publish it. On a busy database this is `1` nearly all the time, because each round hands the writer new work; don't alert on it. |
| `isledb_maintenance_waiting_for_writer_since_timestamp_seconds` | When the work the writer has yet to publish was handed to it; `0` when nothing waits. It moves forward each time the writer publishes. |
| `isledb_maintenance_compaction_jobs_total` | Compactions, counted when they run, including any the writer then refuses. |
| `isledb_maintenance_compaction_l0_merges_total`, `isledb_maintenance_compaction_moves_total` | Of those, merges of newly saved files with each other, and moves of files to another level without rewriting them. |
| `isledb_maintenance_compaction_read_bytes_total`, `isledb_maintenance_compaction_rewritten_bytes_total` | Bytes compaction read from and wrote to storage: most of maintenance's storage cost. |
| `isledb_maintenance_compaction_moved_bytes_total` | Bytes moved without being rewritten. |
| `isledb_maintenance_compaction_oversized_jobs_total` | Compactions larger than usual, run because the level they write into could not be made smaller first. Each writes one unusually large file. |
| `isledb_maintenance_commands_rejected_total` | Finished work the writer refused to publish. The writer logs why. |
| `isledb_maintenance_compaction_rejected_bytes_total` | Bytes compaction wrote for work the writer refused. The files are deleted later. |
| `isledb_maintenance_checkpoints_total` | Checkpoints of the database's metadata prepared for the writer to publish. |
| `isledb_maintenance_deleted_objects_total{kind}`, `isledb_maintenance_delete_failures_total{kind}` | Objects deleted from storage, and deletes that failed and are retried, by kind: `sst`, `change_batch`, `snapshot`, `page`. |

**Alert on:**

- **Stuck:** `time() - isledb_maintenance_last_cycle_timestamp_seconds` above
  a few minutes, or `cycle_errors_total` rising. Usually a storage permission
  problem, or no space in `scratch_dir`; the log says which.
- **No writer:** `time() - waiting_for_writer_since_timestamp_seconds` above a
  few minutes, while it isn't `0`. The work is ready, but no writer is
  publishing it, so files pile up.
- **Refused work:** `commands_rejected_total` rising steadily. An occasional
  refusal is normal: the writer refuses work on files that changed meanwhile,
  and maintenance redoes it. A steady rise means work is thrown away; the
  writer's log says why.
- **Replaced:** `running` at `0`. Another maintenance process took over; see
  [Run one maintenance per database](isledb-maintenance.md#run-one-maintenance-per-database).

## Read cache

| Metric | Shows |
|---|---|
| `isledb_cache_chunks_total{source}` | Pieces of data served, by where they came from: `disk`, `storage` (read for this request), `shared` (read for another request at the same time) or `bridged` (on disk, read again to fill a gap). |
| `isledb_cache_object_responses_total{status}` | Responses, by the `Isledb-Cache` header: `hit`, `shared` or `miss`. |
| `isledb_cache_storage_reads_total`, `isledb_cache_storage_read_bytes_total` | Reads sent to storage, and the bytes they returned. |
| `isledb_cache_storage_reads_queued_total` | Storage reads that waited for their turn (`reads.max_bytes`). |
| `isledb_cache_storage_bytes_reading` | Bytes being read from storage now. |
| `isledb_cache_size_lookups_total` | Object sizes asked of storage, for requests that didn't give one. |
| `isledb_cache_current_position{db}` | The newest revision the cache has seen, by database. |
| `isledb_cache_current_age_seconds{db}` | How long since the cache last checked a database for new writes. |
| `isledb_cache_current_poll_errors_total{db}` | Checks for new writes that failed. |
| `isledb_cache_long_polls_held` | Readers waiting for new writes now. |
| `isledb_cache_disk_bytes`, `isledb_cache_disk_max_bytes`, `isledb_cache_disk_entries` | Data on disk, the disk budget, and the number of pieces held. |
| `isledb_cache_disk_evictions_total`, `isledb_cache_disk_failures_total` | Pieces removed to make room, and pieces that couldn't be written to disk. |

`route` on the cache's request metrics is `current`, `current_long_poll`,
`object` or `other`.

**Alert on:**

- **Storage not answering:** `current_age_seconds` rising. Readers stop
  trusting the cache's copy once it is older than their limit, and read
  storage themselves.
- **Reads waiting:** `storage_reads_queued_total` rising. Raise
  `reads.max_bytes`, or add replicas, before waits reach `reads.max_wait` and
  turn into `502`s.
- **Disk too small:** `disk_evictions_total` rising fast while few responses
  are `hit`s. The working set doesn't fit on disk.
