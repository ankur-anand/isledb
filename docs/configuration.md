# Configuration reference

Every setting of every IsleDB service. Guidance on choosing values is on each
service's page: [writer](isledb-writer.md), [reader](isledb-reader.md),
[maintenance](isledb-maintenance.md) and [read cache](isledb-cache.md).

## How settings work

Each setting can come from a YAML file, an environment variable or a flag. A
flag overrides the environment, the environment overrides the file, and the
file overrides the default.

```
isledb writer --config /etc/isledb/isledb.yaml
```

- **One file for every service.** Settings shared by all services sit at the
  top; each service reads its own section (`writer:`, `reader:`,
  `maintenance:`, `cache:`) and leaves the others alone.
- **Unknown keys are errors,** at the top and inside a service's section, so a
  misspelled setting stops the service at start instead of being ignored.
- **Environment variables** are `ISLEDB_` followed by the key, upper case,
  with dots as underscores: `writer.flush.interval` is
  `ISLEDB_WRITER_FLUSH_INTERVAL`, and `bucket_url` is `ISLEDB_BUCKET_URL`. A
  variable set to an empty value counts as set.
- **Sizes** take binary units: `512KiB`, `256MiB`, `8GiB`, `2TiB`.
  **Durations** look like `250ms`, `30s` or `2m`.
- **`--check`** prints every setting with its final value, or the problems it
  found, and exits without starting the service.
- **No secrets in the configuration.** It names files that hold tokens and
  keys; storage credentials come from the provider's usual places.

## Shared settings

| Setting | Default | Description |
|---|---|---|
| `bucket_url`<br>`--bucket-url`<br>`ISLEDB_BUCKET_URL` | required | The bucket that holds the database. See [Set up object storage](object-store-guide.md). |
| `log_level`<br>`--log-level`<br>`ISLEDB_LOG_LEVEL` | `info` | `debug`, `info`, `warn` or `error`. |
| `--config`<br>`ISLEDB_CONFIG` |  | The YAML file. |

## Authentication, TLS and shutdown

The writer, reader and read cache share these settings, each under its own
section. Replace `<service>` with `writer`, `reader` or `cache`, and
`<SERVICE>` with `WRITER`, `READER` or `CACHE`.

| Setting | Default | Description |
|---|---|---|
| `<service>.auth.token_file`<br>`--auth-token-file`<br>`ISLEDB_<SERVICE>_AUTH_TOKEN_FILE` |  | File of accepted tokens, one per line. Reread on `SIGHUP`. |
| `<service>.auth.insecure`<br>`--insecure`<br>`ISLEDB_<SERVICE>_AUTH_INSECURE` | `false` | Accept requests without a token. For development only. |
| `<service>.tls.cert`, `<service>.tls.key`<br>`--tls-cert`, `--tls-key`<br>`ISLEDB_<SERVICE>_TLS_CERT`, `ISLEDB_<SERVICE>_TLS_KEY` |  | Serve HTTPS with this certificate and key. |
| `<service>.tls.client_ca`<br>`--tls-client-ca`<br>`ISLEDB_<SERVICE>_TLS_CLIENT_CA` |  | Require clients to present a certificate signed by this CA. |
| `<service>.shutdown_drain`<br>`--shutdown-drain`<br>`ISLEDB_<SERVICE>_SHUTDOWN_DRAIN` | `5s` | How long the service reports not ready before it shuts down, so load balancers stop sending it traffic. |

A service with an API refuses to start without a token file or a client CA,
unless `auth.insecure` is set. See [Secure IsleDB](security.md).

## Writer

| Setting | Default | Description |
|---|---|---|
| `writer.prefix`<br>`--prefix`<br>`ISLEDB_WRITER_PREFIX` | required | The database's path in the bucket; `/` for the root of the bucket. |
| `writer.owner_id`<br>`--owner-id`<br>`ISLEDB_WRITER_OWNER_ID` | host name | A name for this writer, shown in logs and storage. In Kubernetes, use the pod name. |
| `writer.flush.interval`<br>`--flush-interval`<br>`ISLEDB_WRITER_FLUSH_INTERVAL` | `250ms`; `1s` on `gs://` | How often writes are saved. At least `1s` on Google Cloud Storage. See [Choose the flush interval](isledb-writer.md#choose-the-flush-interval). |
| `writer.writes.commit_wait`<br>`--commit-wait`<br>`ISLEDB_WRITER_WRITES_COMMIT_WAIT` | `10s` | How long a write waits to be saved before the client gets `504`. Longer than `flush.interval`, and at most `20s`. |
| `writer.values.max_key_bytes`<br>`--max-key-bytes`<br>`ISLEDB_WRITER_VALUES_MAX_KEY_BYTES` | `65527` | Largest key accepted, in bytes. At most `65527`. |
| `writer.values.max_value_bytes`<br>`--max-value-bytes`<br>`ISLEDB_WRITER_VALUES_MAX_VALUE_BYTES` | `16MiB` | Largest value accepted. |
| `writer.writes.max_request_bytes`<br>`--max-request-bytes`<br>`ISLEDB_WRITER_WRITES_MAX_REQUEST_BYTES` | `32MiB` | Largest batch request. At least `1MiB`. |
| `writer.memtable.target_bytes`<br>`--memtable-bytes`<br>`ISLEDB_WRITER_MEMTABLE_TARGET_BYTES` | `16MiB` | How much data the writer buffers before saving it in a separate group. Also the largest batch. |
| `writer.memtable.max_pending`<br>`--max-pending-memtables`<br>`ISLEDB_WRITER_MEMTABLE_MAX_PENDING` | `4` | How many full groups can wait to be saved before new writes get `503`. |
| `writer.writes.max_inflight_bytes`<br>`--max-inflight-bytes`<br>`ISLEDB_WRITER_WRITES_MAX_INFLIGHT_BYTES` | `256MiB` | Memory for requests being read and applied at once. A `PUT` counts its body; a batch counts 4 times its body. Past it, new writes get `503` until earlier ones finish. At least 4 × `max_request_bytes`, and `max_value_bytes`. |
| `writer.writes.body_read_timeout`<br>`--body-read-timeout`<br>`ISLEDB_WRITER_WRITES_BODY_READ_TIMEOUT` | `1m` | How long a client has to send its request body. Past it, the client gets `408`. |
| `writer.change_feed.payload`<br>`--change-feed`<br>`ISLEDB_WRITER_CHANGE_FEED_PAYLOAD` | empty | Turns on the database's change feed: `keys` or `values`. Empty keeps what the database has. Once on, it stays on, and its mode can't change. |
| `writer.policy.max_pinned_view_age`<br>`--max-pinned-view-age`<br>`ISLEDB_WRITER_POLICY_MAX_PINNED_VIEW_AGE` | `1h` | Must match the database's setting. Leave it unset unless the database was created with a different value; otherwise the writer won't start. |
| `writer.listen`<br>`--listen`<br>`ISLEDB_WRITER_LISTEN` | `:8070` | Address for the API. |
| `writer.admin_listen`<br>`--admin-listen`<br>`ISLEDB_WRITER_ADMIN_LISTEN` | `127.0.0.1:8071` | Address for health checks and metrics. Empty turns it off. |

## Reader

| Setting | Default | Description |
|---|---|---|
| `reader.prefix`<br>`--prefix`<br>`ISLEDB_READER_PREFIX` | required | The database to read; `/` for the root of the bucket. |
| `reader.cache_dir`<br>`--cache-dir`<br>`ISLEDB_READER_CACHE_DIR` | required | This replica's disk cache. Give each replica its own. |
| `reader.caches.disk_bytes`<br>`--disk-cache-bytes`<br>`ISLEDB_READER_CACHES_DISK_BYTES` | `8GiB` | Disk cache budget. Must be positive. |
| `reader.caches.block_bytes`<br>`--block-cache-bytes`<br>`ISLEDB_READER_CACHES_BLOCK_BYTES` | `256MiB` | In-memory cache of data. Must be positive. |
| `reader.caches.bloom_bytes`<br>`--bloom-cache-bytes`<br>`ISLEDB_READER_CACHES_BLOOM_BYTES` | `64MiB` | In-memory cache of the filters that skip files without a key. Must be positive. |
| `reader.views.refresh_after`<br>`--refresh-after`<br>`ISLEDB_READER_VIEWS_REFRESH_AFTER` | `1s` | How often new data is loaded. At least `1s`. |
| `reader.reads.max_concurrent`<br>`--max-concurrent-reads`<br>`ISLEDB_READER_READS_MAX_CONCURRENT` | `256` | Reads served at once; past it, reads get `503`. |
| `reader.read_cache.url`<br>`--read-cache-url`<br>`ISLEDB_READER_READ_CACHE_URL` |  | An `isledb cache` to read through. |
| `reader.read_cache.token_file`<br>`--read-cache-token-file`<br>`ISLEDB_READER_READ_CACHE_TOKEN_FILE` |  | The token to send the cache: one token on a line of its own. Read at start; restart the reader after changing it. |
| `reader.read_cache.ca_file`<br>`--read-cache-ca-file`<br>`ISLEDB_READER_READ_CACHE_CA_FILE` |  | CA certificates for a cache with a private CA. Empty trusts the system's CAs. Needs an `https://` URL. |
| `reader.read_cache.cert`, `reader.read_cache.key`<br>`--read-cache-cert`, `--read-cache-key`<br>`ISLEDB_READER_READ_CACHE_CERT`, `ISLEDB_READER_READ_CACHE_KEY` |  | A client certificate for a cache that requires one. Set both. |
| `reader.listen`<br>`--listen`<br>`ISLEDB_READER_LISTEN` | `:8080` | Address for the API. |
| `reader.admin_listen`<br>`--admin-listen`<br>`ISLEDB_READER_ADMIN_LISTEN` | `127.0.0.1:8081` | Address for health checks and metrics. Empty turns it off. |

## Maintenance

| Setting | Default | Description |
|---|---|---|
| `maintenance.prefix`<br>`--prefix`<br>`ISLEDB_MAINTENANCE_PREFIX` | required | The database to maintain; `/` for the root of the bucket. |
| `maintenance.scratch_dir`<br>`--scratch-dir`<br>`ISLEDB_MAINTENANCE_SCRATCH_DIR` | the user's cache directory | Where compaction keeps the files it is merging. Use a local disk. |
| `maintenance.change_feed.retain_for`<br>`--change-feed-retain-for`<br>`ISLEDB_MAINTENANCE_CHANGE_FEED_RETAIN_FOR` | `0s` | How long change-feed history is kept; `0s` keeps it all. |
| `maintenance.admin_listen`<br>`--admin-listen`<br>`ISLEDB_MAINTENANCE_ADMIN_LISTEN` | `127.0.0.1:8061` | Address for health checks and metrics. Empty turns it off. |
| `maintenance.shutdown_drain`<br>`--shutdown-drain`<br>`ISLEDB_MAINTENANCE_SHUTDOWN_DRAIN` | `0s` | How long to report not ready before stopping. |

Maintenance serves no API, so `listen`, `auth` and `tls` don't apply to it:
setting them is an error.

## Read cache

| Setting | Default | Description |
|---|---|---|
| `cache.databases[].prefix`<br>`--db` (repeat)<br>`ISLEDB_CACHE_DATABASES` (comma-separated) | required | The databases served, by prefix; `/` for the root of the bucket. On the command line or in the environment, the list replaces the file's. |
| `cache.databases[].current_poll` | `cache.current.poll` | How often this database is checked for new writes. |
| `cache.disk.dir`<br>`--disk-dir`<br>`ISLEDB_CACHE_DISK_DIR` | required | Where cached data is kept, on local disk. One cache per directory. |
| `cache.disk.bytes`<br>`--disk-bytes`<br>`ISLEDB_CACHE_DISK_BYTES` | required | Disk budget. |
| `cache.reads.max_bytes`<br>`--max-read-bytes`<br>`ISLEDB_CACHE_READS_MAX_BYTES` | `256MiB` | Bytes read from storage at once. At least `4MiB`. |
| `cache.reads.max_wait`<br>`--max-read-wait`<br>`ISLEDB_CACHE_READS_MAX_WAIT` | `5s` | Longest a storage read waits for its turn. About half the readers' timeout to the cache. |
| `cache.current.poll`<br>`--current-poll`<br>`ISLEDB_CACHE_CURRENT_POLL` | `500ms` | How often each database is checked for new writes. |
| `cache.current.long_poll`<br>`--long-poll`<br>`ISLEDB_CACHE_CURRENT_LONG_POLL` | `30s` | Longest a reader's wait for new writes is held. At most `30s`. |
| `cache.listen`<br>`--listen`<br>`ISLEDB_CACHE_LISTEN` | `:8090` | Address for the API. |
| `cache.admin_listen`<br>`--admin-listen`<br>`ISLEDB_CACHE_ADMIN_LISTEN` | `127.0.0.1:8091` | Address for health checks and metrics. Empty turns it off. |
