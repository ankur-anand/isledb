# Running `isledb reader`

`isledb reader` is a service that serves reads of one IsleDB database over
HTTP. Run as many replicas as you need behind a load balancer: they share
nothing, so each one adds capacity. The HTTP API is described in
[reader-api.md](reader-api.md).

## Start a reader

With a configuration file:

```
isledb reader --config /etc/isledb/isledb.yaml
```

Or with flags:

```
isledb reader \
  --bucket-url 's3://my-bucket?region=us-east-1' \
  --prefix prod/accounts \
  --cache-dir /var/cache/isledb \
  --auth-token-file /etc/isledb/tokens
```

The reader reads storage credentials from the usual places for your cloud
provider, like the writer. Credentials never go in its configuration.

Readers and the writer can start in any order. Until the database is first
written, a reader answers every key with `404` at revision `0`, and it picks
up the data at its next refresh after the first write.

## Freshness and cost

A reader loads the database's newest data every `views.refresh_after`, 1
second by default, so reads are about that far behind the writer. Each load
reads at least one small object from storage: about 86,400 reads a day per
replica, more when the database changes.

With an `isledb cache` in the same region (`read_cache.url`), replicas read
through it instead: they usually load new data within a second of each
write, and storage sees reads from the cache rather than from every replica.

## Configuration

You can set each option in a YAML file, as an environment variable, or as a
flag. Flags take priority over environment variables, which take priority
over the file.

A typical configuration:

```yaml
bucket_url: s3://my-bucket?region=us-east-1
log_level: info

reader:
  listen: :8080
  admin_listen: 127.0.0.1:8081

  prefix: prod/accounts
  cache_dir: /var/cache/isledb

  read_cache:
    url: https://isledb-cache:8090
    token_file: /etc/isledb/cache-token
    ca_file: /etc/isledb/tls/cache-ca.pem   # only for a cache with a private CA

  auth:
    token_file: /etc/isledb/tokens
```

Every option of the `reader` section, at its default where it has one:

```yaml
reader:
  listen: :8080
  admin_listen: 127.0.0.1:8081
  shutdown_drain: 5s

  prefix: prod/accounts          # required
  cache_dir: /var/cache/isledb   # required, one per replica

  caches:
    disk_bytes: 8GiB
    block_bytes: 256MiB
    bloom_bytes: 64MiB
  views:
    refresh_after: 1s
  reads:
    max_concurrent: 256

  read_cache:
    url: https://isledb-cache:8090
    token_file: /etc/isledb/cache-token
    ca_file: /etc/isledb/tls/cache-ca.pem    # only for a cache with a private CA
    cert: /etc/isledb/tls/reader.pem         # only for a cache that requires
    key: /etc/isledb/tls/reader-key.pem      # client certificates

  auth:
    token_file: /etc/isledb/tokens
    # insecure: true                         # development only
  tls:
    # cert: /etc/isledb/tls/server.pem       # serve HTTPS
    # key: /etc/isledb/tls/server-key.pem
    # client_ca: /etc/isledb/tls/clients.pem # require client certificates
```

To check a configuration without starting the reader:

```
isledb reader --config isledb.yaml --check
```

It checks the settings and prints the configuration they resolve to. It
doesn't open the token, CA or certificate files: a missing one stops the
reader when it starts.

### Options

| Setting | Flag | Environment | Default | Meaning |
|---|---|---|---|---|
| `reader.prefix` | `--prefix` | `ISLEDB_READER_PREFIX` | | The database to read; `/` for the bucket root. Required. |
| `reader.cache_dir` | `--cache-dir` | `ISLEDB_READER_CACHE_DIR` | | This replica's disk cache. Required; give each replica its own. |
| `reader.caches.disk_bytes` | `--disk-cache-bytes` | `ISLEDB_READER_CACHES_DISK_BYTES` | `8GiB` | Disk cache budget. Must be positive. |
| `reader.caches.block_bytes` | `--block-cache-bytes` | `ISLEDB_READER_CACHES_BLOCK_BYTES` | `256MiB` | In-memory cache of data blocks. Must be positive. |
| `reader.caches.bloom_bytes` | `--bloom-cache-bytes` | `ISLEDB_READER_CACHES_BLOOM_BYTES` | `64MiB` | In-memory cache of Bloom filters. Must be positive. |
| `reader.views.refresh_after` | `--refresh-after` | `ISLEDB_READER_VIEWS_REFRESH_AFTER` | `1s` | How often new data is loaded. At least `1s`. |
| `reader.read_cache.url` | `--read-cache-url` | `ISLEDB_READER_READ_CACHE_URL` | | An `isledb cache` to read through. |
| `reader.read_cache.token_file` | `--read-cache-token-file` | `ISLEDB_READER_READ_CACHE_TOKEN_FILE` | | The token to send the cache: one token, on a line of its own; blank lines and lines starting with `#` are skipped. Read when the reader starts: after changing it, restart the reader. |
| `reader.read_cache.ca_file` | `--read-cache-ca-file` | `ISLEDB_READER_READ_CACHE_CA_FILE` | | CA certificates that sign the cache's certificate, for a cache with a private CA. Empty trusts the system's CAs. Needs an `https://` URL. |
| `reader.read_cache.cert`, `reader.read_cache.key` | `--read-cache-cert`, `--read-cache-key` | `ISLEDB_READER_READ_CACHE_CERT`, `ISLEDB_READER_READ_CACHE_KEY` | | A client certificate for a cache that requires one (its `tls.client_ca`). Set both. |
| `reader.reads.max_concurrent` | `--max-concurrent-reads` | `ISLEDB_READER_READS_MAX_CONCURRENT` | `256` | Reads served at once; past it, reads get `503`. |
| `reader.listen` | `--listen` | `ISLEDB_READER_LISTEN` | `:8080` | API address. |
| `reader.admin_listen` | `--admin-listen` | `ISLEDB_READER_ADMIN_LISTEN` | `127.0.0.1:8081` | Health checks and metrics. |

Authentication, TLS and `shutdown_drain` work as for the writer: see
[isledb-writer.md](isledb-writer.md#authentication).

The caches can't be turned off: each size must be positive.

**After setting `read_cache.url`, check that it works.** A reader that can't
use the cache, because of a wrong URL, a wrong token, or a cache using TLS
with a certificate the reader doesn't trust, still answers every read, from
storage. Watch `isledb_reader_read_cache_fallbacks_total`: if it rises with
every read, the cache isn't being used. A missing or unreadable `ca_file`,
`cert` or `key` is different: the reader refuses to start.

## Memory

Plan for the block and Bloom caches (about 320 MiB by default), plus the
values being served: up to `reads.max_concurrent` values, or scan pages of
about 1 MiB plus one value, at once. With large values, lower
`reads.max_concurrent` or raise the memory limit.

## Health checks

Served on `admin_listen`, without authentication.

| Path | Answers |
|---|---|
| `/healthz` | `200` while the process is running. |
| `/readyz` | `200` while the reader has data to serve. `503` while starting, while shutting down, or after it has lost access to storage for longer than the database's `max_pinned_view_age` (1 hour by default), when its data expires. Until then a reader that can't refresh stays ready and serves older data: see the falling-behind alert below. |
| `/metrics` | Prometheus metrics. |
| `/debug/pprof/` | Go profiling. |

## Metrics

| Metric | Shows |
|---|---|
| `isledb_reader_view_position` | The revision reads are answered from. Compare it across replicas, or with the writer's commits, to see how far behind a replica is. |
| `isledb_reader_view_loaded_timestamp_seconds` | When the reader last loaded the database's data; `time()` minus it is how old the data being served is. |
| `isledb_reader_stale_reads_total` | Reads answered while refreshes were failing. |
| `isledb_reader_refresh_total`, `isledb_reader_refresh_errors_total` | Loads of new data, and loads that failed. |
| `isledb_reader_get_total`, `isledb_reader_get_hits_total`, `isledb_reader_get_misses_total` | Reads, by found or not. |
| `isledb_reader_http_reads_in_progress` | Reads being served now. |
| `isledb_reader_http_requests_total{route,code}`, `isledb_reader_http_request_seconds{route}` | Requests by endpoint and status code. |
| `isledb_reader_read_cache_*` | Reads through the cache, and those that fell back to storage. |

What to alert on:

- **Falling behind:** the reader's data is older than you accept. Alert on
  its age, for example `time() - isledb_reader_view_loaded_timestamp_seconds
  > 30`, which grows whenever refreshes stop succeeding. Look at
  `isledb_reader_refresh_errors_total` for why: usually a storage permission
  problem or an outage. `isledb_reader_stale_reads_total` counts reads
  answered meanwhile. The reader keeps serving what it has, and stays ready,
  until that data is `max_pinned_view_age` old; taking every replica out of
  service at once would turn old reads into no reads.
- **Overloaded:** many `503 backpressure`. Add replicas, or raise
  `reads.max_concurrent` if memory allows.

## Shutting down

On `SIGTERM` or `SIGINT`, the reader reports itself not ready, waits
`shutdown_drain`, finishes reads in progress, and exits. It holds nothing
that needs saving: its disk cache is only a cache.
