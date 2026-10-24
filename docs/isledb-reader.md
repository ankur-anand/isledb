# Running `isledb reader`

`isledb reader` is a service that serves reads of one IsleDB database over
HTTP. Run as many replicas as you need behind a load balancer: they share
nothing, so each one adds capacity. The HTTP API is described in
[the reader HTTP API](reader-api.md).

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

If the database has a change feed, every replica also serves it at
`/v1/changes`; see [Follow changes](change-feed.md). The
feed's newest position comes from the same loads as the data, and new
changes show at the same time as new data. Consumers that have caught up are
answered from memory: polling costs about one read per new position per
replica, however many consumers poll. A change feed turned on later shows at
the next load.

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

Every setting, with its flag, environment variable and default, is in the
[configuration reference](configuration.md#reader). Authentication and TLS
work as for every service: see [Secure IsleDB](security.md).

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

The reader's metrics, and what to alert on, are in the
[metrics reference](metrics.md#reader). Above all, alert when the data a
reader serves gets older than you accept: `time() -
isledb_reader_view_loaded_timestamp_seconds` grows whenever loading new data
fails. The reader keeps serving what it has, and stays ready, until that data
is `max_pinned_view_age` old; taking every replica out of service at once
would turn old reads into no reads.

## Shutting down

On `SIGTERM` or `SIGINT`, the reader reports itself not ready, waits
`shutdown_drain`, finishes reads in progress, and exits. It holds nothing
that needs saving: its disk cache is only a cache.
