# Running `isledb cache`

`isledb cache` is the regional read cache as a service. It relays `CURRENT`
for each database it serves, reading it from storage every 500 ms, and serves
immutable objects from 128 KiB chunks on local disk. The HTTP API is in
[read-cache-api.md](read-cache-api.md).

```
isledb cache --config /etc/isledb/isledb.yaml
```

or, without a file:

```
isledb cache \
  --bucket-url 's3://my-bucket?region=us-east-1' \
  --db prod/policy --db prod/flags \
  --disk-dir /var/cache/isledb --disk-bytes 200GiB \
  --auth-token-file /etc/isledb/tokens
```

Storage credentials come from the provider's usual environment (for S3, the
AWS SDK chain: environment variables, shared config, instance or pod role).
They are never set in the cache's own configuration.

## Configuration

Each setting can come from the YAML file, an environment variable or a flag.
A flag overrides the environment, the environment overrides the file, and the
file overrides the default. `--db` on the command line, or
`ISLEDB_CACHE_DATABASES`, replaces the file's list of databases rather than
adding to it.

One file can configure every isledb role: settings all roles share sit at the
top, and each role reads its own section. The cache reads `cache:`.

```yaml
bucket_url: s3://my-bucket?region=us-east-1
log_level: info

cache:
  listen: :8090
  admin_listen: 127.0.0.1:8091

  disk:
    dir: /var/cache/isledb
    bytes: 200GiB

  reads:
    max_bytes: 256MiB
    max_wait: 2m

  current:
    poll: 500ms
    long_poll: 30s

  databases:
    - prefix: prod/policy
    - prefix: prod/flags
      current_poll: 250ms

  auth:
    token_file: /etc/isledb/tokens

  tls:
    cert: /etc/isledb/tls/cert.pem
    key: /etc/isledb/tls/key.pem
    client_ca: /etc/isledb/tls/clients.pem

  shutdown_drain: 5s
```

Unknown keys are an error, at the top and inside `cache:`, so a misspelled
key stops the cache at start rather than being ignored. Sections of the
other roles this binary has are left for them; a section named after no role
is an error too. The file holds paths, never secrets: tokens and keys stay
in their own files.

A database at the root of the bucket is written `/`. An empty prefix is an
error, so a blank value never names the root, and so is one with an empty,
`.` or `..` segment (`prod//a`, `prod/../a`), whose keys would never match.

`isledb cache --config isledb.yaml --check` validates the configuration,
prints it as resolved from every source, and exits.

**Naming.** A setting's environment variable is `ISLEDB_` followed by its
key, upper case with dots as underscores: the shared settings as they are
(`bucket_url` → `ISLEDB_BUCKET_URL`), the cache's after `CACHE_`
(`cache.disk.bytes` → `ISLEDB_CACHE_DISK_BYTES`). An environment variable
set to an empty value counts: `ISLEDB_CACHE_ADMIN_LISTEN=` turns the admin
listener off.

| File key | Flag | Environment | Default | |
|---|---|---|---|---|
| `bucket_url` | `--bucket-url` | `ISLEDB_BUCKET_URL` | required | Bucket holding the databases |
| `log_level` | `--log-level` | `ISLEDB_LOG_LEVEL` | `info` | `debug`, `info`, `warn`, `error` |
| `cache.databases[].prefix` | `--db` (repeat) | `ISLEDB_CACHE_DATABASES` (comma-separated) | required | Databases served, by prefix; `/` for the bucket root |
| `cache.databases[].current_poll` | – | – | `cache.current.poll` | Per-database poll interval |
| `cache.listen` | `--listen` | `ISLEDB_CACHE_LISTEN` | `:8090` | API address |
| `cache.admin_listen` | `--admin-listen` | `ISLEDB_CACHE_ADMIN_LISTEN` | `127.0.0.1:8091` | Admin address; empty for none |
| `cache.disk.dir` | `--disk-dir` | `ISLEDB_CACHE_DISK_DIR` | required | Chunk directory, on local disk |
| `cache.disk.bytes` | `--disk-bytes` | `ISLEDB_CACHE_DISK_BYTES` | required | Disk budget |
| `cache.reads.max_bytes` | `--max-read-bytes` | `ISLEDB_CACHE_READS_MAX_BYTES` | `256MiB` | Bytes read from storage at once; at least `4MiB` |
| `cache.reads.max_wait` | `--max-read-wait` | `ISLEDB_CACHE_READS_MAX_WAIT` | `2m` | Longest a storage read waits for budget |
| `cache.current.poll` | `--current-poll` | `ISLEDB_CACHE_CURRENT_POLL` | `500ms` | How often `CURRENT` is read |
| `cache.current.long_poll` | `--long-poll` | `ISLEDB_CACHE_CURRENT_LONG_POLL` | `30s` | Longest a long poll is held, at most `30s` |
| `cache.auth.token_file` | `--auth-token-file` | `ISLEDB_CACHE_AUTH_TOKEN_FILE` | | Bearer tokens, one per line |
| `cache.auth.insecure` | `--insecure` | `ISLEDB_CACHE_AUTH_INSECURE` | `false` | No authentication, for development |
| `cache.tls.cert`, `cache.tls.key` | `--tls-cert`, `--tls-key` | `ISLEDB_CACHE_TLS_CERT`, `ISLEDB_CACHE_TLS_KEY` | | Serve TLS |
| `cache.tls.client_ca` | `--tls-client-ca` | `ISLEDB_CACHE_TLS_CLIENT_CA` | | Require client certificates signed by this CA |
| `cache.shutdown_drain` | `--shutdown-drain` | `ISLEDB_CACHE_SHUTDOWN_DRAIN` | `5s` | Not-ready time before shutting down |
| – | `--config` | `ISLEDB_CONFIG` | | YAML file |

Sizes are bytes or binary units: `512KiB`, `256MiB`, `200GiB`, `2TiB`.
Durations are Go durations: `250ms`, `30s`, `2m`.

## Authentication

The cache refuses to start without authentication unless `cache.auth.insecure` is set.

- **Tokens.** `cache.auth.token_file` holds one token per line; blank lines and
  lines starting with `#` are skipped. Clients send
  `Authorization: Bearer <token>`. Send `SIGHUP` to reread the file; if the
  new file cannot be read or holds no tokens, the tokens in use stay.
- **Client certificates.** With `cache.tls.client_ca`, clients must present a
  certificate it signed. With both tokens and client certificates set, both
  are required.

Each database is served only its own objects: those right under its prefix,
not those of a database nested under it (`prod/a/b` under `prod/a`), and,
for the root, not those of the databases under prefixes. A client can read
through the cache only the databases it is configured to serve.

## Admin endpoints

On `admin_listen`, without authentication; keep it on an address only
operators and scrapers reach.

| Path | |
|---|---|
| `/healthz` | 200 while the process runs |
| `/readyz` | 200 once every database's `CURRENT` has been read once; 503 before, and while shutting down. It stays 200 while storage is down: the cache keeps serving its copy, and readers judge its age. |
| `/metrics` | Prometheus |
| `/debug/pprof/` | Go profiles |

## Metrics

| Metric | |
|---|---|
| `isledb_cache_chunks_total{source}` | Chunks served: `disk`, `storage`, `shared`, `bridged` |
| `isledb_cache_object_responses_total{status}` | Object responses by `Isledb-Cache`: `hit`, `shared`, `miss` |
| `isledb_cache_storage_reads_total`, `isledb_cache_storage_read_bytes_total` | Storage range reads and bytes |
| `isledb_cache_storage_reads_queued_total` | Storage reads that waited for `cache.reads.max_bytes` |
| `isledb_cache_storage_bytes_reading` | Bytes being read now |
| `isledb_cache_size_lookups_total` | Sizes asked of storage, for requests without `Isledb-Object-Size` |
| `isledb_cache_current_position{db}`, `isledb_cache_current_age_seconds{db}` | The relayed `CURRENT` |
| `isledb_cache_current_poll_errors_total{db}` | Failed reads of `CURRENT` |
| `isledb_cache_long_polls_held` | Long polls waiting now |
| `isledb_cache_disk_bytes`, `isledb_cache_disk_max_bytes`, `isledb_cache_disk_entries`, `isledb_cache_disk_evictions_total`, `isledb_cache_disk_failures_total` | The chunk store |
| `isledb_cache_http_requests_total{route,code}`, `isledb_cache_http_request_seconds{route}` | Requests; `route` is `current`, `current_long_poll`, `object` or `other` |

What to watch:

- **`current_age_seconds` rising** means storage is not answering; readers
  stop accepting the copy once it is older than their age limit.
- **`storage_reads_queued_total` rising** means reads wait for budget: raise
  `cache.reads.max_bytes`, or add replicas, before waits reach `cache.reads.max_wait`
  and turn into 502s.
- **`disk_evictions_total` rising fast** with a low hit share means the disk
  is too small for the working set.

## Shutdown

On `SIGTERM` or `SIGINT` the cache:

1. reports not ready on `/readyz`;
2. waits `cache.shutdown_drain` for load balancers to notice;
3. answers every held long poll with the position it has;
4. finishes requests in flight, for up to 30 seconds, and exits.

A second `SIGTERM` or `SIGINT` during these steps ends the process at once.

Chunks on disk are kept. After a restart, requests carrying
`Isledb-Object-Size` are served from them at once.

## Deploying

- **Disk.** Put `cache.disk.dir` on local SSD. One process per directory: the
  chunk store locks it.
- **Timeouts in front of the cache.** Load balancers and proxies must allow
  idle connections longer than `cache.current.long_poll`, or they cut long polls
  off. Readers' own timeout to the cache should be about `cache.reads.max_wait`
  or less; set `cache.reads.max_wait` near the readers' timeout plus a margin.
- **Several replicas.** Round-robin works: each replica relays `CURRENT` on
  its own, and readers treat a copy behind their view as nothing new. Each
  replica then caches every hot chunk. Hashing object requests by path at
  the load balancer (Envoy ring hash, NGINX `hash $uri consistent`) puts
  each chunk on one replica, so their disks add up instead of repeating
  each other. `/v1/current` can stay round-robin.
- **Readiness.** Route traffic by `/readyz`, not `/healthz`.

## Kubernetes

The admin address defaults to `127.0.0.1:8091`, which the kubelet's probes
cannot reach from outside the container. Bind it to all interfaces in the
pod, and keep it off the Service.

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: isledb-cache
spec:
  serviceName: isledb-cache
  replicas: 2
  selector:
    matchLabels: { app: isledb-cache }
  template:
    metadata:
      labels: { app: isledb-cache }
    spec:
      # Longer than cache.shutdown_drain (5s) plus the 30s given to
      # requests in flight.
      terminationGracePeriodSeconds: 45
      containers:
        - name: cache
          image: isledb:latest
          args: ["cache", "--config", "/etc/isledb/isledb.yaml"]
          env:
            - name: ISLEDB_CACHE_ADMIN_LISTEN
              value: "0.0.0.0:8091"
          ports:
            - { name: api, containerPort: 8090 }
            - { name: admin, containerPort: 8091 }
          readinessProbe:
            httpGet: { path: /readyz, port: admin }
            periodSeconds: 2
          livenessProbe:
            httpGet: { path: /healthz, port: admin }
            periodSeconds: 10
          volumeMounts:
            - { name: config, mountPath: /etc/isledb, readOnly: true }
            - { name: chunks, mountPath: /var/cache/isledb }
      volumes:
        - name: config
          projected:
            sources:
              - configMap: { name: isledb-config }   # isledb.yaml
              - secret: { name: isledb-tokens }      # tokens
  volumeClaimTemplates:
    - metadata: { name: chunks }
      spec:
        accessModes: [ReadWriteOnce]
        storageClassName: local-ssd
        resources: { requests: { storage: 200Gi } }
---
apiVersion: v1
kind: Service
metadata:
  name: isledb-cache
spec:
  selector: { app: isledb-cache }
  ports:
    - { name: api, port: 8090, targetPort: api }   # the admin port is not here
---
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: isledb-cache-admin
spec:
  podSelector:
    matchLabels: { app: isledb-cache }
  policyTypes: [Ingress]
  ingress:
    - ports: [{ port: 8090 }]                 # the API, from anywhere it is routed
    - ports: [{ port: 8091 }]                 # the admin port, from monitoring only
      from:
        - namespaceSelector:
            matchLabels: { kubernetes.io/metadata.name: monitoring }
```

- **Why a StatefulSet.** Each replica keeps its own chunk volume across
  restarts, so a restarted pod serves hits from disk at once.
- **Probes.** Readiness uses `/readyz`, so a pod takes traffic only once it
  has read `CURRENT` for every database; liveness uses `/healthz`, which
  does not depend on storage, so a storage outage never restarts the pods.
  Kubelet probes come from the node, which most network plugins let
  through regardless of policy; if yours does not, allow the node's
  addresses on port 8091.
- **Admin port.** Not in the Service, and only the monitoring namespace may
  reach it: it serves `/debug/pprof`. Scrape `/metrics` through the pod's
  `admin` port.
- **Shutdown.** The cache drains on its own: on `SIGTERM` it reports not
  ready, waits `cache.shutdown_drain`, then answers held long polls. No
  `preStop` hook is needed.
- **Mixed versions.** A pod whose binary lacks a role named in a shared
  `isledb.yaml` refuses to start, so roll the image before adding a new
  role's section to the shared file.
