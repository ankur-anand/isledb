# Running `isledb writer`

`isledb writer` is a service that writes to one IsleDB database. Your
applications send it writes over HTTP; it saves them to object storage. The
HTTP API is described in [writer-api.md](writer-api.md).

## Start the writer

With a configuration file:

```
isledb writer --config /etc/isledb/isledb.yaml
```

Or with flags:

```
isledb writer \
  --bucket-url 's3://my-bucket?region=us-east-1' \
  --prefix prod/accounts \
  --auth-token-file /etc/isledb/tokens
```

The writer reads storage credentials from the usual places for your cloud
provider. For S3, that's the standard AWS environment variables, shared
config files, or the instance or pod role. Credentials never go in the
writer's configuration.

## Run one writer per database

Only one writer can write to a database at a time. If you start a second
writer for the same database, it takes over, and the first one stops taking
writes and answers `503`.

- Run exactly one replica.
- When you deploy a new version, stop the old writer before starting the new
  one. In Kubernetes, use `strategy: Recreate`.

If a writer's `isledb_writer_status{status="fenced"}` metric is `1`, another
writer has taken over the database. That writer stays running but takes no
traffic, and it won't take the database back on its own. Find out where the
other writer came from (an overlapping deployment, a second Deployment, a
manual run), stop whichever one shouldn't be running, then delete the pod you
don't want.

## Choose the flush interval

The writer saves writes in groups, once every `flush.interval` (250 ms by
default). A write is answered once its group is saved.

This one setting decides two things:

- **How long each write waits.** About one flush interval, plus the time to
  save the group.
- **How many requests the writer makes to your storage.** Each group costs two
  storage requests, however many writes it holds.

| `flush.interval` | Typical write latency | Storage requests per second while writes keep arriving |
|---|---|---|
| 50ms | about 100 ms | about 20 |
| 250ms (default) | about 250 ms | about 8 |
| 1s | about 1 s | about 2 |

These numbers were measured against S3-compatible storage 15–40 ms away. When
no writes arrive, the writer makes no storage requests.

- **Lower latency:** reduce the interval, but not below the time a save takes.
  You can see that time in the `isledb_writer_commit_seconds` metric. Below
  it, writes get slower, not faster.
- **Lower storage cost:** increase the interval.
- **More throughput:** send more requests at once, or use batches. Writes that
  arrive together are saved together, so throughput doesn't depend on the
  interval.

## Configuration

You can set each option in a YAML file, as an environment variable, or as a
flag. Flags take priority over environment variables, which take priority
over the file.

```yaml
bucket_url: s3://my-bucket?region=us-east-1
log_level: info

writer:
  listen: :8070
  admin_listen: 127.0.0.1:8071

  prefix: prod/accounts

  flush:
    interval: 250ms

  writes:
    commit_wait: 10s

  auth:
    token_file: /etc/isledb/tokens
```

To check a configuration without starting the writer:

```
isledb writer --config isledb.yaml --check
```

It prints every setting with its final value, or the problems it found.

### Options

| File key | Flag | Environment variable | Default | Description |
|---|---|---|---|---|
| `bucket_url` | `--bucket-url` | `ISLEDB_BUCKET_URL` | required | The bucket that holds the database. |
| `writer.prefix` | `--prefix` | `ISLEDB_WRITER_PREFIX` | required | The database's path in the bucket. Use `/` for a database at the root of the bucket. |
| `log_level` | `--log-level` | `ISLEDB_LOG_LEVEL` | `info` | `debug`, `info`, `warn` or `error`. |
| `writer.owner_id` | `--owner-id` | `ISLEDB_WRITER_OWNER_ID` | host name | A name for this writer, shown in logs and storage. In Kubernetes, use the pod name. |
| `writer.flush.interval` | `--flush-interval` | `ISLEDB_WRITER_FLUSH_INTERVAL` | `250ms` | How often writes are saved. See [Choose the flush interval](#choose-the-flush-interval). |
| `writer.writes.commit_wait` | `--commit-wait` | `ISLEDB_WRITER_WRITES_COMMIT_WAIT` | `10s` | How long a write waits to be saved before the client gets `504`. Must be longer than `flush.interval`, and at most `20s`. |
| `writer.values.max_key_bytes` | `--max-key-bytes` | `ISLEDB_WRITER_VALUES_MAX_KEY_BYTES` | `65527` | Largest key accepted, in bytes. At most `65527`. |
| `writer.values.max_value_bytes` | `--max-value-bytes` | `ISLEDB_WRITER_VALUES_MAX_VALUE_BYTES` | `16MiB` | Largest value accepted. |
| `writer.writes.max_request_bytes` | `--max-request-bytes` | `ISLEDB_WRITER_WRITES_MAX_REQUEST_BYTES` | `32MiB` | Largest batch request. At least `1MiB`. |
| `writer.memtable.target_bytes` | `--memtable-bytes` | `ISLEDB_WRITER_MEMTABLE_TARGET_BYTES` | `16MiB` | How much data the writer buffers before saving it in a separate group. Also the largest batch. |
| `writer.memtable.max_pending` | `--max-pending-memtables` | `ISLEDB_WRITER_MEMTABLE_MAX_PENDING` | `4` | How many full groups can wait to be saved before new writes get `503`. |
| `writer.writes.max_inflight_bytes` | `--max-inflight-bytes` | `ISLEDB_WRITER_WRITES_MAX_INFLIGHT_BYTES` | `256MiB` | Memory for requests being read and applied at once. A `PUT` counts its body; a batch counts 4 times its body, for its decoded copy and the batch built from it. Past it, new writes get `503` until earlier ones finish. At least 4 × `max_request_bytes`, and `max_value_bytes`. |
| `writer.writes.body_read_timeout` | `--body-read-timeout` | `ISLEDB_WRITER_WRITES_BODY_READ_TIMEOUT` | `1m` | How long a client has to send its request body. Past it, the client gets `408`. |
| `writer.change_feed.payload` | `--change-feed` | `ISLEDB_WRITER_CHANGE_FEED_PAYLOAD` | empty | Turns on the database's change feed: `keys` or `values`. See below. |
| `writer.policy.max_pinned_view_age` | `--max-pinned-view-age` | `ISLEDB_WRITER_POLICY_MAX_PINNED_VIEW_AGE` | `1h` | Must match the database's setting. See below. |
| `writer.listen` | `--listen` | `ISLEDB_WRITER_LISTEN` | `:8070` | Address for the API. |
| `writer.admin_listen` | `--admin-listen` | `ISLEDB_WRITER_ADMIN_LISTEN` | `127.0.0.1:8071` | Address for health checks and metrics. Empty turns it off. |
| `writer.auth.token_file` | `--auth-token-file` | `ISLEDB_WRITER_AUTH_TOKEN_FILE` | | File of accepted tokens, one per line. |
| `writer.auth.insecure` | `--insecure` | `ISLEDB_WRITER_AUTH_INSECURE` | `false` | Accept requests without a token. For development only. |
| `writer.tls.cert`, `writer.tls.key` | `--tls-cert`, `--tls-key` | `ISLEDB_WRITER_TLS_CERT`, `ISLEDB_WRITER_TLS_KEY` | | Serve HTTPS with this certificate. |
| `writer.tls.client_ca` | `--tls-client-ca` | `ISLEDB_WRITER_TLS_CLIENT_CA` | | Require clients to present a certificate signed by this CA. |
| `writer.shutdown_drain` | `--shutdown-drain` | `ISLEDB_WRITER_SHUTDOWN_DRAIN` | `5s` | How long the writer reports itself not ready before it shuts down, so load balancers stop sending traffic. |

Sizes accept `KiB`, `MiB` and `GiB`, as in `512KiB` or `16MiB`. Durations
look like `250ms`, `30s` or `2m`.

**Change feed.** Leave `change_feed.payload` empty unless you want to turn
on the change feed. With it empty, the writer keeps whatever the database
already has. Setting `keys` or `values` turns the feed on permanently. If the
database already has a feed with a different setting, the writer won't start.

**Pinned view age.** If your database was created by an application that set
`MaxPinnedViewAge`, set `policy.max_pinned_view_age` to the same value.
Otherwise the writer won't start. Leave it empty for databases created with
the default.

## Memory

Plan for about 720 MiB of memory with the default settings. Most of it is
data waiting to be saved, and requests being read and applied, which
`writes.max_inflight_bytes` bounds: a batch is counted at 4 times its body,
for the copies made while decoding and applying it. If you raise
`writes.max_inflight_bytes` or `memtable.target_bytes`, raise the memory
limit too.

## Authentication

The writer won't start without authentication unless you set
`writer.auth.insecure`.

- **Tokens:** put accepted tokens in the file named by
  `writer.auth.token_file`, one per line. Lines starting with `#` are ignored.
  Clients send `Authorization: Bearer <token>`. To reload the file without
  restarting, send the writer `SIGHUP`.
- **Client certificates:** with `writer.tls.client_ca`, clients must also
  present a certificate signed by that CA.

Any client with a valid token can write and delete every key in the
database.

## Health checks

These are served on `admin_listen`, without authentication. Keep that address
reachable only by your operators and monitoring.

| Path | Answers |
|---|---|
| `/healthz` | `200` while the process is running. |
| `/readyz` | `200` while the writer is taking writes. `503` while it is shutting down, or after another writer has taken over. Send traffic only to a ready writer. |
| `/metrics` | Prometheus metrics. |
| `/debug/pprof/` | Go profiling. |

## Metrics

| Metric | Shows |
|---|---|
| `isledb_writer_status{status}` | `1` for the writer's current state: `open`, `stopped`, `closed` or `fenced` (another writer took over). |
| `isledb_writer_commits_total`, `isledb_writer_commit_errors_total` | Saves to storage, and saves that failed and are being retried. |
| `isledb_writer_commit_seconds` | How long a save takes. |
| `isledb_writer_oldest_uncommitted_timestamp_seconds` | When the oldest write still waiting to be saved arrived; `0` when nothing is waiting. |
| `isledb_writer_pending_bytes`, `isledb_writer_pending_memtables` | Data waiting to be saved. |
| `isledb_writer_puts_total`, `isledb_writer_deletes_total` | Writes and deletes accepted. |
| `isledb_writer_backpressure_total` | Writes refused because too much data was waiting to be saved. |
| `isledb_writer_http_inflight_body_bytes` | Size of the requests being read right now. |
| `isledb_writer_http_requests_total{route,code}`, `isledb_writer_http_request_seconds{route}` | Requests by endpoint and status code. |

What to alert on:

- **Saves failing:** `commit_errors_total` rising, with the age of the oldest
  waiting write growing. Usually a storage permission problem or an outage.
  The writer keeps retrying and loses nothing it accepted, but clients start
  getting `504`.
- **Writer overloaded:** `backpressure_total` rising, or many `503`s. Writes
  arrive faster than they can be saved. Increase `memtable.max_pending`, or
  check how long saves take.
- **Writer replaced:** `status{status="fenced"}` is `1`. See
  [Run one writer per database](#run-one-writer-per-database).

## Shutting down

When the writer receives `SIGTERM` or `SIGINT`, it:

1. reports itself not ready, so load balancers stop sending traffic, and waits
   `shutdown_drain`;
2. stops taking new writes and saves the ones it has, so waiting clients get
   their answer;
3. finishes requests still in progress;
4. exits.

This normally takes a second or two after `shutdown_drain`. If storage is
slow or down, it can take up to about 70 seconds more. Give the process that
long before killing it. A second signal stops it immediately.

## Deploying

- Run one replica, and don't let an old and a new writer overlap during a
  deployment.
- Send traffic only to a writer whose `/readyz` answers `200`.
- Allow requests to take at least `writes.commit_wait` plus a few seconds in
  load balancers and clients.

### Kubernetes

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: isledb-writer
spec:
  replicas: 1
  strategy: { type: Recreate }   # never two writers at once
  selector:
    matchLabels: { app: isledb-writer }
  template:
    metadata:
      labels: { app: isledb-writer }
    spec:
      terminationGracePeriodSeconds: 90
      containers:
        - name: writer
          image: isledb:latest
          args: ["writer", "--config", "/etc/isledb/isledb.yaml"]
          env:
            - name: ISLEDB_WRITER_ADMIN_LISTEN
              value: "0.0.0.0:8071"
            - name: ISLEDB_WRITER_OWNER_ID
              valueFrom: { fieldRef: { fieldPath: metadata.name } }
          ports:
            - { name: api, containerPort: 8070 }
            - { name: admin, containerPort: 8071 }
          readinessProbe:
            httpGet: { path: /readyz, port: admin }
            periodSeconds: 2
          livenessProbe:
            httpGet: { path: /healthz, port: admin }
            periodSeconds: 10
          resources:
            limits: { memory: 1Gi }
          volumeMounts:
            - { name: config, mountPath: /etc/isledb, readOnly: true }
      volumes:
        - name: config
          projected:
            sources:
              - configMap: { name: isledb-config }   # isledb.yaml
              - secret: { name: isledb-tokens }      # tokens
---
apiVersion: v1
kind: Service
metadata:
  name: isledb-writer
spec:
  selector: { app: isledb-writer }
  ports:
    - { name: api, port: 8070, targetPort: api }   # the admin port is not exposed
```

The writer doesn't need a disk: it saves everything to object storage before
it exits.
