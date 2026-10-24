# Running `isledb writer`

`isledb writer` is a service that writes to one IsleDB database. Your
applications send it writes over HTTP; it saves them to object storage. The
HTTP API is described in [the writer HTTP API](writer-api.md).

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
default, 1 s on Google Cloud Storage). A write is answered once its group is
saved.

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
no writes arrive, the writer makes one small read a second, to check for work
from maintenance.

- **Lower latency:** reduce the interval, but not below the time a save takes.
  You can see that time in the `isledb_writer_commit_seconds` metric. Below
  it, writes get slower, not faster.
- **Lower storage cost:** increase the interval.
- **More throughput:** send more requests at once, or use batches. Writes that
  arrive together are saved together, so throughput doesn't depend on the
  interval.

**On Google Cloud Storage** (`gs://` buckets), the interval defaults to 1 s
and can't be set lower: Cloud Storage allows only about one update per second
to the object each save writes, so the writer refuses to start with a shorter
interval. Writes take about 1 s or more.

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

Every setting, with its flag, environment variable and default, is in the
[configuration reference](configuration.md#writer). Two need care:

- **`change_feed.payload`** turns on the database's change feed, `keys` or
  `values`, permanently. Leave it empty to keep what the database has. See
  [Follow changes](change-feed.md).
- **`policy.max_pinned_view_age`** must match the database's setting. Leave
  it unset unless the database was created by an application that set a
  different `MaxPinnedViewAge`; otherwise the writer won't start.

## Memory

Plan for about 910 MiB of memory with the default settings. Most of it is
data waiting to be saved, and requests being read and applied, which
`writes.max_inflight_bytes` bounds: a batch is counted at 4 times its body,
for the copies made while decoding and applying it. The writer also keeps
its recent saves in memory until it has combined them into one larger file,
about 64 MiB at a time, which takes up to about 190 MiB;
`l0_merge.target_bytes` sets that size. If you raise
`writes.max_inflight_bytes`, `memtable.target_bytes` or
`l0_merge.target_bytes`, raise the memory limit too.

## Authentication

The writer won't start without authentication unless you set
`writer.auth.insecure`. Clients send a token from `writer.auth.token_file`,
and with `writer.tls.client_ca` also a client certificate. Any client with a
valid token can write and delete every key in the database, so give writer
tokens only to applications that write. See [Secure IsleDB](security.md).

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

The writer's metrics, and what to alert on, are in the
[metrics reference](metrics.md#writer). Watch at least for saves failing,
the writer refusing writes because it is overloaded, and the writer being
replaced.

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
          image: ghcr.io/ankur-anand/isledb:0.20.0
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
