# Running `isledb cache`

`isledb cache` is the read cache, run as a service. It serves one or more
databases to the readers in a region. It checks each database for new writes
every 500 ms and tells waiting readers at once, and it keeps the data readers
ask for on its local disk, so the bucket is read once for all of them instead
of once for each. Readers use it through [the cache HTTP API](read-cache-api.md).

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

One file can configure every IsleDB service: settings they share sit at the
top, and each service reads its own section. The cache reads `cache:`.

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
    max_wait: 5s

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

A database at the root of the bucket is written `/`. An empty prefix is an
error, so a blank value never names the root, and so is one with an empty,
`.` or `..` segment (`prod//a`, `prod/../a`), whose keys would never match.

`isledb cache --config isledb.yaml --check` validates the configuration,
prints it as resolved from every source, and exits.

Every setting, with its flag, environment variable and default, is in the
[configuration reference](configuration.md#read-cache).

## Authentication

The cache refuses to start without authentication unless `cache.auth.insecure`
is set. Readers send a token from the cache's `auth.token_file`, and with
`tls.client_ca` also a client certificate; an `isledb reader` sets these with
its `read_cache` settings. See [Secure IsleDB](security.md).

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
| `/readyz` | 200 once it has checked every database for new writes once; 503 before, and while shutting down. It stays 200 while storage is down: the cache keeps serving what it has, and readers judge its age. |
| `/metrics` | Prometheus |
| `/debug/pprof/` | Go profiles |

## Metrics

The cache's metrics, and what to watch, are in the
[metrics reference](metrics.md#read-cache): above all, storage not answering,
reads waiting for their turn, and a disk too small for what readers read.

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
  off. An `isledb reader` waits up to 10 seconds for the cache, then reads
  storage itself. `cache.reads.max_wait`, 5 seconds by default, is about half
  that, so a read that waited for its turn still has time to fetch and
  answer. Raise it only for clients that wait longer, to about half their
  timeout.
- **Several replicas.** Round-robin works: each replica checks for new writes
  on its own, and readers treat a copy behind their view as nothing new. Each
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
      securityContext:
        fsGroup: 65532   # the image's user: lets it write the chunk volume
      containers:
        - name: cache
          image: ghcr.io/ankur-anand/isledb:0.20.0
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
  has checked every database for new writes; liveness uses `/healthz`, which
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
