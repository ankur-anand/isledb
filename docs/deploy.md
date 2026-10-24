# Deploy IsleDB

This page covers what to run for a database, how many of each, and what each
needs. The pages for each service cover its settings, health checks and
metrics in detail. To try IsleDB on one machine first, see
[Get started](https://isledb.com/getting-started/).

## What you run

| Service | How many | Keeps state? | API | Admin |
|---|---|---|---|---|
| [`isledb writer`](isledb-writer.md) | Exactly one per database | No: everything is saved to the bucket before it answers | `:8070` | `127.0.0.1:8071` |
| [`isledb reader`](isledb-reader.md) | As many as you need | A disk cache it can rebuild | `:8080` | `127.0.0.1:8081` |
| [`isledb maintenance`](isledb-maintenance.md) | Exactly one per database | Scratch files it can rebuild | none | `127.0.0.1:8061` |
| [`isledb cache`](isledb-cache.md) | Optional; one or more per region | A disk cache it can rebuild | `:8090` | `127.0.0.1:8091` |

All four are the same program, `isledb`, started with the service's name.
They share nothing but the bucket, so they can run anywhere that reaches it,
in any order.

## Get the program

- **Download** `isledb` for Linux or macOS from the
  [latest release](https://github.com/ankur-anand/isledb/releases/latest).
- **Container image:** `ghcr.io/ankur-anand/isledb`, tagged with each
  release's exact version, such as `0.20.0`, and its minor version, such as
  `0.20`. There is no `latest` tag: name the version you deploy.
- **Build it** with Go:
  `go install github.com/ankur-anand/isledb/cmd/isledb@latest`.

The image runs as user `65532`. Give that user write access to the volumes
for a reader's cache, maintenance's scratch space and the read cache's disk;
in Kubernetes, set `securityContext.fsGroup: 65532`.

## Before you start

1. **A bucket.** Create it as described in
   [Set up object storage](object-store-guide.md): versioning and soft delete
   off, no retention locks, and no lifecycle rules over the database's
   prefix.
2. **Storage credentials.** Each service reads them from the provider's usual
   places: environment variables, shared config files, or the instance or pod
   role. The writer and maintenance need to read, write and delete objects
   under the database's prefix; readers and the read cache only read.
3. **Tokens.** The writer, readers and read cache refuse to start without
   authentication. See [Secure IsleDB](security.md).

## Choose a layout

**A small deployment.** One writer, two readers behind a load balancer, and
maintenance. Readers in the same region as the bucket read it directly.

**Many readers.** Add an `isledb cache` in each region with readers, and point
the readers at it with `read_cache.url`. The bucket then sees reads from the
caches instead of from every reader, and readers usually see new writes within
a second.

**From a Go program.** A program that embeds IsleDB is the writer or a reader
itself. A database still needs maintenance: run `isledb maintenance` against
the same bucket and prefix, or open it in your program with
`DB.OpenMaintenance`.

## One configuration file

One YAML file can configure every service. Settings they share sit at the
top; each service reads its own section and ignores the others':

```yaml
bucket_url: s3://my-bucket?region=us-east-1
log_level: info

writer:
  prefix: prod/accounts
  auth:
    token_file: /etc/isledb/writer-tokens

reader:
  prefix: prod/accounts
  cache_dir: /var/cache/isledb
  auth:
    token_file: /etc/isledb/reader-tokens

maintenance:
  prefix: prod/accounts
  scratch_dir: /var/lib/isledb/scratch
```

Start each service with `isledb <service> --config isledb.yaml`. Any setting
can also come from an environment variable or a flag; see the
[configuration reference](configuration.md). To check a file without starting
anything, add `--check`: the service prints every setting with its final
value, or what is wrong.

## Health checks and traffic

Each service serves `/healthz`, `/readyz` and `/metrics` on its admin address,
without authentication.

- **Send traffic only to services whose `/readyz` answers `200`,** and use
  `/healthz` for liveness. A service that loses storage stays alive and keeps
  serving what it can; restarting it wouldn't help.
- **The admin address listens on `127.0.0.1` by default.** In a container,
  set it to `0.0.0.0:<port>` so probes and metrics scrapers can reach it, and
  keep that port out of any public service: it also serves Go profiling.

## Rolling out a new version

- **Writer and maintenance:** stop the old one before starting the new one
  (in Kubernetes, `strategy: Recreate`). An overlap doesn't corrupt anything,
  since the newer one takes over, but the older one then stops working
  until you remove it.
- **Readers and the read cache:** roll them in any order; each replica is
  independent.
- **Shutting down:** give each process time to finish: about 90 seconds for
  the writer, 45 for the read cache, 30 for maintenance. Each stops on
  `SIGTERM` or `SIGINT`, and a second signal stops it at once.

## Production checklist

- [ ] The bucket follows [Set up object storage](object-store-guide.md).
- [ ] Exactly one writer and one maintenance per database, rolled out with
      `Recreate`.
- [ ] Authentication on the writer, readers and read cache; TLS wherever
      traffic leaves a private network. See [Secure IsleDB](security.md).
- [ ] Admin addresses reachable only by probes and monitoring.
- [ ] Each reader has its own cache directory, on local disk, with room for
      `caches.disk_bytes` (8 GiB by default).
- [ ] Maintenance has local scratch space, and change-feed retention is set
      if the feed is on.
- [ ] Alerts on the writer's failing saves, readers falling behind and
      maintenance getting stuck. See the [metrics reference](metrics.md).
- [ ] Load balancers allow requests at least as long as the writer's
      `writes.commit_wait` plus a few seconds, and, in front of a read cache,
      idle connections longer than its long poll (30 seconds).
