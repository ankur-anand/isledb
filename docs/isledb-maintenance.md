# Running `isledb maintenance`

`isledb maintenance` is a service that looks after one IsleDB database: it
compacts the files the writer saves, keeps the database's metadata short so
readers load it quickly, and deletes what is no longer needed. It serves no
API; it has only an admin address for health checks and metrics.

Run it in production. Without it, every save adds a file that readers check
and that is never deleted, so reads slow down and storage grows.

## Start maintenance

With a configuration file:

```
isledb maintenance --config /etc/isledb/isledb.yaml
```

Or with flags:

```
isledb maintenance \
  --bucket-url 's3://my-bucket?region=us-east-1' \
  --prefix prod/accounts
```

Maintenance reads storage credentials from the usual places for your cloud
provider, like the writer. It needs to read, write and delete objects under
the database's prefix. Credentials never go in its configuration.

Maintenance and the writer can start in any order. Maintenance prepares its
work, and the writer publishes it with its next save, so its results reach
readers only while a writer is running.

## Run one maintenance per database

Only one maintenance process works on a database at a time. If you start a
second one for the same database, it takes over, and the first one stops
working and reports not ready.

- Run exactly one replica.
- When you deploy a new version, stop the old one before starting the new
  one. In Kubernetes, use `strategy: Recreate`. A short overlap does no harm:
  the newer one takes over.

A replaced process stays running, not ready, and doesn't take the database
back on its own. Stop whichever one shouldn't be running.

## Configuration

You can set each option in a YAML file, as an environment variable, or as a
flag. Flags take priority over environment variables, which take priority
over the file.

A typical configuration:

```yaml
bucket_url: s3://my-bucket?region=us-east-1
log_level: info

maintenance:
  prefix: prod/accounts
  scratch_dir: /var/lib/isledb/scratch
```

Every option of the `maintenance` section, at its default where it has one:

```yaml
maintenance:
  admin_listen: 127.0.0.1:8061
  shutdown_drain: 0s

  prefix: prod/accounts          # required
  scratch_dir: ""                # empty: the user's cache directory

  change_feed:
    retain_for: 0s               # 0s keeps all history; e.g. 168h
```

To check a configuration without starting maintenance:

```
isledb maintenance --config isledb.yaml --check
```

Every setting, with its flag, environment variable and default, is in the
[configuration reference](configuration.md#maintenance). Maintenance has no
API, so `listen`, `auth` and `tls` don't apply: setting them is an error.

## Scratch space

Compaction writes the files it is merging to `scratch_dir` instead of holding
them in memory. Give it a local disk with room for the files of the compaction
in progress, and in a container mount a volume there: the default cache
directory may be small or read-only. Maintenance removes its
own files when it stops, and on its next start it removes any a crash left
behind. It never deletes anything else in the directory.

## Change-feed history

By default the change feed keeps every change forever. With
`change_feed.retain_for` set, maintenance deletes changes older than that.
A consumer whose saved position is older than what is kept can't resume from
it without a gap. Set it longer than the longest time a consumer may be
stopped; see [Follow changes](change-feed.md#retention).

## Health checks

Served on `admin_listen`, without authentication.

| Path | Answers |
|---|---|
| `/healthz` | `200` while the process is running. |
| `/readyz` | `200` while maintenance is working. `503` after another maintenance process has taken over, and while shutting down. |
| `/metrics` | Prometheus metrics. |
| `/debug/pprof/` | Go profiling. |

## Metrics

Maintenance's metrics, and what to alert on, are in the
[metrics reference](metrics.md#maintenance). Watch at least for maintenance
getting stuck, finished work waiting for a writer, work the writer keeps
refusing, and maintenance being replaced.

## Shutting down

When maintenance receives `SIGTERM` or `SIGINT`, it stops the round in
progress, removes its scratch files and exits, within about 30 seconds. A
round stopped partway is safe: its work is redone after the next start.
