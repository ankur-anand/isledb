# Separate services with MinIO

This example runs one IsleDB database through three independent Go processes:

- `writer` commits account updates;
- `reader` refreshes and queries the same database;
- `maintenance` compacts and reclaims storage away from the application
  processes.

MinIO provides the local S3-compatible object store. The database is kept in
the `isledb` bucket under the `services` prefix.

## Start MinIO

```bash
docker compose -f examples/minio-services/compose.yaml up -d
docker compose -f examples/minio-services/compose.yaml wait create-bucket
```

The Compose project creates the bucket automatically. The MinIO API is on
`localhost:9000`, and its console is on `localhost:9001`.

## Start the services

Run each command in a separate terminal:

```bash
go run ./examples/minio-services/writer
```

```bash
go run ./examples/minio-services/reader
```

```bash
go run ./examples/minio-services/maintenance
```

Keep the writer running while maintenance runs. Maintenance prepares fenced
commands; the active writer publishes accepted commands through the database's
authoritative manifest update.

## Stopping the writer

The writer shuts down the way a service should on `SIGTERM` (or `Ctrl-C`):

1. `Drain` stops new writes at once and commits every accepted one,
   retrying, within most of the `-grace` period (default 25 seconds, under
   Kubernetes' default 30-second termination grace).
2. `Close` finishes the writer with the rest of the grace period; its error
   names any writes it could not confirm.
3. `DB.Close` releases the database.

While it runs, `http://localhost:8081/ready` (`-ready-addr`) serves the
writer's `State` as JSON: `200` while it takes writes, `503` from the moment
shutdown starts, so a load balancer stops sending work first.

```text
shutting down: 39 writes pending (5852 bytes), grace 25s
drained: everything through sequence 39 is committed
writer closed
```

Stop other processes with `Ctrl-C`. Stop MinIO with:

```bash
docker compose -f examples/minio-services/compose.yaml down
```

Use `-h` on any Go command to see its options. `ISLEDB_MINIO_URL` and
`ISLEDB_PREFIX` override the shared object-store location.
