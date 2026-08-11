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

Stop a process with `Ctrl-C`. Stop MinIO with:

```bash
docker compose -f examples/minio-services/compose.yaml down
```

Use `-h` on any Go command to see its options. `ISLEDB_MINIO_URL` and
`ISLEDB_PREFIX` override the shared object-store location.
