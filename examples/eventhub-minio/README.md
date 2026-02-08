# Event Hub on MinIO (Producer + Consumer)

This example runs IsleDB as an event stream over MinIO object storage with separate processes:

- `producer`: writes JSON events under `events/<topic>/...`
- `consumer`: tails those keys and stores a local checkpoint

## 1) Start MinIO

```bash
docker run --rm -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data --console-address ":9001"
```

## 2) Run consumer (terminal A)

```bash
go run ./examples/eventhub-minio/consumer \
  -bucket-url 's3://isledb-eventhub?endpoint=http://localhost:9000&region=us-east-1&use_path_style=true' \
  -prefix eventhub \
  -topic orders
```

## 3) Run producer (terminal B)

```bash
go run ./examples/eventhub-minio/producer \
  -bucket-url 's3://isledb-eventhub?endpoint=http://localhost:9000&region=us-east-1&use_path_style=true' \
  -prefix eventhub \
  -topic orders \
  -count 100 \
  -interval 200ms
```

The consumer resumes from the last seen key using a checkpoint file (default under `/tmp/isledb-eventhub`).

The producer also persists local sequence state (default `/tmp/isledb-eventhub/<producer-id>.state.json`)
so restart does not regress `seq`. You can override with `-state-file`.
