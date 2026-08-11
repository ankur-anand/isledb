# IsleDB

<img src="docs/isledb.svg" width="100%" height="260" alt="IsleDB">

[![CI Tests](https://github.com/ankur-anand/isledb/actions/workflows/go.yml/badge.svg)](https://github.com/ankur-anand/isledb/actions/workflows/go.yml)
[![Coverage Status](https://coveralls.io/repos/github/ankur-anand/isledb/badge.svg?branch=main)](https://coveralls.io/github/ankur-anand/isledb?branch=main)
[![Go Reference](https://pkg.go.dev/badge/github.com/ankur-anand/isledb.svg)](https://pkg.go.dev/github.com/ankur-anand/isledb)
[![License: Apache 2.0](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

**A key-value database for object storage, written in Go.**

Store durable key-value data on Amazon S3, Google Cloud Storage, Azure Blob
Storage, MinIO, or local files. Add reader services independently and run
compaction outside your application processes.

IsleDB provides:

- point reads, range scans, iterators, snapshots, TTLs, and deletes;
- one fenced writer with any number of independent reader processes;
- configurable local caching for readers;
- optional keys-only or full-value change feeds;
- compaction and storage cleanup that can run in a separate process or
  scheduled job.

## Install

```bash
go get github.com/ankur-anand/isledb
```

IsleDB requires Go 1.25 or newer.

## Quick start

This example uses a local file-backed bucket. `Put` buffers the mutation and
`Flush` makes it durable and visible to readers.

```go
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/ankur-anand/isledb"
)

func main() {
	if err := run(context.Background()); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) error {
	dataDir, err := filepath.Abs("./isledb-data")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return err
	}

	db, err := isledb.Open(ctx, "file://"+dataDir, isledb.DBOptions{
		Prefix: "example",
	})
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	writerOptions := isledb.DefaultWriterOptions()
	writerOptions.Flush.Interval = 0

	writer, err := db.OpenWriter(ctx, writerOptions)
	if err != nil {
		return err
	}
	if err := writer.Put(ctx, []byte("hello"), []byte("world")); err != nil {
		return err
	}
	if err := writer.Flush(ctx); err != nil {
		return err
	}
	if err := writer.Close(ctx); err != nil {
		return err
	}

	cacheDir, err := filepath.Abs("./isledb-cache")
	if err != nil {
		return err
	}
	reader, err := db.OpenReader(
		ctx,
		isledb.DefaultReaderOpenOptions(cacheDir),
	)
	if err != nil {
		return err
	}
	defer func() { _ = reader.Close() }()

	value, found, err := reader.Get(ctx, []byte("hello"))
	if err != nil {
		return err
	}
	if found {
		fmt.Printf("hello = %s\n", value)
	}
	return nil
}
```

The same API works with cloud bucket URLs:

| Provider | Example bucket URL |
| --- | --- |
| Amazon S3 | `s3://my-bucket?region=us-east-1` |
| Google Cloud Storage | `gs://my-bucket` |
| Azure Blob Storage | `azblob://my-container` |

Credentials and provider-specific options come from the corresponding
[Go Cloud bucket driver](https://gocloud.dev/howto/blob/). Use a unique prefix
for each database.

## Behavior to know

- `Put`, `PutWithTTL`, and `Delete` buffer mutations in memory.
- A successful `Flush`, background flush, or `Writer.Close` is the durability
  and visibility boundary.
- One writer owns a database prefix at a time. Writer ownership is fenced across
  processes.
- A reader uses a consistent loaded view. It refreshes according to its view
  policy; call `Refresh` when a newly committed write must be discovered
  immediately.
- Reader services scale independently by opening the same bucket and prefix with
  their own local cache directories.
- Run maintenance in production so compaction, checkpoints, configured
  retention, and physical cleanup continue to make progress.

See the [Go API guide](api.md) for scans, iterators, snapshots, prefetching,
configuration, metrics, and error handling.

## Optional change feed

Enable the change feed when another service needs committed mutations in writer
order:

```go
db, err := isledb.Open(ctx, bucketURL, isledb.DBOptions{
	Prefix: "prod/accounts",
	ChangeFeed: &isledb.ChangeFeedOptions{
		Payload: isledb.ChangeFeedFullValues,
	},
})
```

Use `ChangeFeedKeysOnly` for invalidation or downstream re-fetching. Use
`ChangeFeedFullValues` when consumers need the committed PUT values. The mode
is persisted when the feed is enabled and cannot later be changed for that
database prefix.

Consumers use `OpenChangeReader` with an opaque, persistent cursor. See
[Enable and consume the change feed](api.md#enable-and-consume-the-change-feed)
for the complete example and retention behavior.

## Run maintenance separately

Maintenance can run continuously in its own service or periodically as a job.
It opens the same database prefix as the writer and readers.

```go
db, err := isledb.Open(ctx, bucketURL, isledb.DBOptions{
	Prefix: "prod/accounts",
})
if err != nil {
	log.Fatal(err)
}
defer db.Close()

maintenance, err := db.OpenMaintenance(
	ctx,
	isledb.DefaultMaintenanceOptions(),
)
if err != nil {
	log.Fatal(err)
}
defer maintenance.Close(ctx)

if err := maintenance.Run(ctx); err != nil {
	log.Fatal(err)
}
```

The active writer publishes maintenance results, while cleanup proceeds at its
own bounded pace. Use `RunOnce` instead of `Run` for a scheduled job.

## When IsleDB fits

IsleDB is a good fit when:

- object storage is the durability and capacity layer;
- writes can be buffered and published in batches;
- readers should scale without maintaining database replicas;
- second-scale freshness is acceptable;
- compaction should run away from application servers.

Choose a different database when you need:

- consistent sub-10-millisecond read-after-write latency;
- multi-key transactions or compare-and-swap updates;
- secondary indexes, joins, or a query language;
- high-frequency updates to a small, hot dataset.

## Documentation

- [Go API guide](api.md)
- [Go package reference](https://pkg.go.dev/github.com/ankur-anand/isledb)
- [Object-store layout and operations](docs/object-store-schema.md)
- [Local file example](examples/kvfile)
- [Azure Blob example](examples/wal-azblob)
- [MinIO producer and consumer example](examples/eventhub-minio)

## Acknowledgments

IsleDB uses the SSTable implementation from
[Pebble](https://github.com/cockroachdb/pebble). Thanks to the CockroachDB team
for building and open-sourcing it.

## License

IsleDB is licensed under the [Apache License 2.0](LICENSE).
