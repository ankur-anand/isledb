## IsleDB

<img src="docs/isledb.svg" width="100%" height="260" alt="isledb">

IsleDB is an embedded key-value engine designed for object storage. It borrows ideas from LSM-trees but
rethinks them for object storage.

Writes go to an in-memory memtable first, then get flushed to SST files periodically. This batching
matters—instead of hitting object storage on every `put()`, you amortize costs across many writes. Large
values get stored separately as blobs so the SSTs stay small.

The SST files themselves live in object storage (S3, GCS, Azure, MinIO, etc). Your capacity and
durability scale with the bucket, not your local disk.

### Features
1. Data lives on object storage (S3, GCS, Azure Blob, MinIO). 
2. Bottomless capacity. 
3. Object Store durability. 
4. Readers scale horizontally-no replicas, no connection limits.

### Getting Started

#### Create a Store and DB
```go
ctx := context.Background()

store, err := blobstore.NewFile(ctx, "./data", "db1")
if err != nil {
	log.Fatal(err)
}
defer store.Close()

db, err := isledb.OpenDB(ctx, store, isledb.DBOptions{})
if err != nil {
	log.Fatal(err)
}
defer db.Close()
```

Cloud buckets (S3, GCS, Azure) use Go Cloud bucket URLs:
```go
ctx := context.Background()

// S3
store, err := blobstore.Open(ctx, "s3://my-bucket?region=us-east-1", "db1")
if err != nil {
	log.Fatal(err)
}

// GCS
store, err = blobstore.Open(ctx, "gs://my-bucket", "db1")
if err != nil {
	log.Fatal(err)
}

// Azure Blob
store, err = blobstore.Open(ctx, "azblob://my-container", "db1")
if err != nil {
	log.Fatal(err)
}
defer store.Close()

db, err := isledb.OpenDB(ctx, store, isledb.DBOptions{})
if err != nil {
	log.Fatal(err)
}
defer db.Close()
```

#### Writer (single writer per bucket/prefix)
```go
opts := isledb.DefaultWriterOptions()
writer, err := db.OpenWriter(ctx, opts)
if err != nil {
	log.Fatal(err)
}
defer writer.Close()

if err := writer.Put([]byte("hello"), []byte("world")); err != nil {
	log.Fatal(err)
}
if err := writer.Flush(ctx); err != nil {
	log.Fatal(err)
}
```

#### Reader
```go
reader, err := isledb.OpenReader(ctx, store, isledb.ReaderOpenOptions{
	CacheDir: "./cache",
})
if err != nil {
	log.Fatal(err)
}
defer reader.Close()

value, ok, err := reader.Get(ctx, []byte("hello"))
if err != nil {
	log.Fatal(err)
}
if ok {
	log.Printf("value=%s", value)
}
```

#### Tailing Reader
```go
tr, err := isledb.OpenTailingReader(ctx, store, isledb.TailingReaderOpenOptions{
	RefreshInterval: time.Second,
	ReaderOptions: isledb.ReaderOpenOptions{
		CacheDir: "./cache",
	},
})
if err != nil {
	log.Fatal(err)
}
defer tr.Close()

tr.Start()
err = tr.Tail(ctx, isledb.TailOptions{
	PollInterval: time.Second,
}, func(kv isledb.KV) error {
	log.Printf("%s=%s", kv.Key, kv.Value)
	return nil
})
if err != nil {
	log.Fatal(err)
}
```

#### Compactor (SSTable Compactor)
```go
compactor, err := db.OpenCompactor(ctx, isledb.DefaultCompactorOptions())
if err != nil {
	log.Fatal(err)
}
defer compactor.Close()

compactor.Start()
```

### Examples
- `examples/kvfile`: local file-backed KV usage with writer/reader/tailer.
- `examples/walazblob`: WAL-style event stream on Azurite/Azure Blob (tailing events).
