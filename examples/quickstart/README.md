# Local quickstart

This example opens IsleDB on the local filesystem, commits a small batch, and
reads it back with both `Get` and a bounded prefix scan.

```bash
go run ./examples/quickstart
```

By default, data and the reader cache live under your operating system's
temporary directory. Pass `-data-dir` and `-cache-dir` to choose persistent
locations:

```bash
go run ./examples/quickstart \
  -data-dir ./isledb-data \
  -cache-dir ./isledb-cache
```

