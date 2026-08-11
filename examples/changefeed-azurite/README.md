# Change feed with Azurite

This example enables IsleDB's durable change feed and runs it through separate
producer and consumer processes. Azurite supplies a local Azure Blob Storage
endpoint; the feed API is the same on Azure Blob Storage and other supported
object stores.

The producer commits puts and deletes with full values enabled. The consumer
reads bounded pages and saves `page.Next` only after it processes the complete
page, so it resumes from the same position after a restart.

## Start Azurite

```bash
docker compose -f examples/changefeed-azurite/compose.yaml up -d
```

## Start the producer

Run the producer first so it creates the database with its change feed enabled:

```bash
go run ./examples/changefeed-azurite/producer -count 0
```

## Start the consumer

In another terminal:

```bash
go run ./examples/changefeed-azurite/consumer
```

Stop and restart the consumer to verify that it resumes from its saved cursor.
The default cursor file is
`$TMPDIR/isledb-changefeed-azurite/consumer.cursor`. Delete that file or pass a
different `-cursor-file` to start a new consumer position.

By default an expired cursor stops the consumer, preserving an explicit
recovery decision. Use `-on-expired=oldest` when replaying from the earliest
retained change is the desired policy.

Use `-h` on either program to see its options. Stop Azurite with:

```bash
docker compose -f examples/changefeed-azurite/compose.yaml down
```

