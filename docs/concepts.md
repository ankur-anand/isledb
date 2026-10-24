# How IsleDB works

IsleDB is a key-value database that keeps all of its data in an object
storage bucket, such as Amazon S3, Google Cloud Storage or Azure Blob
Storage. This page explains the parts and how they fit together. The same
model holds whether you run IsleDB as HTTP services or embed it in a Go
program; the table at the end shows the names each uses.

## One database, one prefix

A database is everything under one prefix of a bucket, such as
`s3://my-bucket/prod/accounts/`. The bucket is the only place its data lives:
there is no storage cluster to run, and no disk on any machine that must be
backed up. One bucket can hold many databases, each under its own prefix.

What IsleDB stores there is described in
[the storage layout](object-store-schema.md). Applications never read those
objects directly; they go through a writer and readers.

## The writer

One writer writes to a database. It takes puts and deletes, buffers them,
and saves them to the bucket together, once every flush interval (250 ms by
default). A write is durable once it is saved:

- **Over HTTP,** the writer answers a write once it is saved, unless the
  request asks not to wait.
- **In Go,** `Put` buffers the write; `Flush`, or waiting for its commit,
  confirms it is saved.

Writes made together in one batch are saved together: readers see all of
them or none.

**Only one writer at a time.** If a second writer opens the same database, it
takes over, and the first one stops taking writes. This keeps two writers
from overwriting each other, even when an old and a new one overlap during a
deployment. Run exactly one.

## Readers

Readers serve reads: single keys, scans of a key range, and the change feed.
Run as many as you need. They share nothing with each other or with the
writer, so each one adds capacity, and any of them can answer any request.

A reader answers from the newest version of the database it has loaded, and
it loads the newest version about once a second. It keeps what it reads in
caches on its own disk and in memory, so repeated reads don't go back to the
bucket.

## Revisions and freshness

Every save gives the database a new **revision**, a number that only grows.

- The writer tells you the revision of each write.
- Every read tells you the revision it was answered from (the
  `Isledb-Revision` header over HTTP).
- A read at a revision at or past a write's includes that write.

So a read can be up to about a second behind the writer: it may not yet
include a write made in the last second. When you need to read your own
write, compare revisions and read again if the reader is behind.

If a reader loses access to the bucket, it keeps answering from what it has,
for up to the database's maximum view age (one hour by default), and only
then refuses reads. During a storage outage, reads can therefore be up to
that old.

## Maintenance

As the writer saves, the database gathers many small files. Maintenance
merges them into fewer, larger ones (compaction), so reads stay fast, and
deletes what is no longer needed, so storage doesn't grow without end. It
also deletes change-feed history older than you choose to keep.

Maintenance runs in its own process, on whatever compute suits you, so this
work never competes with the processes serving traffic. It prepares its work,
and the writer publishes it with its next save. Run one per database, in
production always.

## The read cache

The read cache is optional. It sits between the readers in one region and
the bucket: readers read through it, and it keeps one copy of what they read
on its local disk. Many readers then cost the bucket one read instead of one
each, and they usually learn about a new write within a second of it.

If the cache can't answer, readers read the bucket directly, so the cache
never stops reads.

## The change feed

The change feed is optional. When it is on, the database keeps every
committed put and delete, in order, alongside the data. Consumers read it a
page at a time and save their place, a **cursor**, to resume from later. It
keeps a cache, a search index or another system in sync with the database.

The feed keeps either keys only, or keys and values. See
[Follow changes](change-feed.md).

## Services and the Go library

| Part | As a service | In Go |
|---|---|---|
| Database | A bucket URL and a prefix in each service's configuration | `isledb.Open` |
| Writer | `isledb writer`, writes over HTTP | `DB.OpenWriter` |
| Reader | `isledb reader`, reads over HTTP | `DB.OpenReader` |
| Maintenance | `isledb maintenance` | `DB.OpenMaintenance` |
| Read cache | `isledb cache` | A reader's `ReadCache` options |
| Change feed | `GET /v1/changes` on readers | `DB.OpenChangeReader` |

The two mix freely: a Go program can read a database that the writer service
writes, and the maintenance service can maintain a database a Go program
writes.

## What IsleDB is not

- **Not built for low-latency writes.** A write takes about the flush
  interval to be saved, and about a second more to reach readers.
- **Not transactional across reads and writes.** A batch of writes is atomic,
  but there are no transactions that read, decide and write.
- **Not a query engine.** Reads are by key or by key range: no SQL, joins or
  secondary indexes.
