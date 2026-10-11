# Read cache HTTP API

The read cache sits between IsleDB readers in one region and object storage.
It serves two things:

- **`CURRENT`**, the one mutable object, relayed from a single copy the cache
  reads from storage every 500 ms.
- **Immutable objects** (SSTs, change batches, manifest pages and snapshots),
  by byte range, from 128 KiB chunks kept on the cache's disk.

Running it: [isledb-cache.md](isledb-cache.md).

Version: `v1`. Every path starts with `/v1/`.

## Endpoints

| Method and path | Serves |
|---|---|
| `GET /v1/current?db=P` | The cache's copy of database `P`'s `CURRENT` |
| `GET /v1/current?db=P&after=N` | Its position only, once it passes `N` (long poll) |
| `GET /v1/object/{key}` | Bytes of an immutable object, whole or by range |

`P` is a database's prefix in the bucket, as configured on the cache, and
`/` for a database at the bucket root. `db` may be left out when the cache
serves one database.

`{key}` is the object's full storage key, including the database prefix,
exactly as the reader's store names it, for example
`db/sstable/a3f/000123.sst`. Only objects of a database the cache serves are
answered (see below).

## Authentication

Every request to these endpoints carries `Authorization: Bearer <token>`, or
comes over TLS with a client certificate, as the cache is configured. A
request without valid credentials answers **401** with
`WWW-Authenticate: Bearer`.

## `GET /v1/current`

Returns the cache's copy of `CURRENT`, or, as a long poll, only its
position once it passes a given one. A long poll is a doorbell: a reader
that sees a new position reads `CURRENT` with a plain `GET /v1/current`, so
`CURRENT` reaches readers by one path, where the client rules below apply.

**Log position.** The position is `CURRENT`'s `next_seq`: the sequence
number the next manifest log entry will get. Every entry appended to the
manifest log raises it by one, in the same conditional write of `CURRENT`
that adds the entry. Entries record changes to the database's structure:
a flush adding an SST (with its change batch), a compaction, SSTs removed,
a checkpoint, or a writer or compactor taking ownership. User writes do not
move it until a flush. It never goes down. Some rewrites of `CURRENT` add no
entry and keep the position, such as folding the log into a new snapshot.

**Query**

| Parameter | Meaning |
|---|---|
| `after=N` | Optional. Long poll: hold the request until the copy's log position is above `N`, or until the server's long-poll timeout, then answer with the position only. `N` is a log position. |

**Response headers**

| Header | Without `after` | With `after` |
|---|---|---|
| `Isledb-Position` | Log position of the `CURRENT` returned: its `next_seq` | The copy's log position |
| `Isledb-Age-Ms` | Milliseconds since the cache read this `CURRENT` from storage, rounded up. Measured on the cache's monotonic clock; not a timestamp. | – |
| `Content-Type` | `application/json` | – |

Without `after`, the body is `CURRENT`'s bytes as stored. With `after`, the
body is empty.

**Status**

| Status | When |
|---|---|
| 200 | The copy, with position and age; or, for a long poll, the position. A long poll that timed out also answers 200, with a position not above `N`. |
| 400 | `after` is not a number, or `db` is missing on a cache serving several databases |
| 401 | No valid credentials |
| 404 | `db` names a database the cache does not serve |
| 503 | The cache has not read `CURRENT` once yet; a long poll gets it at once, not after the timeout |

**Behaviour**

- The copy never moves back. A `CURRENT` in storage at a lower position is
  refused and logged; a store recreated under the same prefix needs the cache
  restarted.
- A `CURRENT` at the same position with other bytes, such as one rewritten
  around a new snapshot, replaces the copy. It does not answer long polls, which wait for a higher
  position; the next request gets the new bytes.
- When storage cannot be read, the cache keeps serving its copy, and its age
  grows. Readers decide whether it is still young enough.

**Client rules** (enforced by the reader, so a faulty cache cannot break it)

1. **Age limit.** Reject a `CURRENT` whose age is above 5 seconds and read
   storage directly.
2. **Load time = the time the request was sent (own clock) − the age.** This
   is when the view's `MaxPinnedViewAge` starts.
3. **Older means nothing new.** A `CURRENT` from the cache at a position
   below the reader's published view means "no change", not "the store went
   back".
4. **`Refresh` reads storage directly.** The relay can be up to one poll
   interval behind.
5. **Long poll.** Ask with `after=<published position>`; on an answer above
   it, call `RequestRefresh` and ask again. `RequestRefresh` reads `CURRENT`
   with a plain `GET`, where rules 1 to 3 apply. After a timeout or error,
   wait a random 0 to 1 second before asking again.
6. **Fallback.** On a connection error, a timeout or a 5xx, read storage
   directly.

**Example**

```
$ curl -si -H "Authorization: Bearer $TOKEN" 'http://cache:8090/v1/current?db=prod/policy&after=41'
HTTP/1.1 200 OK
Isledb-Position: 42
Content-Length: 0

$ curl -si -H "Authorization: Bearer $TOKEN" 'http://cache:8090/v1/current?db=prod/policy'
HTTP/1.1 200 OK
Content-Type: application/json
Isledb-Age-Ms: 212
Isledb-Position: 42

{"layout_version":…,"next_seq":42,…}
```

## `GET /v1/object/{key}`

Returns bytes of an immutable object.

**Keys served.** Only objects that are never rewritten, each a fixed shape
right after its database's prefix `P` (nothing before the shape for the
root):

| Object | Key |
|---|---|
| SST | `P/sstable/{bucket}/{id}`, where `{bucket}` is the one `id` maps to |
| Change batch | `P/changes/{bucket}/{id}`, same rule |
| Manifest snapshot | `P/manifest/snapshots/{name}.manifest.zst` |
| Manifest page | `P/manifest/pages/l{NN}/{name}.page.zst` |

A key is answered only when what comes before its shape is exactly a
database the cache serves. So each database is served only its own objects:
serving `prod/a` does not serve `prod/a/b/sstable/…` of a database nested
under it, and serving the root does not serve `prod/a/sstable/…`. Anything
else, including `manifest/CURRENT` and `maintenance/HEAD`, answers 403.

**Request headers**

| Header | Meaning |
|---|---|
| `Range` | Optional. One range: `bytes=a-b`, `bytes=a-` or `bytes=-n`. Without it, the whole object. Several ranges in one header are refused (416). |
| `Isledb-Object-Size` | Optional. The object's size in bytes, its full stored length, at most 5 TiB. With it, the cache does not ask storage for the size. IsleDB readers always send it. |
| `Isledb-No-Cache` | Optional, any value. Serve the bytes without keeping what is read from storage. Chunks already on disk are still used. For scans. |

**Response headers**

| Header | Meaning |
|---|---|
| `Content-Range` | On 206: `bytes a-b/size`. On 416: `bytes */size`. |
| `Content-Length` | Bytes in the body |
| `Isledb-Cache` | How the first 4 MiB of the response were served: `hit` (all from disk), `shared` (read from storage by another request in flight, none by this one), `miss` (this request read from storage) |
| `Accept-Ranges` | `bytes` |
| `Content-Type` | `application/octet-stream` |

**Status**

| Status | When | Client should |
|---|---|---|
| 200 | Whole object | Use it |
| 206 | The range asked for | Use it |
| 400 | `Isledb-Object-Size` is not a positive number up to 5 TiB, or is not the object's size in storage | Read storage directly; report a bug |
| 401 | No valid credentials | Fix the token or certificate; read storage directly meanwhile |
| 403 | Not an immutable object of a database the cache serves | Read storage directly; report a bug |
| 404 | The object is not in storage | Retry storage directly once, then report not found |
| 405 | `HEAD` request; only `GET` is served (`Allow: GET`) | Use `GET` |
| 416 | Range outside the object, or several ranges | Report a bug |
| 502 | Storage failed | Read storage directly |
| other 5xx, connection error, timeout | The cache is unhealthy | Read storage directly |

A 404 is retried against storage because a cache pointed at the wrong
bucket or prefix would answer 404 for everything; readers must not trust it
alone.

**The size**

- The size must be the object's full stored length. For an SST that is
  `max(Size, BloomOffset+BloomLength)`, because the Bloom sidecar is stored
  after the SST's `Size`. For a change batch it is the batch's `Size`.
- Chunks are kept per key and size. A request with a wrong size never reads
  or replaces chunks kept for the right one.
- Every read from storage checks the size against the size storage reports,
  at no extra request. A wrong size is refused (400) and nothing is kept, so
  only bytes read under the right size are ever served from disk.
- Without the header the cache asks storage once (a HEAD) and remembers the
  size in memory until restart.
- A wrong size larger than the object can instead fail at storage, which
  refuses a range past the end; the cache then answers 502.

**Behaviour**

- Objects are kept as 128 KiB chunks, aligned at offset 0, the same size as
  the reader's own chunks. The last chunk is shorter.
- Missing chunks next to each other are read from storage in one request,
  up to 32 chunks (4 MiB). A request also reads again up to 8 chunks it
  already has, to join two missing runs into one request.
- Concurrent requests for the same missing chunk share one storage read.
- A storage read is not tied to the request that started it: a client that
  disconnects does not fail others waiting on the same chunks.
- Storage reads in progress are capped in bytes (`MaxReadBytes`, default
  256 MiB). Past the cap, a read waits its turn, in arrival order, for at most
  `MaxReadWait` (default 2 minutes), and then fails with 502. The wait does
  not shorten the storage request's own 30-second timeout. A busy cache
  therefore answers late rather than failing: a reader's timeout to the cache
  should be as long as the delay it accepts before reading storage itself.
  Set `MaxReadWait` near that timeout plus a margin: a queued read keeps its
  place after its reader gives up, so a wait much longer than the readers'
  timeout spends budget on fetches whose requesters have left, ahead of
  requests still waiting.
  Hits are never queued.
- A response is sent 4 MiB at a time. The status and headers are sent once
  the first 4 MiB are in hand, so an error there gets its own status. A
  failure after that ends the body early; the client sees fewer bytes than
  `Content-Length`.
- The cache does not check object contents. Readers verify every object as
  they do when reading storage directly (SST, Bloom, page, snapshot and
  change-index checksums, and Pebble block checksums).

**Examples**

A lookup's chunk, cold and then warm:

```
$ curl -si -H "Authorization: Bearer $TOKEN" http://cache:8090/v1/object/db/sstable/a3f/000123.sst \
    -H 'Range: bytes=131072-262143' -H 'Isledb-Object-Size: 8421376'
HTTP/1.1 206 Partial Content
Content-Range: bytes 131072-262143/8421376
Content-Length: 131072
Isledb-Cache: miss
…
```

The next request for that chunk answers `Isledb-Cache: hit`.

A manifest page, whole:

```
$ curl -s -H "Authorization: Bearer $TOKEN" http://cache:8090/v1/object/db/manifest/pages/l00/p17.page.zst \
    -H 'Isledb-Object-Size: 20381' -o p17.page.zst
```

A scan that should not fill the cache:

```
$ curl -s -H "Authorization: Bearer $TOKEN" http://cache:8090/v1/object/db/sstable/a3f/000123.sst \
    -H 'Range: bytes=0-4194303' -H 'Isledb-Object-Size: 8421376' \
    -H 'Isledb-No-Cache: 1'
```

## Not built yet

- The reader-side client: reading through the cache, the rules above, and
  fallback.
