# Reader HTTP API

The reader service reads keys, and scans ranges of keys, from one IsleDB
database. Run as many readers
as you like: each serves reads on its own, so add replicas to serve more.

To run the service, see [isledb-reader.md](isledb-reader.md). Writes go to
the writer service, described in [writer-api.md](writer-api.md).

Every path starts with `/v1/`.

## Quick start

```bash
curl -H "Authorization: Bearer $TOKEN" 'http://isledb-reader:8080/v1/kv/user/42'
# {"name":"a"}
```

## Authentication

Send your token in every request:

```
Authorization: Bearer <token>
```

Without a valid token, the reader answers `401`.

## Endpoints

| Request | Does |
|---|---|
| `GET /v1/kv/{key}` | Reads one key. The response body is the value. |
| `HEAD /v1/kv/{key}` | The same, without the body: whether the key exists, and its size. |
| `GET /v1/kv?prefix=…` | Lists keys and their values, a page at a time. |

The paths match the writer's, so a gateway can serve one API: send `GET` and
`HEAD` requests for `/v1/kv` and `/v1/kv/…` to readers, and everything else, including
`GET /v1/writes/…`, to the writer.

## Read a key

```
GET /v1/kv/user/42
```
```
200 OK
Content-Type: application/octet-stream
Content-Length: 12
Isledb-Revision: 1042

{"name":"a"}
```

- **Keys go in the URL path**, as for the writer. A key like `user/42` can
  be sent as it is: `/v1/kv/user/42`. URL-encode a key (`%XX`) when it holds `%`, `?`, `#`, a
  space or anything other than printable ASCII, or when it has an empty, `.`
  or `..` part between slashes, like `a//b` or `../x`: clients and proxies
  change those before the request arrives. Encoding every character that is
  not a letter, digit, `-`, `_`, `.` or `~` is always safe, so `user/42` may
  also be written `user%2F42`. Any byte can be part of a key.
- **The body is the value's bytes**, exactly as written. It's the only
  response that isn't JSON.
- **`Isledb-Revision`** is the revision the reader answered from. Every
  write whose `revision` is at or below it is included.
- **A missing key** is `404` with `{"error":"key_not_found",…}`, still with
  `Isledb-Revision`: the key was not there at that revision. A key whose TTL
  has passed is missing.
- **`HEAD`** answers the same status and headers, without the body.

## Scan keys

```
GET /v1/kv?prefix=user/&limit=2
```
```
200 OK
Content-Type: application/json
Isledb-Revision: 1042

{"items":[{"key":"user/1","value":"{\"name\":\"a\"}"},
          {"key":"user/2","value":"{\"name\":\"b\"}"}],
 "next_page":"AXVzZXIvMw"}
```

Choose the keys with `prefix`, or with `start` and `end`; leave all three
out to scan every key:

| Parameter | Meaning |
|---|---|
| `prefix` | Keys that start with it. Not with `start` or `end`. |
| `start` | The first key, included. |
| `end` | Where to stop: keys before it, not itself. |
| `limit` | The most items in a page: 1 to 1000, 100 by default. |
| `page` | The `next_page` from the page before. |

For example, with the keys `order/0001` to `order/0500`:

```
GET /v1/kv?start=order/0100&end=order/0200     order/0100 to order/0199
GET /v1/kv?start=order/0450                    order/0450 and every key after it
GET /v1/kv?end=order/0003                      every key before order/0003
GET /v1/kv?prefix=order/                       every key starting with order/
```

`end` itself is never included. To include it, add a zero byte:
`end=order/0200%00` stops right after `order/0200`.

- **Items come in key order**, sorted by their bytes.
- **Each item is text or base64**: `key` and `value` when they are valid
  UTF-8 text, else `key_base64` and `value_base64`, the same fields a write
  batch takes. An empty value is `"value":""`.
- **Get the next page** by sending the same request with `page` set to
  `next_page`. No `next_page` means there are no more keys.
- **A page may hold fewer than `limit` items** even when more follow: it also
  stops at about 1 MiB of keys and values. It always holds at least one item.
- **Each page reads the newest data** and has its own `Isledb-Revision`. A key
  written or deleted while you page through may or may not be seen, but no
  key comes twice, and every key there for the whole scan is seen. A scan can
  move to another replica between pages.
- **Values in query parameters are URL-encoded**, so any byte can be part of
  a prefix or bound.

## How fresh a read is

A reader answers from the newest data it has loaded. It loads the latest
data about once a second, or, when it reads through an `isledb cache`,
usually within a second of each write. So a read can miss a write made in the
last second or so. On Google Cloud Storage, which takes at most one write a
second to the database, allow about two seconds.

If a reader can't reach storage, it keeps answering from the data it has
until that data is `max_pinned_view_age` old (1 hour by default), and only
then answers `503`. So during a storage outage, reads can be up to that old.

Responses carry `Cache-Control: no-cache`, so caches in front of the readers
check before reusing an answer.

To check whether a read includes a particular write, compare its
`Isledb-Revision` with the `revision` the writer returned: a read at or past
that revision includes it.

## Errors

Error responses are JSON:

```json
{"error":"key_not_found","message":"no such key"}
```

| Status | `error` | Meaning | What to do |
|---|---|---|---|
| 400 | `bad_request` | The request is malformed: an empty key, a key not encoded correctly, a query parameter on a key read, or a scan parameter that is unknown, repeated, empty or out of range, or a `page` from another scan. | Fix the request. |
| 401 | | The token is missing or wrong. The body is plain text. | Send a valid token. |
| 404 | `key_not_found` | The key doesn't exist, or its TTL has passed. | |
| 404 | `not_found` | There is no such endpoint. | Check the path. |
| 405 | `method_not_allowed` | A write was sent to a reader, or a scan wasn't a `GET`. | Send writes to the writer. |
| 503 | `backpressure` | The reader is busy. | Retry after `Retry-After` seconds, or on another replica. |
| 503 | `unavailable` | The reader has no current data to read from, for example while starting, or after losing access to storage for longer than the database's `max_pinned_view_age` (1 hour by default); or reading storage failed, or took longer than a minute. | Retry after `Retry-After` seconds, or on another replica. |
