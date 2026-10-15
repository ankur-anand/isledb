# Writer HTTP API

The writer service stores keys and values in one IsleDB database. You can
write a key, delete a key, write several keys at once, and check whether a
write was saved.

To run the service, see [isledb-writer.md](isledb-writer.md).

Every path starts with `/v1/`. Every response body is JSON.

## Quick start

```bash
# Write a value
curl -X PUT -H "Authorization: Bearer $TOKEN" \
  --data '{"name":"a"}' 'http://isledb-writer:8070/v1/kv/user%2F42'
# {"write_id":"7.10442","revision":"1042"}

# Delete it
curl -X DELETE -H "Authorization: Bearer $TOKEN" \
  'http://isledb-writer:8070/v1/kv/user%2F42'
```

## Authentication

Send your token in every request:

```
Authorization: Bearer <token>
```

Without a valid token, the writer answers `401`.

## Endpoints

| Request | Does |
|---|---|
| `PUT /v1/kv/{key}` | Writes one key. The request body is the value. |
| `DELETE /v1/kv/{key}` | Deletes one key. |
| `POST /v1/kv` | Writes and deletes several keys at once. |
| `GET /v1/writes/{write_id}` | Tells whether a write was saved. |

## Keys and values

- **Keys in the URL are URL-encoded.** Encode every character that is not a
  letter, digit, `-`, `_`, `.` or `~`. For example, the key `user/42` is
  written `user%2F42`. Any byte can be part of a key.
- **A value can be any bytes**, including nothing at all.
- **In a batch**, keys and values are base64-encoded.

## Write a key

```
PUT /v1/kv/user%2F42
Content-Type: application/json

{"name":"a"}
```
```
200 OK

{"write_id":"7.10442","revision":"1042"}
```

The writer answers once the value is saved, usually within a few hundred
milliseconds. A `200` means the value is stored durably.

The response has two fields:

- **`write_id`** identifies this write. Use it to check on the write later,
  for example after a timeout.
- **`revision`** identifies the version of the database that includes your
  write. Keep it if you need to read your own write back: a read at this
  revision or later always includes it. Treat it as an opaque string; don't
  compare revisions or do arithmetic with them.

**Expiring values.** Add `?ttl=` to make the value expire after a while:

```
PUT /v1/kv/session%2Fabc?ttl=24h
```

Durations look like `500ms`, `30s`, `10m` or `24h`. Without `ttl`, the value
never expires. An empty `ttl=` is an error, so a missing value in your code
doesn't silently store a key forever.

## Delete a key

```
DELETE /v1/kv/user%2F42
```
```
200 OK

{"write_id":"7.10443","revision":"1043"}
```

Deleting a key that doesn't exist is not an error.

## Write several keys at once

Send a list of operations. They are applied together: anyone reading the
database sees either all of them or none of them.

```
POST /v1/kv
Content-Type: application/json

{"ops":[
  {"op":"put","key":"b3JkZXIvNDI=","value":"eyJ0b3RhbCI6OX0=","ttl":"24h"},
  {"op":"put","key":"c3RvY2svNw==","value":"MTI="},
  {"op":"delete","key":"Y2FydC80Mg=="}
]}
```
```
200 OK

{"write_id":"7.10446","revision":"1044"}
```

- `op` is `put` or `delete`.
- `key` and `value` are base64-encoded.
- `ttl` is optional on a `put`.
- A `delete` has only a `key`.
- If the same key appears twice, the last operation wins.

Batches are much faster than one request per key when you write a lot of
data. A batch can hold up to about 16 MiB of keys and values. If it's too
large, the writer answers `413`; split it into smaller batches.

## Write without waiting

To get an answer as soon as the writer has accepted a write, without waiting
for it to be saved, add this header:

```
Prefer: respond-async
```
```
202 Accepted
Location: /v1/writes/7.10447

{"write_id":"7.10447"}
```

The write is saved shortly afterwards. Until then it can still be lost, for
example if the writer crashes. To find out when it's saved, check it at the
`Location` URL.

**Waiting less.** To wait at most a few seconds for a write to be saved, send
`Prefer: wait=N`, where `N` is a number of seconds. If the write isn't
confirmed in time, the writer answers `504` with its `write_id`.

## Check a write

```
GET /v1/writes/7.10442
```
```
200 OK

{"status":"committed","revision":"1042"}
```

| `status` | Meaning |
|---|---|
| `committed` | The write is saved. |
| `pending` | The write is accepted but not saved yet. Ask again after the number of seconds in the `Retry-After` header. |
| `not_committed` | The write was not saved and never will be. It's safe to send it again. |
| `unknown` | The writer can't tell, because the write is too old. |

A writer that was replaced, and can't tell what happened to its own write,
answers `503 unavailable` instead of `unknown`: the writer now holding the
database can answer. Ask again after `Retry-After` seconds.

## Sending a request body

`PUT` and `POST` requests must include a `Content-Length` header. Most HTTP
clients add it automatically: curl with `--data`, Python `requests`,
JavaScript `fetch` with a string body, and Go with a byte slice. Requests
without it are answered `411`.

## Errors

Errors have a JSON body:

```
503 Service Unavailable
Retry-After: 1

{"error":"backpressure","message":"too many writes waiting to commit; retry"}
```

| Status | `error` | Meaning | What to do |
|---|---|---|---|
| 400 | `bad_request` | The request is malformed: bad JSON, an unknown `op`, a bad or empty `ttl`, or an unknown query parameter. The message says what's wrong. | Fix the request. |
| 400 | `invalid_mutation` | A key, value or TTL isn't allowed: an empty or too-long key, a too-large value, a negative TTL, or an empty batch. The message says which operation. | Fix the request. |
| 400 | `bad_write_id` | The write ID isn't valid. | Check the ID. |
| 401 | | Missing or wrong token. | Send a valid token. |
| 404 | `not_found` | No such endpoint. | Check the path. |
| 405 | `method_not_allowed` | Wrong method for this path. The `Allow` header lists the right ones. | Use an allowed method. |
| 408 | `body_timeout` | The request body arrived too slowly. | Retry. |
| 411 | `length_required` | The request has no `Content-Length` header. | Send `Content-Length`. |
| 413 | `too_large` | The value or batch is too large. | Send a smaller value, or split the batch. |
| 503 | `backpressure` | The writer is busy. Nothing was written. | Retry after `Retry-After` seconds. |
| 503 | `unavailable` | The writer isn't taking writes right now, for example while restarting; nothing was written. On `GET /v1/writes/…`, this writer was replaced and another can answer. | Retry after `Retry-After` seconds. |
| 503 | `not_committed` | The write was accepted but will never be saved. The body includes its `write_id`. | Send it again. |
| 504 | `outcome_unknown` | The write was accepted but not confirmed in time. It may still be saved. The body includes its `write_id`. | Check it at `GET /v1/writes/{write_id}` before sending it again. |

## Retrying safely

- **`503` with `backpressure` or `unavailable`:** nothing was written, so
  retry the same request.
- **`503` with `not_committed`:** the write will never be saved, so send it
  again.
- **`504`:** the write may or may not be saved. Check its status first.
  Sending it again without checking can apply it twice. That's harmless when
  you write the same value to the same key, but not for anything else.
