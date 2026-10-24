# Follow changes

The change feed is an ordered record of every put and delete committed to a
database. A consumer reads it a page at a time, applies each change, and saves
its place, so after a restart it carries on exactly where it stopped. Use it
to keep a cache, a search index or another system in sync with the database.

This guide covers turning the feed on, reading it over HTTP or in Go, and
running a consumer that doesn't lose or skip changes. The exact requests and
types are in the [HTTP API](reader-api.md#follow-the-change-feed) and the
[Go API](../api.md#change-feed).

## Turn it on

The writer turns the feed on, and it stays on for the life of the database.
Choose what it keeps:

| Mode | Keeps | For |
|---|---|---|
| `keys` | The key and the operation of each change | Invalidating caches; consumers that read the current value themselves |
| `values` | Also the value of each put | Copying the data elsewhere; replaying history |

- **Writer service:** `--change-feed keys` or `--change-feed values`
  (`writer.change_feed.payload`).
- **Go:** open the database with `DBOptions.ChangeFeed`, then open the writer.

The mode can't change once set. Turning the feed on for a database that
already has data starts it at that moment: earlier history isn't added.

## Read it

Every change has its `sequence`, its operation (`put` or `delete`), its key,
its value in `values` mode, and its `revision`: the revision of the commit
that holds it. A read at or past that revision includes the change.

**Over HTTP,** any reader replica serves the feed:

```
GET /v1/changes?from=oldest&limit=1000
```
```json
{"changes":[{"sequence":41,"revision":1042,"op":"put","key":"user/42","value":"…"}],
 "next":"cf1_AAAAAAAAAAIAAAAAAAAAAA",
 "caught_up":true}
```

`from` is a saved cursor, `oldest` for every change still kept, or `head` for
only changes from now on. When `caught_up` is true there is nothing newer
yet: poll again with the same `next`, for example every second. A reader
answers a caught-up poll from what it has loaded, so polling often is cheap.

**In Go,** open a `ChangeReader` and call `Read` with a cursor. `Bounds` gives
the oldest and newest positions, and `WaitForChanges` returns once there is
something new.

## Keep your place

The cursor is the consumer's whole state. To never lose or repeat more than
you mean to:

1. **Choose where to start once, and save that cursor before applying
   anything.** A consumer that starts from `head`, crashes before saving, and
   restarts would start from a newer head and skip what changed between.
2. **Save `next` only after the whole page is applied.** After a crash, the
   page is read again, so apply changes in a way that can safely run twice:
   a put sets a key, a delete removes it.
3. **Resume from your saved cursor,** on any replica.

A page may hold fewer changes than asked for, even none, while more follow.
Keep reading until it is caught up.

## Build a copy, then keep it current

To start a copy of the database and then follow it without a gap:

**Over HTTP:**

1. Read `GET /v1/changes?from=head` and save its `next`.
2. Copy every key with a scan.
3. Follow the feed from the saved cursor.

Changes made during the scan are applied twice, once by the scan and once by
the feed, and the copy ends up right.

**In Go,** `Reader.BootstrapView` gives a snapshot of the data and the cursor
that follows it exactly: copy the snapshot, then follow the feed from that
cursor.

## Keys-only consumers

In `keys` mode a put carries no value, so the consumer reads the key itself.
The replica that answers may not have loaded the change yet, and would return
the old value. Check the read's revision:

- If the read's `Isledb-Revision` is at or past the change's `revision`, use
  the value; a `404` there means the key has since been deleted or has
  expired.
- If it is below, the replica is behind: read again shortly.

## Retention

By default the feed keeps every change forever. To delete changes older than
a given age, set it on maintenance:

- **Maintenance service:** `--change-feed-retain-for 168h`
  (`maintenance.change_feed.retain_for`).
- **Go:** `MaintenanceOptions.ChangeFeedRetention`.

Keep changes longer than the longest time a consumer may be stopped. A
consumer whose cursor is older than what is kept gets `410 cursor_expired`
over HTTP, or `ErrChangeCursorExpired` in Go. It can either continue from the
oldest change kept, accepting the gap, or rebuild its copy as above and
follow from there.

## Freshness and cost

A new change reaches consumers when a reader loads it: within about a second,
or sooner through a read cache. However many consumers poll, a reader reads
storage about once for each new commit; consumers that are caught up are
answered from memory.
