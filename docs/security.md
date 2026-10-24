# Secure IsleDB

IsleDB's services protect three things: the API each serves, the admin
address each exposes, and the storage credentials they all use. This page
covers each, for all services. The settings themselves are in the
[configuration reference](configuration.md).

## Authentication

The writer, readers and read cache refuse to start without authentication,
unless it is turned off with `auth.insecure`, which is for development only.
Maintenance serves no API, so it has nothing to authenticate.

**Tokens.** Put the tokens a service accepts in a file, one per line, and
name it with `auth.token_file`. Blank lines and lines starting with `#` are
skipped. Clients send one of them:

```
Authorization: Bearer <token>
```

A request without a valid token gets `401`. To change the tokens without a
restart, edit the file and send the service `SIGHUP`; if the new file can't
be read or holds no tokens, the service keeps the ones it has. `SIGHUP` never
stops a service.

**What a token allows.** A token is all or nothing for the service that
accepts it: any token the writer accepts can write and delete every key in
the database, and any token a reader accepts can read every key. To keep
read-only clients read-only, give the writer and the readers different token
files, and give writer tokens only to the applications that write.

**Client certificates.** With `tls.client_ca`, a service also requires
clients to present a certificate signed by that CA. With both tokens and a
client CA set, a client needs both.

## TLS

Set `tls.cert` and `tls.key` to serve HTTPS. Use it wherever traffic between
clients and services leaves a network you trust.

A reader that reads through a read cache over HTTPS trusts the system's CAs.
For a cache with a certificate from a private CA, name that CA with the
reader's `read_cache.ca_file`. For a cache that requires client certificates,
give the reader one with `read_cache.cert` and `read_cache.key`, and the
cache's token with `read_cache.token_file`.

## Admin addresses

Each service serves health checks, Prometheus metrics and Go profiling on
its admin address, **without authentication**.

- By default the admin address listens on `127.0.0.1` only.
- In a container, where probes and scrapers come from outside the process,
  listen on all interfaces, but keep the port out of any load balancer or
  public service, and limit who can reach it, for example with a network
  policy that admits only your monitoring.
- An empty admin address turns it off.

## Storage credentials

Services read storage credentials from the provider's usual places:
environment variables, shared config files, or the instance or pod role.
They never go in IsleDB's configuration, which holds only file paths, never
secrets.

Give each service only what it needs, on the database's prefix only:

| Service | Needs |
|---|---|
| Writer | Read, write, delete and list objects |
| Maintenance | Read, write, delete and list objects |
| Reader | Read and list objects |
| Read cache | Read and list objects |

The exact policies for Amazon S3, Google Cloud Storage and Azure Blob Storage
are in [Set up object storage](object-store-guide.md).

## Checklist

- [ ] No service runs with `auth.insecure` outside development.
- [ ] Writer tokens and reader tokens are different files, and writer tokens
      go only to applications that write.
- [ ] TLS wherever traffic leaves a trusted network.
- [ ] Admin addresses reachable only by probes and monitoring.
- [ ] Storage credentials scoped to the database's prefix, with readers and
      the read cache read-only.
