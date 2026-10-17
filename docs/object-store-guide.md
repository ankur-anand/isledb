# Choosing and setting up object storage

IsleDB keeps each database in one object-storage bucket, below one prefix.
This guide says which object stores work, how to create a bucket for IsleDB,
and which bucket features to leave off.

For what IsleDB stores and where, see
[object-store-schema.md](object-store-schema.md).

## What IsleDB needs from a store

- **Conditional writes.** IsleDB writes "only if this object is unchanged" and
  "only if this object doesn't exist yet". This is what keeps two writers from
  overwriting each other's commits. A store that ignores these conditions
  can lose data, so it isn't supported.
- **Strong consistency.** A read or a listing right after a write sees that
  write.
- **One object rewritten on every save.** The writer rewrites
  `manifest/CURRENT`, a small object, each time it saves a group of writes:
  up to several times a second while writes keep arriving. Features that keep
  a copy of every overwritten object (versioning, soft delete) turn this into
  a pile of stored copies that grows with every save.
- **Many deletes.** IsleDB deletes data files it no longer needs. Features
  that block or delay deletes (retention locks, holds, minimum storage
  durations) make storage grow.

## Which stores work

| Store | Works? | Notes |
|---|---|---|
| Amazon S3 (general purpose buckets) | Yes | |
| Google Cloud Storage | Yes | The writer saves at most once per second. |
| Azure Blob Storage | Yes | Use a standard account without hierarchical namespace. |
| MinIO and MinIO AIStor | Yes | Self-hosted. |
| Tigris | Yes | Use a single-region or multi-region bucket, not global or dual-region. |
| Cloudflare R2 | Readers yes; writer not yet recommended | R2 allows about one write per second to an object, and the writer can exceed it. |
| Ceph Object Gateway (RGW) | Not recommended | Current releases have known bugs in conditional writes under contention. |
| Amazon S3 Express One Zone (directory buckets) | Untested | |
| Wasabi, Hetzner, Scaleway, SeaweedFS, IBM Cloud Object Storage, Akamai | Untested | Check conditional writes before using one; see [Check a store yourself](#check-a-store-yourself). |
| Backblaze B2, DigitalOcean Spaces, Oracle Cloud (S3 API), Garage | No | They don't support conditional writes, or ignore them. |
| Local directory (`file://`) | Development only | One process at a time. |

## Settings for every bucket

Whatever the store, create the bucket like this:

- **Use a bucket, or a prefix, for IsleDB alone.** Lifecycle rules and other
  bucket-wide settings then can't touch the database by accident.
- **Turn versioning off.** Leave it off for the bucket's lifetime: on S3 it
  can be suspended but never turned off again.
- **Turn soft delete off,** or keep it as short as the store allows.
- **Don't add retention policies, object locks, legal holds or default
  holds.** They stop IsleDB deleting data it no longer needs, or stop the
  writer saving at all.
- **Don't add lifecycle rules that delete or move objects under the
  database's prefix.** A rule that deletes old objects deletes live data. A
  rule that moves objects to a colder class makes reads slow.
  One rule is safe, and recommended on S3: abort incomplete multipart uploads
  after a day.
- **Use the standard storage class** (Standard on S3 and GCS, Hot on Azure).
  Infrequent-access and archive classes charge minimum storage durations and
  retrieval fees, which IsleDB triggers constantly.
- **Put the bucket in the same region as the writer and readers.** Every
  save waits for a round trip to storage.
- **Don't use bucket replication as a failover copy.** Replication copies
  objects one by one, in no particular order, so a replica can be missing
  data that its `manifest/CURRENT` refers to. Use it for backups you restore
  and check, not as a database you switch to.
- **Don't turn on requester pays.**

## Amazon S3

**Bucket URL:**

```
s3://my-isledb?region=us-west-2
```

**Create the bucket.** Versioning is off on a new bucket.

```bash
aws s3api create-bucket --bucket my-isledb --region us-west-2 \
  --create-bucket-configuration LocationConstraint=us-west-2

# Clean up uploads left by a crash.
aws s3api put-bucket-lifecycle-configuration --bucket my-isledb \
  --lifecycle-configuration '{"Rules":[{"ID":"abort-incomplete-uploads","Status":"Enabled","Filter":{},"AbortIncompleteMultipartUpload":{"DaysAfterInitiation":1}}]}'
```

**Permissions.** For a database at the prefix `prod/orders`:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
      "Resource": "arn:aws:s3:::my-isledb/prod/orders/*"
    },
    {
      "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::my-isledb",
      "Condition": {"StringLike": {"s3:prefix": "prod/orders/*"}}
    }
  ]
}
```

Readers need only `s3:GetObject` and `s3:ListBucket`.

**Encryption.** The default (SSE-S3) works. With SSE-KMS, turn on S3 Bucket
Keys: without them every read and write is also a KMS request, which counts
against your KMS quota.

**Versioning, if you must have it.** Add a lifecycle rule that deletes
noncurrent versions after a day, and one that removes expired delete markers.
Expect to keep a day of copies of `manifest/CURRENT`. Without the rule,
that object gains a version on every save, and S3 slows down the whole
bucket once an object has hundreds of thousands of versions.

**Request limits.** S3 handles at least 3,500 writes and 5,500 reads per
second per prefix, and answers `503 SlowDown` while it scales up; IsleDB
retries those.

## Google Cloud Storage

**Bucket URL:**

```
gs://my-isledb
```

**Create the bucket.** Versioning is off on a new bucket, but soft delete is
on, for 7 days. It keeps a copy of every overwritten object, so turn it off.

```bash
gcloud storage buckets create gs://my-isledb --location=us-central1 \
  --default-storage-class=STANDARD --uniform-bucket-level-access
gcloud storage buckets update gs://my-isledb --clear-soft-delete
```

Leave hierarchical namespace off (the default).

**Permissions.** Grant the writer `roles/storage.objectUser` on the bucket.
Readers need only `roles/storage.objectViewer`.

**One save per second.** Cloud Storage allows about one write per second to
an object, so the writer saves at most once per second there.
`flush.interval` defaults to 1s on `gs://` buckets and can't be set lower;
see [isledb-writer.md](isledb-writer.md#choose-the-flush-interval).

**Request limits.** A new bucket handles about 1,000 writes and 5,000 reads
per second at first, and scales up over minutes as traffic grows.

## Azure Blob Storage

**Bucket URL** (the container `isledb` in the storage account `myisledb`):

```
azblob://isledb?storage_account=myisledb
```

**Create the account and container.** Use a standard general-purpose v2
account with hierarchical namespace off. Accounts created in the Azure portal
have blob soft delete on by default; it keeps a copy of every overwritten
blob, so turn it off.

```bash
az storage account create --name myisledb --resource-group my-rg \
  --location westus2 --sku Standard_ZRS --kind StorageV2 \
  --hns false --access-tier Hot
az storage account blob-service-properties update \
  --account-name myisledb --resource-group my-rg \
  --enable-delete-retention false --enable-versioning false
az storage container create --account-name myisledb --name isledb --auth-mode login
```

Keeping container soft delete on is fine: it protects against deleting the
whole container and doesn't affect the database's objects.

**Permissions.** Grant the writer the *Storage Blob Data Contributor* role on
the container. Readers need only *Storage Blob Data Reader*.

**Also avoid:**

- Leases on IsleDB's blobs: a leased `manifest/CURRENT` stops every save.
- Immutability policies and object replication on the container.
- Cool, Cold and Archive tiers.

**Premium block blob accounts** give lower, steadier latency for the writer's
saves. They have no geo-redundancy.

**Geo-redundant accounts (GRS, GZRS).** After an unplanned failover, the
secondary region may be missing recent writes, and its objects may not match
each other. Check the database before using it again.

## S3-compatible stores

IsleDB uses the S3 API for these. Point it at the store's endpoint:

```
s3://my-isledb?endpoint=https://s3.example.com&region=us-east-1&use_path_style=true&request_checksum_calculation=when_required&response_checksum_validation=when_required
```

- **`endpoint`** is the store's S3 endpoint.
- **`use_path_style=true`** is needed by most self-hosted stores.
- **`request_checksum_calculation=when_required` and
  `response_checksum_validation=when_required`** turn off checksums that
  recent S3 clients send by default and that many S3-compatible stores
  reject.

### MinIO

```
s3://my-isledb?endpoint=http://minio:9000&region=us-east-1&use_path_style=true&request_checksum_calculation=when_required&response_checksum_validation=when_required
```

Keep versioning off on the bucket. The open-source MinIO repository is no
longer maintained; use MinIO AIStor, or pin a release you have tested.

### Tigris

```
s3://my-isledb?endpoint=<your Tigris S3 endpoint>&region=auto&request_checksum_calculation=when_required&response_checksum_validation=when_required
```

Create a single-region or multi-region bucket. Global and dual-region buckets
are only eventually consistent between regions, which IsleDB doesn't
support.

### Cloudflare R2

```
s3://my-isledb?endpoint=https://<account-id>.r2.cloudflarestorage.com&region=auto&request_checksum_calculation=when_required&response_checksum_validation=when_required
```

Readers work. The writer isn't recommended on R2 yet: R2 allows about one
write per second to an object and answers faster writes with `429`, and the
writer can save more often than that, even with `flush.interval: 1s`.

Don't read through a custom domain with caching turned on: cached reads can
be stale.

### Ceph Object Gateway

Not recommended yet. Current releases can answer a losing conditional write
with a server error, or report success for a write that didn't happen. If you
run Ceph, use the latest release and test it as below before trusting it
with data.

## Check a store yourself

Before using a store this guide marks as untested, check that it honours
conditional writes. With the AWS CLI:

```bash
B=my-isledb; E=--endpoint-url=https://s3.example.com
echo one > /tmp/one; echo two > /tmp/two

# 1. Creating an object that exists must fail with 412.
aws s3api put-object $E --bucket $B --key probe --body /tmp/one
aws s3api put-object $E --bucket $B --key probe --body /tmp/two --if-none-match '*'

# 2. Overwriting with a stale ETag must fail with 412.
aws s3api put-object $E --bucket $B --key probe --body /tmp/two --if-match '"0000"'

# 3. Overwriting with the current ETag must succeed.
ETAG=$(aws s3api head-object $E --bucket $B --key probe --query ETag --output text)
aws s3api put-object $E --bucket $B --key probe --body /tmp/two --if-match "$ETAG"

aws s3api delete-object $E --bucket $B --key probe
```

If step 1 or 2 succeeds, the store ignores conditional writes and can't be
used for IsleDB.
