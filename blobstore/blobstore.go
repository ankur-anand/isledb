package blobstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	azblobblob "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"gocloud.dev/blob"
	"gocloud.dev/gcerrors"
	"google.golang.org/api/googleapi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	ErrNotFound           = errors.New("object not found")
	ErrPreconditionFailed = errors.New("precondition failed")
	ErrBucketNameRequired = errors.New("bucket name required for cloud providers")
)

const (
	SSTBucketBits   = 12
	SSTBucketHexLen = 3
	SSTBucketCount  = 1 << SSTBucketBits
)

const sstBucketMask uint32 = SSTBucketCount - 1

var sstBucketTable = crc32.MakeTable(crc32.Castagnoli)

type BatchDeleteError struct {
	Failed map[string]error
}

func (e *BatchDeleteError) Error() string {
	if e == nil || len(e.Failed) == 0 {
		return "batch delete failed"
	}
	keys := make([]string, 0, len(e.Failed))
	for key := range e.Failed {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	first := keys[0]
	return fmt.Sprintf("batch delete failed for %d key(s), first key %q: %v", len(keys), first, e.Failed[first])
}

type Store struct {
	bucket     *blob.Bucket
	bucketName string
	prefix     string
	owns       bool
}

func Open(ctx context.Context, bucketURL, prefix string) (*Store, error) {

	parsed, err := url.Parse(bucketURL)
	if err != nil {
		return nil, fmt.Errorf("parse bucket %q: %w", bucketURL, err)
	}
	bucketName := parsed.Host
	switch parsed.Scheme {
	case "s3", "gs", "azblob":
		if bucketName == "" {
			return nil, fmt.Errorf("%w %q ", ErrBucketNameRequired, bucketURL)
		}
	}

	bkt, err := blob.OpenBucket(ctx, bucketURL)
	if err != nil {
		return nil, fmt.Errorf("open bucket %q: %w", bucketURL, err)
	}
	return &Store{
		bucket:     bkt,
		bucketName: bucketName,
		prefix:     strings.TrimSuffix(prefix, "/"),
		owns:       true,
	}, nil
}

// New wraps an existing bucket. For cloud providers, bucketName is required
// for CAS writes; use Open() when possible.
func New(bkt *blob.Bucket, bucketName, prefix string) *Store {
	return &Store{
		bucket:     bkt,
		bucketName: bucketName,
		prefix:     strings.TrimSuffix(prefix, "/"),
		owns:       false,
	}
}

func (s *Store) Close() error {
	if s.owns && s.bucket != nil {
		return s.bucket.Close()
	}
	return nil
}

func (s *Store) Bucket() *blob.Bucket {
	return s.bucket
}

func (s *Store) Prefix() string {
	return s.prefix
}

func (s *Store) path(parts ...string) string {
	if s.prefix == "" {
		return path.Join(parts...)
	}
	return path.Join(append([]string{s.prefix}, parts...)...)
}

func (s *Store) SSTPath(id string) string {
	return s.path("sstable", SSTBucket(id), id)
}

func (s *Store) ChangeBatchPath(id string) string {
	return s.path("changes", ChangeBatchBucket(id), id)
}

// SSTBucket returns the deterministic object-store bucket for an SST ID.
// The bucket is part of the object layout; changing it changes every SST key.
func SSTBucket(id string) string {
	sum := crc32.Checksum([]byte(id), sstBucketTable)
	return fmt.Sprintf("%0*x", SSTBucketHexLen, sum&sstBucketMask)
}

// ChangeBatchBucket returns the deterministic object-store bucket for a change
// batch ID. It intentionally uses the same CRC32C layout as SSTBucket.
func ChangeBatchBucket(id string) string {
	return SSTBucket(id)
}

func (s *Store) ManifestPath() string {
	return s.path("manifest", "CURRENT")
}

func (s *Store) MaintenanceHeadPath() string {
	return s.path("maintenance", "HEAD")
}

func (s *Store) ManifestSnapshotPath(id string) string {
	return s.path("manifest", "snapshots", id+".manifest.zst")
}

func (s *Store) ManifestPagePath(level uint8, id string) string {
	return s.path("manifest", "pages", fmt.Sprintf("l%02d", level), id+".page.zst")
}

type Attributes struct {
	Size    int64
	ETag    string
	ModTime time.Time
	// Generation is used/set for GCS. GCS Doesn't use Etag.
	Generation int64
}

func (s *Store) Read(ctx context.Context, key string) (data []byte, attrs Attributes, err error) {
	r, err := s.bucket.NewReader(ctx, key, nil)
	if err != nil {
		return nil, Attributes{}, s.mapError(err)
	}
	defer func() {
		if closeErr := r.Close(); closeErr != nil {
			data = nil
			attrs = Attributes{}
			err = errors.Join(err, closeErr)
		}
	}()

	data, err = io.ReadAll(r)
	if err != nil {
		return nil, Attributes{}, err
	}

	attrs = Attributes{
		Size:    r.Size(),
		ModTime: r.ModTime(),
	}

	s.extractReaderAttrs(r, &attrs)

	return data, attrs, nil
}

// extractReaderAttrs extracts ETag/generation from the underlying provider-specific reader.
// https://gocloud.dev/concepts/as/
// https://gocloud.dev/howto/blob/
func (s *Store) extractReaderAttrs(r *blob.Reader, attrs *Attributes) {

	// https://pkg.go.dev/github.com/google/go-cloud/blob/s3blob#hdr-As
	// Reader: s3.GetObjectOutput
	var s3Output s3.GetObjectOutput
	if r.As(&s3Output) && s3Output.ETag != nil {
		attrs.ETag = *s3Output.ETag
		return
	}

	// https://pkg.go.dev/gocloud.dev/blob/gcsblob#hdr-As
	// Reader: *storage.Reader (use Reader.Attrs for Generation)
	var gcsReader *storage.Reader
	if r.As(&gcsReader) && gcsReader != nil {
		attrs.Generation = gcsReader.Attrs.Generation
		return
	}

	// https://pkg.go.dev/gocloud.dev/blob/azureblob#hdr-As
	var azureResp azblobblob.DownloadStreamResponse
	if r.As(&azureResp) && azureResp.ETag != nil {
		attrs.ETag = string(*azureResp.ETag)
		return
	}
}

func (s *Store) ReadRange(ctx context.Context, key string, offset, length int64) (data []byte, err error) {
	r, err := s.bucket.NewRangeReader(ctx, key, offset, length, nil)
	if err != nil {
		return nil, s.mapError(err)
	}
	defer func() {
		if closeErr := r.Close(); closeErr != nil {
			data = nil
			err = errors.Join(err, closeErr)
		}
	}()

	if length < 0 {
		return io.ReadAll(r)
	}
	if uint64(length) > uint64(^uint(0)>>1) {
		return nil, fmt.Errorf("range length too large: %d", length)
	}
	data = make([]byte, int(length))
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, err
	}
	return data, nil
}

func (s *Store) ReadStream(ctx context.Context, key string) (*blob.Reader, error) {
	r, err := s.bucket.NewReader(ctx, key, nil)
	if err != nil {
		return nil, s.mapError(err)
	}
	return r, nil
}

func (s *Store) ReadRangeStream(ctx context.Context, key string, offset, length int64) (*blob.Reader, error) {
	r, err := s.bucket.NewRangeReader(ctx, key, offset, length, nil)
	if err != nil {
		return nil, s.mapError(err)
	}
	return r, nil
}

func (s *Store) Attributes(ctx context.Context, key string) (Attributes, error) {
	attr, err := s.bucket.Attributes(ctx, key)
	if err != nil {
		return Attributes{}, s.mapError(err)
	}
	gen := generationFromAttrs(attr)
	return Attributes{
		Size:       attr.Size,
		ETag:       s.stableETag(attr),
		ModTime:    attr.ModTime,
		Generation: gen,
	}, nil
}

// stableETag returns a content-based ETag when the underlying provider
// (e.g. fileblob) computes ETags from file metadata rather than content.
// For such providers, we derive the ETag from the stored MD5 hash instead.
func (s *Store) stableETag(attr *blob.Attributes) string {
	if s.providerKind() != providerUnknown {
		return attr.ETag
	}
	if len(attr.MD5) > 0 {
		return fmt.Sprintf("%x", attr.MD5)
	}
	return attr.ETag
}

func (s *Store) Exists(ctx context.Context, key string) (bool, error) {
	exists, err := s.bucket.Exists(ctx, key)
	if err != nil {
		return false, err
	}
	return exists, nil
}

// HasImmutableDatabaseObjects reports whether the database prefix contains
// durable data or manifest-history objects that cannot exist before CURRENT
// is initialized. Control records such as maintenance/HEAD and manifest/gc
// are intentionally excluded because they may be staged before the first
// writer claims its fence.
func (s *Store) HasImmutableDatabaseObjects(ctx context.Context) (bool, error) {
	for _, relativePrefix := range []string{
		"sstable",
		"changes",
		"manifest/pages",
		"manifest/snapshots",
	} {
		prefix := s.path(relativePrefix) + "/"
		iter := s.bucket.List(&blob.ListOptions{Prefix: prefix})
		_, err := iter.Next(ctx)
		if err == nil {
			return true, nil
		}
		if !errors.Is(err, io.EOF) {
			return false, s.mapError(err)
		}
	}
	return false, nil
}

func (s *Store) Write(ctx context.Context, key string, data []byte) (Attributes, error) {
	if _, err := s.WriteReader(ctx, key, bytes.NewReader(data), nil); err != nil {
		return Attributes{}, err
	}
	attr, err := s.bucket.Attributes(ctx, key)
	if err != nil {
		return Attributes{}, err
	}
	gen := generationFromAttrs(attr)
	return Attributes{
		Size:       attr.Size,
		ETag:       s.stableETag(attr),
		Generation: gen,
	}, nil
}

func (s *Store) WriteReader(ctx context.Context, key string, r io.Reader, opts *blob.WriterOptions) (Attributes, error) {
	if opts == nil {
		opts = &blob.WriterOptions{
			ContentType: "application/octet-stream",
		}
	}

	w, err := s.bucket.NewWriter(ctx, key, opts)
	if err != nil {
		return Attributes{}, s.mapError(err)
	}

	written, err := io.Copy(w, r)
	if err != nil {
		_ = w.Close()
		return Attributes{}, err
	}

	if err := w.Close(); err != nil {
		return Attributes{}, s.mapError(err)
	}
	return Attributes{
		Size: written,
	}, nil
}

func (s *Store) WriteIfMatch(ctx context.Context, key string, data []byte, ifMatch string) (Attributes, error) {
	if ifMatch == "" {
		return s.WriteIfNotExist(ctx, key, data)
	}
	return s.writeWithCAS(ctx, key, data, ifMatch)
}

func (s *Store) WriteIfNotExist(ctx context.Context, key string, data []byte) (Attributes, error) {
	return s.writeIfNotExistWithETag(ctx, key, data)
}

func (s *Store) writeIfNotExist(ctx context.Context, key string, data []byte) (Attributes, error) {
	opts := &blob.WriterOptions{
		ContentType: "application/octet-stream",
		IfNotExist:  true,
	}
	return s.WriteReader(ctx, key, bytes.NewReader(data), opts)
}

func (s *Store) writeIfMatchFallback(ctx context.Context, key string, data []byte, ifMatch string) (Attributes, error) {
	currentAttr, err := s.bucket.Attributes(ctx, key)
	objectExists := err == nil
	if err != nil && gcerrors.Code(err) != gcerrors.NotFound {
		return Attributes{}, err
	}
	if !objectExists {
		return Attributes{}, ErrPreconditionFailed
	}
	if s.stableETag(currentAttr) != ifMatch {
		return Attributes{}, ErrPreconditionFailed
	}
	return s.Write(ctx, key, data)
}

func generationFromAttrs(attr *blob.Attributes) int64 {
	if attr == nil {
		return 0
	}
	var gcsAttrs storage.ObjectAttrs
	if attr.As(&gcsAttrs) {
		return gcsAttrs.Generation
	}
	return 0
}

type providerKind int

const (
	providerUnknown providerKind = iota
	providerS3
	providerGCS
	providerAzure
)

func (s *Store) providerKind() providerKind {
	var s3Client *s3.Client
	if s.bucket.As(&s3Client) {
		return providerS3
	}
	var gcsClient *storage.Client
	if s.bucket.As(&gcsClient) {
		return providerGCS
	}
	var azureClient *container.Client
	if s.bucket.As(&azureClient) {
		return providerAzure
	}
	return providerUnknown
}

func parseGeneration(ifMatch string) (int64, error) {
	gen, err := strconv.ParseInt(ifMatch, 10, 64)
	if err != nil {
		return 0, err
	}
	if gen <= 0 {
		return 0, fmt.Errorf("invalid generation %d", gen)
	}
	return gen, nil
}

func (s *Store) Delete(ctx context.Context, key string) error {
	err := s.bucket.Delete(ctx, key)
	if err != nil && gcerrors.Code(err) == gcerrors.NotFound {
		return nil
	}
	return err
}

func (s *Store) BatchDelete(ctx context.Context, keys []string) error {
	uniqueKeys := uniqueNonEmptyKeys(keys)
	if len(uniqueKeys) == 0 {
		return nil
	}
	return s.batchDeleteFallback(ctx, uniqueKeys)
}

func (s *Store) batchDeleteFallback(ctx context.Context, keys []string) error {
	failed := make(map[string]error)
	for _, key := range keys {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := s.Delete(ctx, key); err != nil {
			failed[key] = err
			if ctxErr := ctx.Err(); ctxErr != nil {
				return errors.Join(ctxErr, &BatchDeleteError{Failed: failed})
			}
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return errors.Join(err, &BatchDeleteError{Failed: failed})
			}
		}
	}
	if len(failed) == 0 {
		return nil
	}
	return &BatchDeleteError{Failed: failed}
}

func uniqueNonEmptyKeys(keys []string) []string {
	if len(keys) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(keys))
	out := make([]string, 0, len(keys))
	for _, key := range keys {
		if key == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, key)
	}
	return out
}

type ListOptions struct {
	Prefix    string
	Delimiter string
}

type ListResult struct {
	Objects []ObjectInfo
}

type ObjectInfo struct {
	Key   string
	Size  int64
	IsDir bool
}

// ListIterator retains the provider continuation state for a bounded,
// incremental object scan. It is not safe for concurrent use.
type ListIterator struct {
	iter *blob.ListIterator
}

// NewListIterator starts an incremental scan. The returned iterator keeps the
// provider cursor in memory, so callers can consume bounded pages without
// restarting the listing from the prefix on every pass.
func (s *Store) NewListIterator(opts ListOptions) *ListIterator {
	prefix := s.prefix
	if opts.Prefix != "" {
		prefix = s.path(opts.Prefix)
	}
	return &ListIterator{iter: s.bucket.List(&blob.ListOptions{
		Prefix:    prefix,
		Delimiter: opts.Delimiter,
	})}
}

// Next returns the next object in provider listing order. It returns io.EOF
// after the current scan is exhausted.
func (it *ListIterator) Next(ctx context.Context) (ObjectInfo, error) {
	if it == nil || it.iter == nil {
		return ObjectInfo{}, io.EOF
	}
	object, err := it.iter.Next(ctx)
	if err != nil {
		return ObjectInfo{}, err
	}
	return ObjectInfo{Key: object.Key, Size: object.Size, IsDir: object.IsDir}, nil
}

func (s *Store) List(ctx context.Context, opts ListOptions) (*ListResult, error) {
	var result ListResult
	if err := s.Walk(ctx, opts, func(object ObjectInfo) (bool, error) {
		result.Objects = append(result.Objects, object)
		return true, nil
	}); err != nil {
		return nil, err
	}
	return &result, nil
}

// ListPage returns at most pageSize objects and an opaque continuation token.
// Pass nil as pageToken for the first page. A nil token in the result means
// the listing is complete and must not be passed back as another page.
func (s *Store) ListPage(
	ctx context.Context,
	pageToken []byte,
	pageSize int,
	opts ListOptions,
) (*ListResult, []byte, error) {
	if pageToken == nil {
		pageToken = blob.FirstPageToken
	}
	prefix := s.prefix
	if opts.Prefix != "" {
		prefix = s.path(opts.Prefix)
	}
	objects, nextPageToken, err := s.bucket.ListPage(ctx, pageToken, pageSize, &blob.ListOptions{
		Prefix:    prefix,
		Delimiter: opts.Delimiter,
	})
	if err != nil {
		return nil, nil, s.mapError(err)
	}
	result := &ListResult{Objects: make([]ObjectInfo, len(objects))}
	for i, object := range objects {
		result.Objects[i] = ObjectInfo{Key: object.Key, Size: object.Size, IsDir: object.IsDir}
	}
	if len(nextPageToken) == 0 {
		return result, nil, nil
	}
	return result, bytes.Clone(nextPageToken), nil
}

// Walk streams objects in provider listing order. Returning false from visit
// stops the listing without materializing the remainder of the prefix.
func (s *Store) Walk(ctx context.Context, opts ListOptions, visit func(ObjectInfo) (bool, error)) error {
	if visit == nil {
		return errors.New("nil object visitor")
	}
	iter := s.NewListIterator(opts)
	for {
		obj, err := iter.Next(ctx)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		keepGoing, err := visit(obj)
		if err != nil {
			return err
		}
		if !keepGoing {
			return nil
		}
	}
}

func (s *Store) ListSSTFiles(ctx context.Context) ([]ObjectInfo, error) {
	result, err := s.List(ctx, ListOptions{Prefix: "sstable/"})
	if err != nil {
		return nil, err
	}
	return result.Objects, nil
}

func (s *Store) mapError(err error) error {
	if err == nil {
		return nil
	}

	switch gcerrors.Code(err) {
	case gcerrors.NotFound:
		return ErrNotFound
	case gcerrors.FailedPrecondition:
		return ErrPreconditionFailed
	}

	// https://docs.aws.amazon.com/sdk-for-go/v2/developer-guide/handle-errors.html
	// From the Above Doc: All service API response errors implement the smithy.APIError interface type.
	// This interface can be used to handle both modeled or un-modeled service error responses.
	// https://pkg.go.dev/github.com/aws/smithy-go#APIError
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "NotFound", "NoSuchKey":
			return ErrNotFound
		case "PreconditionFailed", "ConditionalRequestConflict":
			return ErrPreconditionFailed
		}
	}

	// if some proxy doesn't respect the above.
	var smithyResp *smithyhttp.ResponseError
	if errors.As(err, &smithyResp) {
		switch smithyResp.HTTPStatusCode() {
		case http.StatusNotFound:
			return ErrNotFound
		case http.StatusPreconditionFailed:
			return ErrPreconditionFailed
		}
	}

	var azRespErr *azcore.ResponseError
	if errors.As(err, &azRespErr) {
		switch azRespErr.StatusCode {
		case http.StatusNotFound:
			return ErrNotFound
		case http.StatusConflict:
			// Azure returns 409 for BlobAlreadyExists on If-None-Match writes.
			return ErrPreconditionFailed
		case http.StatusPreconditionFailed:
			return ErrPreconditionFailed
		}
	}

	var gcsErr *googleapi.Error
	if errors.As(err, &gcsErr) {
		switch gcsErr.Code {
		case http.StatusNotFound:
			return ErrNotFound
		case http.StatusPreconditionFailed:
			return ErrPreconditionFailed
		}
	}

	// if grpc transport is used for gcs.
	if st, ok := status.FromError(err); ok {
		switch st.Code() {
		case codes.NotFound:
			return ErrNotFound
		case codes.FailedPrecondition:
			return ErrPreconditionFailed
		}
	}

	return err
}

func (s *Store) DebugString() string {
	ctx := context.Background()
	result, err := s.List(ctx, ListOptions{})
	if err != nil {
		return fmt.Sprintf("error listing: %v", err)
	}

	var sb strings.Builder
	sb.WriteString("Objects:\n")
	for _, obj := range result.Objects {
		if obj.IsDir {
			fmt.Fprintf(&sb, "  [dir] %s\n", obj.Key)
		} else {
			fmt.Fprintf(&sb, "  %s (%d bytes)\n", obj.Key, obj.Size)
		}
	}
	return sb.String()
}
