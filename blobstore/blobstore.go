package blobstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	azblobblob "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"gocloud.dev/blob"
	"gocloud.dev/gcerrors"
)

var (
	ErrNotFound           = errors.New("object not found")
	ErrPreconditionFailed = errors.New("precondition failed")
)

type Store struct {
	bucket *blob.Bucket
	prefix string
	owns   bool
}

func Open(ctx context.Context, bucketURL, prefix string) (*Store, error) {
	bkt, err := blob.OpenBucket(ctx, bucketURL)
	if err != nil {
		return nil, fmt.Errorf("open bucket %q: %w", bucketURL, err)
	}
	return &Store{
		bucket: bkt,
		prefix: strings.TrimSuffix(prefix, "/"),
		owns:   true,
	}, nil
}

func New(bkt *blob.Bucket, prefix string) *Store {
	return &Store{
		bucket: bkt,
		prefix: strings.TrimSuffix(prefix, "/"),
		owns:   false,
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
	return s.path("sstable", id)
}

func (s *Store) VLogPath(id string) string {
	return s.path("vlogs", id+".vlog")
}

func (s *Store) BlobPath(blobID string) string {
	if len(blobID) < 2 {
		return s.path("blobs", blobID+".blob")
	}
	return s.path("blobs", blobID[:2], blobID+".blob")
}

func (s *Store) ListBlobFiles(ctx context.Context) ([]ObjectInfo, error) {
	result, err := s.List(ctx, ListOptions{Prefix: "blobs/"})
	if err != nil {
		return nil, err
	}
	return result.Objects, nil
}

func (s *Store) ManifestPath() string {
	return s.path("manifest", "CURRENT")
}

func (s *Store) ManifestLogPath(id string) string {
	return s.path("manifest", "log", id+".json")
}

func (s *Store) ManifestSnapshotPath(id string) string {
	return s.path("manifest", "snapshots", id+".manifest")
}

type Attributes struct {
	Size    int64
	ETag    string
	ModTime time.Time
	// Generation is used/set for GCS. GCS Doesn't use Etag.
	Generation int64
}

func (s *Store) Read(ctx context.Context, key string) ([]byte, Attributes, error) {
	r, err := s.bucket.NewReader(ctx, key, nil)
	if err != nil {
		return nil, Attributes{}, s.mapError(err)
	}
	defer r.Close()

	data, err := io.ReadAll(r)
	if err != nil {
		return nil, Attributes{}, err
	}

	return data, Attributes{
		Size: int64(len(data)),
	}, nil
}

func (s *Store) ReadRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	r, err := s.bucket.NewRangeReader(ctx, key, offset, length, nil)
	if err != nil {
		return nil, s.mapError(err)
	}
	defer r.Close()

	data, err := io.ReadAll(r)
	if err != nil {
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
		ETag:       attr.ETag,
		ModTime:    attr.ModTime,
		Generation: gen,
	}, nil
}

func (s *Store) Exists(ctx context.Context, key string) (bool, error) {
	exists, err := s.bucket.Exists(ctx, key)
	if err != nil {
		return false, err
	}
	return exists, nil
}

func (s *Store) Write(ctx context.Context, key string, data []byte) (Attributes, error) {
	return s.WriteReader(ctx, key, bytes.NewReader(data), nil)
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

	if _, err := io.Copy(w, r); err != nil {
		_ = w.Close()
		return Attributes{}, err
	}

	if err := w.Close(); err != nil {
		return Attributes{}, s.mapError(err)
	}

	attr, err := s.bucket.Attributes(ctx, key)
	if err != nil {
		return Attributes{}, err
	}
	gen := generationFromAttrs(attr)

	return Attributes{
		Size:       attr.Size,
		ETag:       attr.ETag,
		Generation: gen,
	}, nil
}

func (s *Store) WriteIfMatch(ctx context.Context, key string, data []byte, ifMatch string) (Attributes, error) {
	if ifMatch == "" {
		return s.writeIfNotExist(ctx, key, data)
	}

	if attr, ok, err := s.writeIfMatchAtomic(ctx, key, data, ifMatch); ok || err != nil {
		return attr, err
	}

	return s.writeIfMatchFallback(ctx, key, data, ifMatch)
}

func (s *Store) WriteIfNotExist(ctx context.Context, key string, data []byte) error {
	w, err := s.bucket.NewWriter(ctx, key, &blob.WriterOptions{
		ContentType: "application/octet-stream",
		IfNotExist:  true,
	})
	if err != nil {
		return s.mapError(err)
	}

	if _, err := w.Write(data); err != nil {
		w.Close()
		return err
	}

	return s.mapError(w.Close())
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
	if currentAttr.ETag != ifMatch {
		return Attributes{}, ErrPreconditionFailed
	}
	return s.Write(ctx, key, data)
}

func generationFromAttrs(attr *blob.Attributes) int64 {
	if attr == nil {
		return 0
	}
	var gcsAttrs *storage.ObjectAttrs
	if attr.As(&gcsAttrs) && gcsAttrs != nil {
		return gcsAttrs.Generation
	}
	return 0
}

func (s *Store) writeIfMatchAtomic(ctx context.Context, key string, data []byte, ifMatch string) (Attributes, bool, error) {
	switch s.providerKind() {
	case providerS3:
		return s.writeIfMatchS3(ctx, key, data, ifMatch)
	case providerAzure:
		return s.writeIfMatchAzure(ctx, key, data, ifMatch)
	case providerGCS:
		gen, err := parseGeneration(ifMatch)
		if err != nil {
			return Attributes{}, true, fmt.Errorf("gcs requires generation match token: %w", err)
		}
		return s.writeIfMatchGCS(ctx, key, data, gen)
	default:
		return Attributes{}, false, nil
	}
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

func (s *Store) writeIfMatchS3(ctx context.Context, key string, data []byte, ifMatch string) (Attributes, bool, error) {
	opts := &blob.WriterOptions{
		ContentType: "application/octet-stream",
		BeforeWrite: func(asFunc func(any) bool) error {
			var input *s3.PutObjectInput
			if asFunc(&input) && input != nil {
				input.IfMatch = aws.String(ifMatch)
			}
			return nil
		},
	}
	attr, err := s.WriteReader(ctx, key, bytes.NewReader(data), opts)
	if err != nil {
		return Attributes{}, true, err
	}
	return attr, true, nil
}

func (s *Store) writeIfMatchAzure(ctx context.Context, key string, data []byte, ifMatch string) (Attributes, bool, error) {
	opts := &blob.WriterOptions{
		ContentType: "application/octet-stream",
		BeforeWrite: func(asFunc func(any) bool) error {
			var uploadOpts *azblob.UploadStreamOptions
			if asFunc(&uploadOpts) && uploadOpts != nil {
				if uploadOpts.AccessConditions == nil {
					uploadOpts.AccessConditions = &azblob.AccessConditions{}
				}
				if uploadOpts.AccessConditions.ModifiedAccessConditions == nil {
					uploadOpts.AccessConditions.ModifiedAccessConditions = &azblobblob.ModifiedAccessConditions{}
				}
				etag := azcore.ETag(ifMatch)
				uploadOpts.AccessConditions.ModifiedAccessConditions.IfMatch = &etag
			}
			return nil
		},
	}
	attr, err := s.WriteReader(ctx, key, bytes.NewReader(data), opts)
	if err != nil {
		return Attributes{}, true, err
	}
	return attr, true, nil
}

func (s *Store) writeIfMatchGCS(ctx context.Context, key string, data []byte, generation int64) (Attributes, bool, error) {
	opts := &blob.WriterOptions{
		ContentType: "application/octet-stream",
		BeforeWrite: func(asFunc func(any) bool) error {
			var obj **storage.ObjectHandle
			if asFunc(&obj) && obj != nil && *obj != nil {
				*obj = (*obj).If(storage.Conditions{GenerationMatch: generation})
			}
			return nil
		},
	}
	attr, err := s.WriteReader(ctx, key, bytes.NewReader(data), opts)
	if err != nil {
		return Attributes{}, true, err
	}
	return attr, true, nil
}

func (s *Store) Delete(ctx context.Context, key string) error {
	err := s.bucket.Delete(ctx, key)
	if err != nil && gcerrors.Code(err) == gcerrors.NotFound {
		return nil
	}
	return err
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

func (s *Store) List(ctx context.Context, opts ListOptions) (*ListResult, error) {
	prefix := s.prefix
	if opts.Prefix != "" {
		prefix = s.path(opts.Prefix)
	}

	iter := s.bucket.List(&blob.ListOptions{
		Prefix:    prefix,
		Delimiter: opts.Delimiter,
	})

	var result ListResult
	for {
		obj, err := iter.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		result.Objects = append(result.Objects, ObjectInfo{
			Key:   obj.Key,
			Size:  obj.Size,
			IsDir: obj.IsDir,
		})
	}

	return &result, nil
}

func (s *Store) ListSSTFiles(ctx context.Context) ([]ObjectInfo, error) {
	result, err := s.List(ctx, ListOptions{Prefix: "sstable/"})
	if err != nil {
		return nil, err
	}
	return result.Objects, nil
}

func (s *Store) ListVLogFiles(ctx context.Context) ([]ObjectInfo, error) {
	result, err := s.List(ctx, ListOptions{Prefix: "vlogs/"})
	if err != nil {
		return nil, err
	}
	return result.Objects, nil
}

func (s *Store) ListManifestLogs(ctx context.Context) ([]ObjectInfo, error) {
	result, err := s.List(ctx, ListOptions{Prefix: "manifest/log/"})
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
	default:
		return err
	}
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
			sb.WriteString(fmt.Sprintf("  [dir] %s\n", obj.Key))
		} else {
			sb.WriteString(fmt.Sprintf("  %s (%d bytes)\n", obj.Key, obj.Size))
		}
	}
	return sb.String()
}
