package blobstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"

	"gocloud.dev/blob"
	"gocloud.dev/gcerrors"
)

var (
	ErrNotFound           = errors.New("object not found")
	ErrPreconditionFailed = errors.New("precondition failed")
)

// Store provides object storage operations for a tenant/prefix.
type Store struct {
	bucket *blob.Bucket
	prefix string
	owns   bool
}

// Open opens a store from a bucket URL and prefix.
// The URL format is driver-specific, e.g.:
//   - "mem://" for in-memory
//   - "file:///path/to/dir" for local filesystem
//   - "s3://bucket?region=us-east-1" for S3
//   - "gs://bucket" for GCS
//   - "azblob://container" for Azure
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

func (s *Store) ManifestPath() string {
	return s.path("manifest.json")
}

func (s *Store) ManifestLogPath(id string) string {
	return s.path("manifest", id+".json")
}

// Attributes contains object metadata.
type Attributes struct {
	Size int64
	ETag string
}

// Read downloads an object and returns its content and attributes.
func (s *Store) Read(ctx context.Context, key string) ([]byte, Attributes, error) {
	attr, err := s.bucket.Attributes(ctx, key)
	if err != nil {
		return nil, Attributes{}, s.mapError(err)
	}

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
		Size: attr.Size,
		ETag: attr.ETag,
	}, nil
}

// ReadStream returns a reader for an object.
func (s *Store) ReadStream(ctx context.Context, key string) (*blob.Reader, error) {
	r, err := s.bucket.NewReader(ctx, key, nil)
	if err != nil {
		return nil, s.mapError(err)
	}
	return r, nil
}

// Attributes fetches object metadata without downloading content.
func (s *Store) Attributes(ctx context.Context, key string) (Attributes, error) {
	attr, err := s.bucket.Attributes(ctx, key)
	if err != nil {
		return Attributes{}, s.mapError(err)
	}
	return Attributes{
		Size: attr.Size,
		ETag: attr.ETag,
	}, nil
}

// Exists checks if an object exists.
func (s *Store) Exists(ctx context.Context, key string) (bool, error) {
	exists, err := s.bucket.Exists(ctx, key)
	if err != nil {
		return false, err
	}
	return exists, nil
}

// Write uploads content to an object.
func (s *Store) Write(ctx context.Context, key string, data []byte) (Attributes, error) {
	return s.WriteReader(ctx, key, bytes.NewReader(data), nil)
}

// WriteReader uploads content from a reader.
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

	return Attributes{
		Size: attr.Size,
		ETag: attr.ETag,
	}, nil
}

// WriteIfMatch writes content only if the current ETag matches.
func (s *Store) WriteIfMatch(ctx context.Context, key string, data []byte, ifMatch string) (Attributes, error) {
	currentAttr, err := s.bucket.Attributes(ctx, key)
	objectExists := err == nil
	if err != nil && gcerrors.Code(err) != gcerrors.NotFound {
		return Attributes{}, err
	}

	if ifMatch == "" {
		if objectExists {
			return Attributes{}, ErrPreconditionFailed
		}
	} else {
		if !objectExists {
			return Attributes{}, ErrPreconditionFailed
		}
		if currentAttr.ETag != ifMatch {
			return Attributes{}, ErrPreconditionFailed
		}
	}

	return s.Write(ctx, key, data)
}

// Delete removes an object. Returns nil if object doesn't exist.
func (s *Store) Delete(ctx context.Context, key string) error {
	err := s.bucket.Delete(ctx, key)
	if err != nil && gcerrors.Code(err) == gcerrors.NotFound {
		return nil
	}
	return err
}

// ListOptions configures listing behavior.
type ListOptions struct {
	Prefix    string
	Delimiter string
}

// ListResult contains a list of objects.
type ListResult struct {
	Objects []ObjectInfo
}

// ObjectInfo contains information about a listed object.
type ObjectInfo struct {
	Key   string
	Size  int64
	IsDir bool
}

// List returns objects matching the options.
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

// ListSSTFiles returns all SST files in the sstable directory.
func (s *Store) ListSSTFiles(ctx context.Context) ([]ObjectInfo, error) {
	result, err := s.List(ctx, ListOptions{Prefix: "sstable/"})
	if err != nil {
		return nil, err
	}
	return result.Objects, nil
}

// ListVLogFiles returns all VLog files in the vlogs directory.
func (s *Store) ListVLogFiles(ctx context.Context) ([]ObjectInfo, error) {
	result, err := s.List(ctx, ListOptions{Prefix: "vlogs/"})
	if err != nil {
		return nil, err
	}
	return result.Objects, nil
}

// ListManifestLogs returns all manifest log files.
func (s *Store) ListManifestLogs(ctx context.Context) ([]ObjectInfo, error) {
	result, err := s.List(ctx, ListOptions{Prefix: "manifest/"})
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
