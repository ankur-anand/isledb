package manifest

import (
	"crypto/sha256"
	"fmt"
	"time"
)

func newManifestObjectRef(path string, encoded []byte, kind byte, createdAt time.Time) (ObjectRef, error) {
	if path == "" {
		return ObjectRef{}, fmt.Errorf("%w: immutable manifest object path is empty", ErrInvalidManifest)
	}
	maxRawBytes, err := manifestObjectRawLimit(kind)
	if err != nil {
		return ObjectRef{}, err
	}
	if _, err := manifestObjectRawBytes(encoded, kind, maxRawBytes); err != nil {
		return ObjectRef{}, err
	}
	sum := sha256.Sum256(encoded)
	return ObjectRef{
		Path:         path,
		EncodedBytes: uint64(len(encoded)),
		Checksum:     fmt.Sprintf("sha256:%x", sum[:]),
		CreatedAt:    createdAt,
	}, nil
}

func verifyManifestObjectRef(encoded []byte, ref ObjectRef, kind byte) error {
	if err := validateManifestObjectRef(ref, kind); err != nil {
		return err
	}
	if uint64(len(encoded)) != ref.EncodedBytes {
		return fmt.Errorf("%w: immutable manifest encoded bytes=%d want=%d path=%q", ErrInvalidManifest, len(encoded), ref.EncodedBytes, ref.Path)
	}
	sum := sha256.Sum256(encoded)
	checksum := fmt.Sprintf("sha256:%x", sum[:])
	if checksum != ref.Checksum {
		return fmt.Errorf("%w: immutable manifest checksum mismatch path=%q", ErrInvalidManifest, ref.Path)
	}
	maxRawBytes, err := manifestObjectRawLimit(kind)
	if err != nil {
		return err
	}
	if _, err := manifestObjectRawBytes(encoded, kind, maxRawBytes); err != nil {
		return err
	}
	return nil
}

func validateManifestObjectRef(ref ObjectRef, kind byte) error {
	if ref.Path == "" || ref.EncodedBytes <= manifestObjectHeaderBytes || ref.CreatedAt.IsZero() {
		return fmt.Errorf("%w: incomplete immutable manifest reference path=%q", ErrInvalidManifest, ref.Path)
	}
	maxRawBytes, err := manifestObjectRawLimit(kind)
	if err != nil {
		return err
	}
	const maxEncodedOverhead = 1 << 20
	if ref.EncodedBytes > maxRawBytes+manifestObjectHeaderBytes+maxEncodedOverhead {
		return fmt.Errorf("%w: immutable manifest encoded bytes=%d limit=%d path=%q",
			ErrInvalidManifest, ref.EncodedBytes, maxRawBytes+manifestObjectHeaderBytes+maxEncodedOverhead, ref.Path)
	}
	if len(ref.Checksum) != len("sha256:")+sha256.Size*2 || ref.Checksum[:len("sha256:")] != "sha256:" {
		return fmt.Errorf("%w: invalid immutable manifest checksum path=%q", ErrInvalidManifest, ref.Path)
	}
	return nil
}

func manifestObjectRawLimit(kind byte) (uint64, error) {
	switch kind {
	case manifestObjectKindSnapshot:
		return maxManifestSnapshotRawBytes, nil
	case manifestObjectKindPage:
		return maxManifestPageRawBytes, nil
	default:
		return 0, fmt.Errorf("%w: immutable manifest kind=%d", ErrInvalidManifest, kind)
	}
}

func (r *ObjectRef) Clone() *ObjectRef {
	if r == nil {
		return nil
	}
	clone := *r
	return &clone
}

func objectRefsEqual(a, b *ObjectRef) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}
