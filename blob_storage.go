package isledb

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/ankur-anand/isledb/blobstore"
)

type BlobStorage struct {
	store  *blobstore.Store
	config ValueStorageConfig
}

func NewBlobStorage(store *blobstore.Store, config ValueStorageConfig) *BlobStorage {
	return &BlobStorage{
		store:  store,
		config: config,
	}
}

func (b *BlobStorage) Write(ctx context.Context, value []byte) ([32]byte, error) {

	blobID := sha256.Sum256(value)
	blobIDHex := hex.EncodeToString(blobID[:])

	blobPath := b.store.BlobPath(blobIDHex)
	exists, err := b.store.Exists(ctx, blobPath)
	if err != nil {
		return [32]byte{}, fmt.Errorf("check blob existence: %w", err)
	}

	if exists {

		return blobID, nil
	}

	if _, err := b.store.Write(ctx, blobPath, value); err != nil {
		return [32]byte{}, fmt.Errorf("upload blob: %w", err)
	}

	return blobID, nil
}

func (b *BlobStorage) Read(ctx context.Context, blobID [32]byte) ([]byte, error) {
	blobIDHex := hex.EncodeToString(blobID[:])
	blobPath := b.store.BlobPath(blobIDHex)

	data, _, err := b.store.Read(ctx, blobPath)
	if err != nil {
		return nil, fmt.Errorf("fetch blob %s: %w", blobIDHex[:8], err)
	}

	if b.config.VerifyBlobsOnRead {
		hash := sha256.Sum256(data)
		if hash != blobID {
			return nil, fmt.Errorf("blob integrity check failed: expected %s, got %s",
				blobIDHex[:8], hex.EncodeToString(hash[:])[:8])
		}
	}

	return data, nil
}

func (b *BlobStorage) Exists(ctx context.Context, blobID [32]byte) (bool, error) {
	blobIDHex := hex.EncodeToString(blobID[:])
	blobPath := b.store.BlobPath(blobIDHex)
	return b.store.Exists(ctx, blobPath)
}

func (b *BlobStorage) Delete(ctx context.Context, blobID [32]byte) error {
	blobIDHex := hex.EncodeToString(blobID[:])
	blobPath := b.store.BlobPath(blobIDHex)
	return b.store.Delete(ctx, blobPath)
}

func BlobIDToHex(blobID [32]byte) string {
	return hex.EncodeToString(blobID[:])
}

func HexToBlobID(hexStr string) ([32]byte, error) {
	var blobID [32]byte
	decoded, err := hex.DecodeString(hexStr)
	if err != nil {
		return blobID, err
	}
	if len(decoded) != 32 {
		return blobID, fmt.Errorf("invalid blob ID length: %d", len(decoded))
	}
	copy(blobID[:], decoded)
	return blobID, nil
}

func ComputeBlobID(value []byte) [32]byte {
	return sha256.Sum256(value)
}
