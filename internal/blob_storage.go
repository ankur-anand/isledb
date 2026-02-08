package internal

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/config"
	"gocloud.dev/blob"
)

type BlobStorage struct {
	store  *blobstore.Store
	config config.ValueStorageConfig
}

func NewBlobStorage(store *blobstore.Store, config config.ValueStorageConfig) *BlobStorage {
	return &BlobStorage{
		store:  store,
		config: config,
	}
}

func (b *BlobStorage) Write(ctx context.Context, value []byte) ([32]byte, error) {

	blobID := sha256.Sum256(value)
	blobIDHex := hex.EncodeToString(blobID[:])

	blobPath := b.store.BlobPath(blobIDHex)
	if _, err := b.store.WriteReader(ctx, blobPath, bytes.NewReader(value), &blob.WriterOptions{
		ContentType: "application/octet-stream",
		IfNotExist:  true,
	}); err != nil {
		if errors.Is(err, blobstore.ErrPreconditionFailed) {
			return blobID, nil
		}
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

func (b *BlobStorage) BatchDelete(ctx context.Context, blobIDs [][32]byte) error {
	keys := make([]string, 0, len(blobIDs))
	for _, blobID := range blobIDs {
		blobIDHex := hex.EncodeToString(blobID[:])
		keys = append(keys, b.store.BlobPath(blobIDHex))
	}
	return b.store.BatchDelete(ctx, keys)
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
