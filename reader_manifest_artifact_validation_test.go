package isledb

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal"
	"github.com/ankur-anand/isledb/internal/manifest"
)

const testManifestArtifactChecksum = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func TestOpenReaderRejectsInvalidManifestArtifactMetadata(t *testing.T) {
	tests := []struct {
		name string
		meta manifest.SSTMeta
		want string
	}{
		{
			name: "missing SST checksum",
			meta: manifest.SSTMeta{
				ID: "missing-sst-checksum", Size: 128,
				Bloom: manifest.BloomMeta{Offset: 128},
			},
			want: "invalid SST checksum",
		},
		{
			name: "missing Bloom checksum",
			meta: manifest.SSTMeta{
				ID: "missing-bloom-checksum", Size: 128, Checksum: testManifestArtifactChecksum,
				Bloom: manifest.BloomMeta{
					BitsPerKey: 10, K: 6, Offset: 128, Length: 32,
				},
			},
			want: "invalid Bloom checksum",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			store := blobstore.NewMemory("invalid-reader-manifest")
			defer store.Close()
			manifestStore := manifest.NewStore(store)
			if _, err := manifestStore.ClaimWriter(ctx, "writer"); err != nil {
				t.Fatal(err)
			}
			if _, err := manifestStore.AppendAddSSTableWithFence(ctx, test.meta); err != nil {
				t.Fatal(err)
			}

			reader, err := newReader(ctx, store, readerOptions{CacheDir: t.TempDir()})
			if reader != nil {
				_ = reader.Close()
				t.Fatal("newReader returned a Reader for invalid artifact metadata")
			}
			if !errors.Is(err, manifest.ErrInvalidManifest) ||
				!strings.Contains(err.Error(), test.meta.ID) ||
				!strings.Contains(err.Error(), test.want) {
				t.Fatalf("newReader error=%v want manifest error containing SST ID and %q", err, test.want)
			}
		})
	}
}

func TestReaderRefreshKeepsPreviousViewWhenArtifactMetadataIsInvalid(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("invalid-refreshed-manifest")
	defer store.Close()
	manifestStore := manifest.NewStore(store)
	valid := writeReaderArtifactCacheTestSST(t, ctx, store, manifestStore, []internal.MemEntry{
		{Key: []byte("key"), Seq: 1, Kind: internal.OpPut, Value: []byte("value")},
	}, 1)

	reader, err := newReader(ctx, store, readerOptions{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	invalid := manifest.SSTMeta{
		ID: "invalid-refresh-sst", SeqLo: 2, SeqHi: 2, Size: 128,
		Checksum: testManifestArtifactChecksum,
		Bloom: manifest.BloomMeta{
			BitsPerKey: 10, K: 6, Offset: 128, Length: 32,
		},
	}
	if _, err := manifestStore.AppendAddSSTableWithFence(ctx, invalid); err != nil {
		t.Fatal(err)
	}

	err = reader.Refresh(ctx)
	if !errors.Is(err, manifest.ErrInvalidManifest) || !strings.Contains(err.Error(), invalid.ID) {
		t.Fatalf("Refresh error=%v want invalid manifest for %q", err, invalid.ID)
	}
	view := reader.currentManifest()
	if ids := view.AllSSTIDs(); len(ids) != 1 || ids[0] != valid.Meta.ID {
		t.Fatalf("Reader published invalid refreshed view: ids=%v want=[%s]", ids, valid.Meta.ID)
	}
}
