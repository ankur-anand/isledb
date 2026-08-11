package manifest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
)

func TestWriterPolicyIsPersistedAndImmutable(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("writer-view-policy")
	defer store.Close()

	manifestStore := NewStore(store)
	if _, err := manifestStore.Replay(ctx); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	const age = 45 * time.Minute
	if _, err := manifestStore.ClaimWriterWithPolicy(ctx, "writer-1", age); err != nil {
		t.Fatalf("ClaimWriterWithPolicy: %v", err)
	}
	current := manifestStore.CurrentData()
	if current.MaxPinnedViewAge != age {
		t.Fatalf("MaxPinnedViewAge=%s, want %s", current.MaxPinnedViewAge, age)
	}
	if _, err := manifestStore.ClaimWriterWithPolicy(ctx, "writer-2", time.Hour); !errors.Is(err, ErrStorePolicyMismatch) {
		t.Fatalf("mismatched writer policy error=%v, want %v", err, ErrStorePolicyMismatch)
	}
}

func TestCurrentRejectsNegativePinnedViewAge(t *testing.T) {
	if _, err := EncodeCurrent(&Current{MaxPinnedViewAge: -time.Second}); !errors.Is(err, ErrInvalidManifest) {
		t.Fatalf("EncodeCurrent error=%v, want %v", err, ErrInvalidManifest)
	}
	if _, err := DecodeCurrent([]byte(`{"max_pinned_view_age_nanos":-1}`)); !errors.Is(err, ErrInvalidManifest) {
		t.Fatalf("DecodeCurrent error=%v, want %v", err, ErrInvalidManifest)
	}
}
