package isledb

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal"
	"github.com/ankur-anand/isledb/internal/manifest"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// bloomMayContainLoaded answers from the filter after its background load.
func bloomMayContainLoaded(r *Reader, meta sstMetadata, key []byte) bool {
	r.bloomMayContain(meta, key)
	r.bloomBackground.Wait()
	return r.bloomMayContain(meta, key)
}

// holdBloomLoad stands in for a slow filter download until release is closed.
func holdBloomLoad(t *testing.T, r *Reader, id string) (release chan struct{}) {
	t.Helper()
	release = make(chan struct{})
	started := make(chan struct{})
	go func() {
		_, _ = r.bloomLoads.Do(context.Background(), id, func(ctx context.Context) (any, error) {
			close(started)
			select {
			case <-release:
				return nil, errors.New("held load released")
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		})
	}()
	<-started
	return release
}

func TestLookupDoesNotWaitForFilter(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("bloom-no-wait")
	defer store.Close()
	manifestStore := manifest.NewStore(store)
	result := writeReaderArtifactCacheTestSST(t, ctx, store, manifestStore, []internal.MemEntry{
		{Key: []byte("key"), Seq: 1, Kind: internal.OpPut, Value: []byte("value")},
	}, 1)
	metrics := DefaultReaderMetrics(nil)
	reader, err := newReader(ctx, store, readerOptions{CacheDir: t.TempDir(), Metrics: metrics})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	release := holdBloomLoad(t, reader, result.Meta.ID)
	type lookup struct {
		maybe bool
		value []byte
		found bool
		err   error
	}
	done := make(chan lookup, 1)
	go func() {
		var l lookup
		l.maybe = reader.bloomMayContain(result.Meta, []byte("absent"))
		l.value, l.found, l.err = reader.Get(ctx, []byte("key"))
		done <- l
	}()
	var l lookup
	select {
	case l = <-done:
	case <-time.After(5 * time.Second):
		close(release)
		t.Fatal("lookups waited for a filter still loading")
	}
	if !l.maybe {
		t.Fatal("a filter still loading answered definitely absent")
	}
	if l.err != nil || !l.found || string(l.value) != "value" {
		t.Fatalf("Get while the filter loads = %q, %v, %v", l.value, l.found, l.err)
	}
	if got := testutil.ToFloat64(metrics.BloomFilterSkips); got < 2 {
		t.Fatalf("filter skips = %v, want the 2 lookups that ran without it", got)
	}

	// Clear the failed load's backoff.
	close(release)
	reader.bloomBackground.Wait()
	reader.bloomLoading.Delete(result.Meta.ID)
	if bloomMayContainLoaded(reader, result.Meta, []byte("absent")) {
		t.Fatal("the loaded filter did not rule out an absent key")
	}
}

func TestCloseDoesNotWaitOutFilterLoad(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("bloom-close")
	defer store.Close()
	result := writeReaderArtifactCacheTestSST(t, ctx, store, manifest.NewStore(store), []internal.MemEntry{
		{Key: []byte("key"), Seq: 1, Kind: internal.OpPut, Value: []byte("value")},
	}, 1)
	reader, err := newReader(ctx, store, readerOptions{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	holdBloomLoad(t, reader, result.Meta.ID) // never released
	reader.bloomMayContain(result.Meta, []byte("key"))

	closed := make(chan error, 1)
	go func() { closed <- reader.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close waited for a filter load in flight")
	}
}

func TestFailedFilterLoadBacksOff(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("bloom-backoff")
	defer store.Close()
	key := []byte("present")
	data := bloomBytesForCacheTest(t, key)
	meta := sstMetadata{
		ID: "sst-unloadable-bloom",
		Bloom: bloomMetadata{
			Format:   manifest.BloomFormatExactV1,
			Length:   int64(len(data)),
			Checksum: bloomChecksum(data),
		},
	}
	corrupt := append([]byte(nil), data...)
	corrupt[len(corrupt)/2] ^= 0x01
	if _, err := store.Write(ctx, store.SSTPath(meta.ID), corrupt); err != nil {
		t.Fatal(err)
	}
	metrics := DefaultReaderMetrics(nil)
	reader := &Reader{
		store: store, fetcher: newSSTFetcher(store, nil, metrics),
		bloomCache: newBloomFilterCache(1 << 20), metrics: metrics,
	}
	for range 20 {
		if !bloomMayContainLoaded(reader, meta, []byte("absent")) {
			t.Fatal("an unloadable filter answered definitely absent")
		}
	}
	if got := testutil.ToFloat64(metrics.BloomFilterErrors); got != 1 {
		t.Fatalf("filter loads attempted = %v, want 1 within the retry delay", got)
	}
}

func TestLookupLoadsFilterFromDisk(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("bloom-from-disk")
	defer store.Close()
	result := writeReaderArtifactCacheTestSST(t, ctx, store, manifest.NewStore(store), []internal.MemEntry{
		{Key: []byte("key"), Seq: 1, Kind: internal.OpPut, Value: []byte("value")},
	}, 1)
	cacheDir := t.TempDir()
	warm, err := newReader(ctx, store, readerOptions{CacheDir: cacheDir})
	if err != nil {
		t.Fatal(err)
	}
	bloomMayContainLoaded(warm, result.Meta, []byte("key"))
	if err := warm.Close(); err != nil {
		t.Fatal(err)
	}

	metrics := DefaultReaderMetrics(nil)
	reader, err := newReader(ctx, store, readerOptions{CacheDir: cacheDir, Metrics: metrics})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if err := store.Delete(ctx, store.SSTPath(result.Meta.ID)); err != nil {
		t.Fatal(err)
	}
	if reader.bloomMayContain(result.Meta, []byte("absent")) {
		t.Fatal("the filter on disk did not rule out an absent key")
	}
	if got := testutil.ToFloat64(metrics.BloomFilterSkips); got != 0 {
		t.Fatalf("filter skips = %v, want none with the filter on disk", got)
	}
	if got := testutil.ToFloat64(metrics.BloomFilterErrors); got != 0 {
		t.Fatalf("filter errors = %v", got)
	}
}

func TestFilterLoadsAreCapped(t *testing.T) {
	store := blobstore.NewMemory("bloom-cap")
	defer store.Close()
	reader := &Reader{
		store: store, fetcher: newSSTFetcher(store, nil, nil),
		bloomCache: newBloomFilterCache(1 << 20),
	}
	var releases []chan struct{}
	for i := range 10 {
		meta := sstMetadata{
			ID:    fmt.Sprintf("sst-%d", i),
			Bloom: bloomMetadata{Format: manifest.BloomFormatExactV1, Length: 64, Checksum: "sha256:00"},
		}
		releases = append(releases, holdBloomLoad(t, reader, meta.ID))
		if !reader.bloomMayContain(meta, []byte("key")) {
			t.Fatal("a filter not loaded answered definitely absent")
		}
	}
	if got := reader.bloomLoadsRunning.Load(); got != maxBloomLoads {
		t.Fatalf("%d filter loads running, want %d", got, maxBloomLoads)
	}
	for _, release := range releases {
		close(release)
	}
	reader.bloomBackground.Wait()
	if got := reader.bloomLoadsRunning.Load(); got != 0 {
		t.Fatalf("%d filter loads running after all finished", got)
	}
}
