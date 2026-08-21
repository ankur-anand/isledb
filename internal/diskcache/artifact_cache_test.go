package diskcache

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func testArtifactDescriptor(kind ArtifactKind, id string, data []byte) ArtifactDescriptor {
	digest := sha256.Sum256(data)
	return ArtifactDescriptor{
		Key:      ArtifactKey{Kind: kind, SSTID: id},
		Size:     int64(len(data)),
		Checksum: fmt.Sprintf("sha256:%x", digest[:]),
	}
}

func openTestArtifactCache(t testing.TB, dir string, sstBytes, bloomBytes int64) *ArtifactCache {
	t.Helper()
	cache, err := OpenArtifactCache(ArtifactCacheOptions{
		Dir: dir, SSTMaxBytes: sstBytes, BloomMaxBytes: bloomBytes,
		MaxOpenEntries: 8, TouchInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("open artifact cache: %v", err)
	}
	return cache
}

func TestArtifactCachePersistsAndLazilyVerifiesAfterReopen(t *testing.T) {
	dir := t.TempDir()
	data := []byte("persistent-sst-payload")
	desc := testArtifactDescriptor(ArtifactSST, "sst-persistent", data)

	cache := openTestArtifactCache(t, dir, 1<<20, 1<<20)
	handle, admission, err := cache.AdmitBytes(desc, data)
	if err != nil {
		t.Fatalf("admit artifact: %v", err)
	}
	if admission != ArtifactAdmitted {
		t.Fatalf("admission=%d want=%d", admission, ArtifactAdmitted)
	}
	if !bytes.Equal(handle.Bytes(), data) {
		t.Fatalf("admitted bytes=%q want=%q", handle.Bytes(), data)
	}
	if presence, err := cache.Probe(desc); err != nil || presence != ArtifactResidentVerified {
		t.Fatalf("probe after admission=%d err=%v", presence, err)
	}
	if err := handle.Close(); err != nil {
		t.Fatalf("close admitted handle: %v", err)
	}
	if err := cache.Close(); err != nil {
		t.Fatalf("close cache: %v", err)
	}

	reopened := openTestArtifactCache(t, dir, 1<<20, 1<<20)
	defer reopened.Close()
	if presence, err := reopened.Probe(desc); err != nil || presence != ArtifactResidentUnverified {
		t.Fatalf("probe recovered=%d err=%v", presence, err)
	}
	recovered, ok, err := reopened.Acquire(desc)
	if err != nil || !ok {
		t.Fatalf("acquire recovered ok=%t err=%v", ok, err)
	}
	if !bytes.Equal(recovered.Bytes(), data) {
		t.Fatalf("recovered bytes=%q want=%q", recovered.Bytes(), data)
	}
	if presence, err := reopened.Probe(desc); err != nil || presence != ArtifactResidentVerified {
		t.Fatalf("probe verified recovery=%d err=%v", presence, err)
	}
	if err := recovered.Close(); err != nil {
		t.Fatalf("close recovered handle: %v", err)
	}
	stats := reopened.Stats(ArtifactSST)
	if stats.Hits != 1 || stats.RecoveredEntries != 1 || stats.ResidentBytes != int64(len(data)) {
		t.Fatalf("recovered stats=%+v", stats)
	}
}

func TestArtifactCacheFormatMismatchRebuildsOwnedData(t *testing.T) {
	dir := t.TempDir()
	sstData := []byte("old-format-sst")
	bloomData := []byte("old-format-bloom")
	sstDesc := testArtifactDescriptor(ArtifactSST, "sst-old-format", sstData)
	bloomDesc := testArtifactDescriptor(ArtifactBloom, "bloom-old-format", bloomData)

	cache := openTestArtifactCache(t, dir, 1<<20, 1<<20)
	for _, artifact := range []struct {
		desc ArtifactDescriptor
		data []byte
	}{
		{desc: sstDesc, data: sstData},
		{desc: bloomDesc, data: bloomData},
	} {
		handle, _, err := cache.AdmitBytes(artifact.desc, artifact.data)
		if err != nil {
			t.Fatal(err)
		}
		if err := handle.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := cache.Close(); err != nil {
		t.Fatal(err)
	}

	preservedPath := filepath.Join(dir, "README.keep")
	if err := os.WriteFile(preservedPath, []byte("not cache-owned"), 0o600); err != nil {
		t.Fatal(err)
	}
	futurePath := filepath.Join(dir, "v99", "future.cache")
	if err := os.MkdirAll(filepath.Dir(futurePath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(futurePath, []byte("future-format"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "incoming", "stale.part"), []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}
	metaPath := filepath.Join(dir, "CACHEMETA")
	if err := os.WriteFile(metaPath, []byte("isledb-artifact-cache-v99\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	reopened := openTestArtifactCache(t, dir, 1<<20, 1<<20)
	defer reopened.Close()
	for _, desc := range []ArtifactDescriptor{sstDesc, bloomDesc} {
		if presence, err := reopened.Probe(desc); err != nil || presence != ArtifactAbsent {
			t.Fatalf("old-format artifact presence=%d err=%v", presence, err)
		}
	}
	if stats := reopened.Stats(ArtifactSST); stats.ResidentEntries != 0 || stats.ResidentBytes != 0 {
		t.Fatalf("rebuilt SST stats=%+v", stats)
	}
	if stats := reopened.Stats(ArtifactBloom); stats.ResidentEntries != 0 || stats.ResidentBytes != 0 {
		t.Fatalf("rebuilt Bloom stats=%+v", stats)
	}
	if data, err := os.ReadFile(metaPath); err != nil || string(data) != artifactCacheFormat {
		t.Fatalf("rebuilt CACHEMETA=%q err=%v", data, err)
	}
	if _, err := os.Stat(futurePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("future cache version survived rebuild: %v", err)
	}
	if data, err := os.ReadFile(preservedPath); err != nil || string(data) != "not cache-owned" {
		t.Fatalf("unrelated root file=%q err=%v", data, err)
	}
	incoming, err := os.ReadDir(filepath.Join(dir, "incoming"))
	if err != nil || len(incoming) != 0 {
		t.Fatalf("rebuilt incoming=%v err=%v", incoming, err)
	}
	if _, err := os.Stat(filepath.Join(dir, artifactCacheLockName)); err != nil {
		t.Fatalf("cache lock was not preserved: %v", err)
	}
}

func TestArtifactCacheRejectsConcurrentOwnerAndReleasesOnClose(t *testing.T) {
	dir := t.TempDir()
	cache := openTestArtifactCache(t, dir, 1<<20, 1<<20)

	if _, err := OpenArtifactCache(ArtifactCacheOptions{Dir: dir}); !errors.Is(err, ErrArtifactCacheLocked) {
		t.Fatalf("second open error=%v want=%v", err, ErrArtifactCacheLocked)
	}
	if err := cache.Close(); err != nil {
		t.Fatalf("close owner: %v", err)
	}
	reopened := openTestArtifactCache(t, dir, 1<<20, 1<<20)
	if err := reopened.Close(); err != nil {
		t.Fatalf("close reopened cache: %v", err)
	}
}

func TestArtifactCacheCloseRetainsLockUntilPinnedHandleCloses(t *testing.T) {
	dir := t.TempDir()
	data := []byte("pinned")
	desc := testArtifactDescriptor(ArtifactSST, "sst-pinned-close", data)
	cache := openTestArtifactCache(t, dir, 1<<20, 1<<20)
	handle, _, err := cache.AdmitBytes(desc, data)
	if err != nil {
		t.Fatal(err)
	}
	if err := cache.Close(); err != nil {
		t.Fatalf("close cache: %v", err)
	}
	if _, err := OpenArtifactCache(ArtifactCacheOptions{Dir: dir}); !errors.Is(err, ErrArtifactCacheLocked) {
		t.Fatalf("open while handle pinned error=%v", err)
	}
	if !bytes.Equal(handle.Bytes(), data) {
		t.Fatal("pinned bytes changed after cache close")
	}
	if err := handle.Close(); err != nil {
		t.Fatalf("close pinned handle: %v", err)
	}
	reopened := openTestArtifactCache(t, dir, 1<<20, 1<<20)
	_ = reopened.Close()
}

func TestArtifactCacheCloseRetainsLockUntilActiveFillAborts(t *testing.T) {
	dir := t.TempDir()
	data := []byte("active-fill")
	desc := testArtifactDescriptor(ArtifactSST, "sst-active-fill", data)
	cache := openTestArtifactCache(t, dir, 1<<20, 1<<20)
	fill, err := cache.BeginFill(desc)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fill.Write(data[:4]); err != nil {
		t.Fatal(err)
	}
	if err := cache.Close(); err != nil {
		t.Fatalf("close cache: %v", err)
	}
	if _, err := OpenArtifactCache(ArtifactCacheOptions{Dir: dir}); !errors.Is(err, ErrArtifactCacheLocked) {
		t.Fatalf("open while fill active error=%v", err)
	}
	if err := fill.Abort(); err != nil {
		t.Fatalf("abort fill: %v", err)
	}
	reopened := openTestArtifactCache(t, dir, 1<<20, 1<<20)
	_ = reopened.Close()
}

func TestArtifactCacheCommitAfterCloseCleansFillAndReleasesLock(t *testing.T) {
	dir := t.TempDir()
	data := []byte("complete-active-fill")
	desc := testArtifactDescriptor(ArtifactSST, "sst-commit-after-close", data)
	cache := openTestArtifactCache(t, dir, 1<<20, 1<<20)
	fill, err := cache.BeginFill(desc)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fill.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := cache.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := fill.Commit(); !errors.Is(err, ErrArtifactCacheClosed) {
		t.Fatalf("commit after close error=%v", err)
	}
	reopened := openTestArtifactCache(t, dir, 1<<20, 1<<20)
	defer reopened.Close()
	if presence, err := reopened.Probe(desc); err != nil || presence != ArtifactAbsent {
		t.Fatalf("closed fill presence=%d err=%v", presence, err)
	}
}

func TestArtifactCacheCrashReleasesLockAndCleansIncoming(t *testing.T) {
	if os.Getenv("ISLEDB_ARTIFACT_CACHE_CRASH_HELPER") == "1" {
		dir := os.Getenv("ISLEDB_ARTIFACT_CACHE_CRASH_DIR")
		cache, err := OpenArtifactCache(ArtifactCacheOptions{Dir: dir})
		if err != nil {
			fmt.Fprintf(os.Stdout, "error:%v\n", err)
			os.Exit(2)
		}
		completedData := []byte("completed-before-crash")
		completedDesc := testArtifactDescriptor(ArtifactSST, "sst-completed-before-crash", completedData)
		handle, _, err := cache.AdmitBytes(completedDesc, completedData)
		if err != nil {
			fmt.Fprintf(os.Stdout, "error:%v\n", err)
			os.Exit(2)
		}
		if err := handle.Close(); err != nil {
			fmt.Fprintf(os.Stdout, "error:%v\n", err)
			os.Exit(2)
		}
		partialData := []byte("unfinished-download")
		fill, err := cache.BeginFill(testArtifactDescriptor(ArtifactSST, "sst-crash", partialData))
		if err != nil {
			fmt.Fprintf(os.Stdout, "error:%v\n", err)
			os.Exit(2)
		}
		_, _ = fill.Write(partialData[:5])
		fmt.Fprintln(os.Stdout, "ready")
		time.Sleep(time.Hour)
		return
	}

	dir := t.TempDir()
	command := exec.Command(os.Args[0], "-test.run=^TestArtifactCacheCrashReleasesLockAndCleansIncoming$")
	command.Env = append(os.Environ(),
		"ISLEDB_ARTIFACT_CACHE_CRASH_HELPER=1",
		"ISLEDB_ARTIFACT_CACHE_CRASH_DIR="+dir,
	)
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	line, err := bufio.NewReader(stdout).ReadString('\n')
	if err != nil || strings.TrimSpace(line) != "ready" {
		_ = command.Process.Kill()
		_ = command.Wait()
		t.Fatalf("crash helper line=%q err=%v", line, err)
	}
	if _, err := OpenArtifactCache(ArtifactCacheOptions{Dir: dir}); !errors.Is(err, ErrArtifactCacheLocked) {
		_ = command.Process.Kill()
		_ = command.Wait()
		t.Fatalf("open while helper owns lock error=%v", err)
	}
	if err := command.Process.Kill(); err != nil {
		t.Fatalf("kill cache owner: %v", err)
	}
	_ = command.Wait()

	cache := openTestArtifactCache(t, dir, 1<<20, 1<<20)
	defer cache.Close()
	incoming, err := os.ReadDir(filepath.Join(dir, "incoming"))
	if err != nil {
		t.Fatal(err)
	}
	if len(incoming) != 0 {
		t.Fatalf("incoming artifacts survived crash recovery: %v", incoming)
	}
	completedData := []byte("completed-before-crash")
	completedDesc := testArtifactDescriptor(ArtifactSST, "sst-completed-before-crash", completedData)
	handle, ok, err := cache.Acquire(completedDesc)
	if err != nil || !ok {
		t.Fatalf("acquire completed crash artifact ok=%t err=%v", ok, err)
	}
	if !bytes.Equal(handle.Bytes(), completedData) {
		t.Fatalf("completed crash artifact bytes=%q", handle.Bytes())
	}
	if err := handle.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestArtifactCacheAdmissionValidation(t *testing.T) {
	cache := openTestArtifactCache(t, t.TempDir(), 1<<20, 1<<20)
	defer cache.Close()
	data := []byte("valid-data")

	wrongSize := testArtifactDescriptor(ArtifactSST, "wrong-size", data)
	wrongSize.Size++
	if _, _, err := cache.AdmitBytes(wrongSize, data); !errors.Is(err, ErrArtifactSizeMismatch) {
		t.Fatalf("wrong-size admission error=%v", err)
	}

	wrongChecksum := testArtifactDescriptor(ArtifactBloom, "wrong-checksum", data)
	wrongChecksum.Checksum = fmt.Sprintf("sha256:%064x", 1)
	if _, _, err := cache.AdmitBytes(wrongChecksum, data); !errors.Is(err, ErrArtifactChecksumMismatch) {
		t.Fatalf("wrong-checksum admission error=%v", err)
	}

	incoming, err := os.ReadDir(cache.incomingDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(incoming) != 0 {
		t.Fatalf("rejected fills remain in incoming: %v", incoming)
	}

	retryDesc := testArtifactDescriptor(ArtifactBloom, "wrong-checksum", data)
	handle, admission, err := cache.AdmitBytes(retryDesc, data)
	if err != nil || admission != ArtifactAdmitted {
		t.Fatalf("retry admission=%d err=%v", admission, err)
	}
	if err := handle.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestArtifactFillRejectsOverlongWriteBeforeSpendingDisk(t *testing.T) {
	cache := openTestArtifactCache(t, t.TempDir(), 1<<20, 1<<20)
	defer cache.Close()
	data := []byte("bounded")
	desc := testArtifactDescriptor(ArtifactSST, "overlong-write", data)
	fill, err := cache.BeginFill(desc)
	if err != nil {
		t.Fatal(err)
	}
	defer fill.Abort()

	written, err := fill.Write(append(append([]byte(nil), data...), '!'))
	if !errors.Is(err, ErrArtifactSizeMismatch) {
		t.Fatalf("overlong Write bytes=%d error=%v want=%v", written, err, ErrArtifactSizeMismatch)
	}
	if written != 0 {
		t.Fatalf("overlong Write spent %d bytes before rejection", written)
	}
}

func TestArtifactCachePinnedCapacityBypassDoesNotEvictUsefulEntries(t *testing.T) {
	cache := openTestArtifactCache(t, t.TempDir(), 10, 10)
	defer cache.Close()
	unpinnedData := []byte("aaa")
	pinnedData := []byte("bbbbbbb")
	bypassData := []byte("cccccccc")
	unpinnedDesc := testArtifactDescriptor(ArtifactSST, "unpinned", unpinnedData)
	pinnedDesc := testArtifactDescriptor(ArtifactSST, "pinned", pinnedData)
	bypassDesc := testArtifactDescriptor(ArtifactSST, "bypass", bypassData)

	unpinned, _, err := cache.AdmitBytes(unpinnedDesc, unpinnedData)
	if err != nil {
		t.Fatal(err)
	}
	if err := unpinned.Close(); err != nil {
		t.Fatal(err)
	}
	pinned, _, err := cache.AdmitBytes(pinnedDesc, pinnedData)
	if err != nil {
		t.Fatal(err)
	}
	defer pinned.Close()

	bypassed, admission, err := cache.AdmitBytes(bypassDesc, bypassData)
	if err != nil {
		t.Fatal(err)
	}
	if admission != ArtifactBypassedPinnedCapacity {
		t.Fatalf("admission=%d want=%d", admission, ArtifactBypassedPinnedCapacity)
	}
	if err := bypassed.Close(); err != nil {
		t.Fatal(err)
	}
	if presence, err := cache.Probe(unpinnedDesc); err != nil || presence == ArtifactAbsent {
		t.Fatalf("useful unpinned entry was evicted before bypass: presence=%d err=%v", presence, err)
	}
}

func TestArtifactCachePublishRunsEveryDetachedCleanup(t *testing.T) {
	cache := openTestArtifactCache(t, t.TempDir(), 8, 8)
	defer cache.Close()
	dataA := []byte("aaaa")
	dataB := []byte("bbbb")
	dataC := []byte("cccccccc")
	descA := testArtifactDescriptor(ArtifactSST, "cleanup-a", dataA)
	descB := testArtifactDescriptor(ArtifactSST, "cleanup-b", dataB)
	descC := testArtifactDescriptor(ArtifactSST, "cleanup-c", dataC)

	for _, item := range []struct {
		desc ArtifactDescriptor
		data []byte
	}{{descA, dataA}, {descB, dataB}} {
		handle, _, err := cache.AdmitBytes(item.desc, item.data)
		if err != nil {
			t.Fatal(err)
		}
		if err := handle.Close(); err != nil {
			t.Fatal(err)
		}
	}
	pathB := cache.artifactPath(artifactIDFor(descB.Key))

	// Make the first victim's unlink fail after its mmap and descriptor have
	// been released. Publication must still run the second victim's cleanup.
	cache.mu.Lock()
	cache.tiers[ArtifactSST].entries[artifactIDFor(descA.Key).digest].path = cache.dir
	cache.mu.Unlock()

	if _, _, err := cache.AdmitBytes(descC, dataC); err == nil {
		t.Fatal("publication unexpectedly succeeded after injected cleanup failure")
	}
	if _, err := os.Stat(pathB); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("later detached cleanup was abandoned: stat error=%v", err)
	}
}

func TestArtifactCachePublishDoesNotHoldGlobalMutexDuringDirectorySync(t *testing.T) {
	cache := openTestArtifactCache(t, t.TempDir(), 1<<20, 1<<20)
	defer cache.Close()
	data := []byte("sync-without-global-lock")
	desc := testArtifactDescriptor(ArtifactSST, "sync-lock-scope", data)
	started := make(chan struct{})
	unblock := make(chan struct{})
	cache.syncDirectory = func(path string) error {
		if path == filepath.Dir(cache.artifactPath(artifactIDFor(desc.Key))) {
			close(started)
			<-unblock
		}
		return syncArtifactDirectory(path)
	}

	result := make(chan error, 1)
	go func() {
		handle, _, err := cache.AdmitBytes(desc, data)
		if handle != nil {
			err = errors.Join(err, handle.Close())
		}
		result <- err
	}()
	<-started
	lockAvailable := cache.mu.TryLock()
	if lockAvailable {
		cache.mu.Unlock()
	}
	close(unblock)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if !lockAvailable {
		t.Fatal("global cache mutex was held during directory sync")
	}
}

func TestArtifactCacheFirstShardCreationSyncsTierDirectory(t *testing.T) {
	cache := openTestArtifactCache(t, t.TempDir(), 1<<20, 1<<20)
	defer cache.Close()
	data := []byte("durable-first-shard")
	desc := testArtifactDescriptor(ArtifactSST, "first-shard", data)
	finalPath := cache.artifactPath(artifactIDFor(desc.Key))
	shardDir := filepath.Dir(finalPath)
	tierDir := filepath.Dir(shardDir)
	var synced []string
	cache.syncDirectory = func(path string) error {
		synced = append(synced, filepath.Clean(path))
		return syncArtifactDirectory(path)
	}

	handle, _, err := cache.AdmitBytes(desc, data)
	if err != nil {
		t.Fatal(err)
	}
	if err := handle.Close(); err != nil {
		t.Fatal(err)
	}
	if len(synced) < 2 || synced[0] != filepath.Clean(tierDir) || synced[1] != filepath.Clean(shardDir) {
		t.Fatalf("directory sync order=%v want tier then shard", synced)
	}
}

func TestArtifactCacheHardCapacityBypassesPinnedAndEvictsAfterRelease(t *testing.T) {
	cache := openTestArtifactCache(t, t.TempDir(), 10, 10)
	defer cache.Close()
	dataA := []byte("aaaaaa")
	dataB := []byte("bbbbbb")
	descA := testArtifactDescriptor(ArtifactSST, "sst-a", dataA)
	descB := testArtifactDescriptor(ArtifactSST, "sst-b", dataB)

	first, admission, err := cache.AdmitBytes(descA, dataA)
	if err != nil || admission != ArtifactAdmitted {
		t.Fatalf("admit A admission=%d err=%v", admission, err)
	}
	if stats := cache.Stats(ArtifactSST); stats.ResidentBytes != 6 || stats.PinnedBytes != 6 {
		t.Fatalf("stats with A pinned=%+v", stats)
	}

	bypass, admission, err := cache.AdmitBytes(descB, dataB)
	if err != nil || admission != ArtifactBypassedPinnedCapacity {
		t.Fatalf("pinned admission=%d err=%v", admission, err)
	}
	if !bytes.Equal(bypass.Bytes(), dataB) {
		t.Fatalf("bypass bytes=%q", bypass.Bytes())
	}
	if stats := cache.Stats(ArtifactSST); stats.ResidentBytes > stats.MaxBytes || stats.ResidentBytes != 6 {
		t.Fatalf("hard capacity violated: %+v", stats)
	}
	if err := bypass.Close(); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	second, admission, err := cache.AdmitBytes(descB, dataB)
	if err != nil || admission != ArtifactAdmitted {
		t.Fatalf("admit B after release admission=%d err=%v", admission, err)
	}
	defer second.Close()
	if presence, _ := cache.Probe(descA); presence != ArtifactAbsent {
		t.Fatalf("evicted A presence=%d", presence)
	}
	stats := cache.Stats(ArtifactSST)
	if stats.ResidentBytes != 6 || stats.Evictions != 1 || stats.AdmissionBypasses != 1 {
		t.Fatalf("capacity stats=%+v", stats)
	}
}

func TestArtifactCacheOversizedArtifactUsesTransientHandle(t *testing.T) {
	cache := openTestArtifactCache(t, t.TempDir(), 4, 4)
	defer cache.Close()
	data := []byte("oversized")
	desc := testArtifactDescriptor(ArtifactBloom, "bloom-oversized", data)

	handle, admission, err := cache.AdmitBytes(desc, data)
	if err != nil || admission != ArtifactBypassedOversized {
		t.Fatalf("oversized admission=%d err=%v", admission, err)
	}
	if stats := cache.Stats(ArtifactBloom); stats.ResidentBytes != 0 || stats.AdmissionBypasses != 1 {
		t.Fatalf("oversized stats=%+v", stats)
	}
	if !bytes.Equal(handle.Bytes(), data) {
		t.Fatalf("transient bytes=%q", handle.Bytes())
	}
	if err := handle.Close(); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(cache.incomingDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("transient artifact survived handle close: %v", entries)
	}
}

func TestArtifactCachePurgeDefersPinnedRemoval(t *testing.T) {
	cache := openTestArtifactCache(t, t.TempDir(), 1<<20, 1<<20)
	defer cache.Close()
	data := []byte("pinned-for-purge")
	desc := testArtifactDescriptor(ArtifactSST, "sst-purge", data)
	handle, _, err := cache.AdmitBytes(desc, data)
	if err != nil {
		t.Fatal(err)
	}
	path := cache.artifactPath(artifactIDFor(desc.Key))
	if err := cache.Purge(ArtifactSST); err != nil {
		t.Fatalf("purge: %v", err)
	}
	if presence, _ := cache.Probe(desc); presence != ArtifactAbsent {
		t.Fatalf("pending purge presence=%d", presence)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("pinned file removed early: %v", err)
	}
	if !bytes.Equal(handle.Bytes(), data) {
		t.Fatal("pinned purge bytes changed")
	}
	if err := handle.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("purged file stat error=%v", err)
	}
}

func TestArtifactCacheRecoveredCorruptionBecomesMiss(t *testing.T) {
	dir := t.TempDir()
	data := []byte("verified-before-restart")
	desc := testArtifactDescriptor(ArtifactBloom, "bloom-corrupt", data)
	cache := openTestArtifactCache(t, dir, 1<<20, 1<<20)
	handle, _, err := cache.AdmitBytes(desc, data)
	if err != nil {
		t.Fatal(err)
	}
	_ = handle.Close()
	path := cache.artifactPath(artifactIDFor(desc.Key))
	_ = cache.Close()

	corrupt := bytes.Repeat([]byte("x"), len(data))
	if err := os.WriteFile(path, corrupt, 0o600); err != nil {
		t.Fatal(err)
	}
	reopened := openTestArtifactCache(t, dir, 1<<20, 1<<20)
	defer reopened.Close()
	got, ok, err := reopened.Acquire(desc)
	if err != nil || ok || got != nil {
		t.Fatalf("corrupt acquire handle=%v ok=%t err=%v", got, ok, err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("corrupt artifact was not removed: %v", err)
	}
	stats := reopened.Stats(ArtifactBloom)
	if stats.Corruptions != 1 || stats.Misses != 1 {
		t.Fatalf("corruption stats=%+v", stats)
	}
}

func TestArtifactCacheSuccessfulVerificationPreservesPendingRemovalReason(t *testing.T) {
	dir := t.TempDir()
	data := []byte("verified-while-purged")
	desc := testArtifactDescriptor(ArtifactSST, "purged-during-verification", data)
	cache := openTestArtifactCache(t, dir, 1<<20, 1<<20)
	handle, _, err := cache.AdmitBytes(desc, data)
	if err != nil {
		t.Fatal(err)
	}
	if err := handle.Close(); err != nil {
		t.Fatal(err)
	}
	if err := cache.Close(); err != nil {
		t.Fatal(err)
	}

	cache = openTestArtifactCache(t, dir, 1<<20, 1<<20)
	defer cache.Close()
	started := make(chan struct{})
	unblock := make(chan struct{})
	cache.verifyFile = func(path string, got ArtifactDescriptor) (bool, error) {
		close(started)
		<-unblock
		return verifyArtifactFile(path, got)
	}
	result := make(chan error, 1)
	go func() {
		acquired, ok, err := cache.Acquire(desc)
		if acquired != nil {
			err = errors.Join(err, acquired.Close())
		}
		if ok {
			err = errors.Join(err, errors.New("purged verification returned a hit"))
		}
		result <- err
	}()
	<-started
	if err := cache.Purge(ArtifactSST); err != nil {
		close(unblock)
		t.Fatal(err)
	}
	close(unblock)
	if err := <-result; err != nil {
		t.Fatal(err)
	}

	stats := cache.Stats(ArtifactSST)
	if stats.Corruptions != 0 || stats.PurgeRemovals != 1 {
		t.Fatalf("pending purge was misclassified: %+v", stats)
	}
}

func TestArtifactCacheRefillOfUnverifiedRecoveryIsNotCorruption(t *testing.T) {
	dir := t.TempDir()
	data := []byte("recovered-and-refilled")
	desc := testArtifactDescriptor(ArtifactBloom, "refilled-recovery", data)
	cache := openTestArtifactCache(t, dir, 1<<20, 1<<20)
	handle, _, err := cache.AdmitBytes(desc, data)
	if err != nil {
		t.Fatal(err)
	}
	if err := handle.Close(); err != nil {
		t.Fatal(err)
	}
	if err := cache.Close(); err != nil {
		t.Fatal(err)
	}

	cache = openTestArtifactCache(t, dir, 1<<20, 1<<20)
	defer cache.Close()
	handle, admission, err := cache.AdmitBytes(desc, data)
	if err != nil || admission != ArtifactAdmitted {
		t.Fatalf("refill admission=%d err=%v", admission, err)
	}
	if err := handle.Close(); err != nil {
		t.Fatal(err)
	}
	if stats := cache.Stats(ArtifactBloom); stats.Corruptions != 0 {
		t.Fatalf("unverified recovery refill was classified as corruption: %+v", stats)
	}
}

func TestArtifactCacheConcurrentRecoveredAcquire(t *testing.T) {
	dir := t.TempDir()
	data := bytes.Repeat([]byte("concurrent-recovery"), 1024)
	desc := testArtifactDescriptor(ArtifactSST, "sst-concurrent-recovery", data)
	cache := openTestArtifactCache(t, dir, 1<<20, 1<<20)
	handle, _, err := cache.AdmitBytes(desc, data)
	if err != nil {
		t.Fatal(err)
	}
	_ = handle.Close()
	_ = cache.Close()

	reopened := openTestArtifactCache(t, dir, 1<<20, 1<<20)
	defer reopened.Close()
	const workers = 32
	var wait sync.WaitGroup
	wait.Add(workers)
	start := make(chan struct{})
	for range workers {
		go func() {
			defer wait.Done()
			<-start
			handle, ok, err := reopened.Acquire(desc)
			if err != nil || !ok {
				t.Errorf("concurrent acquire ok=%t err=%v", ok, err)
				return
			}
			if !bytes.Equal(handle.Bytes(), data) {
				t.Error("concurrent acquire returned wrong bytes")
			}
			if err := handle.Close(); err != nil {
				t.Errorf("close concurrent handle: %v", err)
			}
		}()
	}
	close(start)
	wait.Wait()
	stats := reopened.Stats(ArtifactSST)
	if stats.Hits != workers || stats.Misses != 0 || stats.PinnedBytes != 0 {
		t.Fatalf("concurrent stats=%+v", stats)
	}
}

func TestArtifactCacheRecoveryEnforcesReducedCapacityAndCleansMalformedFiles(t *testing.T) {
	dir := t.TempDir()
	cache := openTestArtifactCache(t, dir, 20, 20)
	dataA := []byte("aaaaaa")
	dataB := []byte("bbbbbb")
	descA := testArtifactDescriptor(ArtifactSST, "sst-oldest", dataA)
	descB := testArtifactDescriptor(ArtifactSST, "sst-newest", dataB)
	handleA, _, err := cache.AdmitBytes(descA, dataA)
	if err != nil {
		t.Fatal(err)
	}
	_ = handleA.Close()
	handleB, _, err := cache.AdmitBytes(descB, dataB)
	if err != nil {
		t.Fatal(err)
	}
	_ = handleB.Close()
	oldTime := time.Unix(1_700_000_000, 0)
	newTime := oldTime.Add(time.Second)
	if err := os.Chtimes(cache.artifactPath(artifactIDFor(descA.Key)), oldTime, oldTime); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(cache.artifactPath(artifactIDFor(descB.Key)), newTime, newTime); err != nil {
		t.Fatal(err)
	}
	malformed := filepath.Join(cache.versionDir, ArtifactSST.dirName(), "not-a-shard", "junk.sst")
	if err := os.MkdirAll(filepath.Dir(malformed), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(malformed, []byte("junk"), 0o600); err != nil {
		t.Fatal(err)
	}
	_ = cache.Close()

	reopened := openTestArtifactCache(t, dir, 10, 20)
	defer reopened.Close()
	stats := reopened.Stats(ArtifactSST)
	if stats.ResidentBytes != 6 || stats.ResidentEntries != 1 {
		t.Fatalf("reduced-capacity recovery stats=%+v", stats)
	}
	if presence, _ := reopened.Probe(descA); presence != ArtifactAbsent {
		t.Fatalf("oldest recovered artifact presence=%d", presence)
	}
	if presence, _ := reopened.Probe(descB); presence != ArtifactResidentUnverified {
		t.Fatalf("newest recovered artifact presence=%d", presence)
	}
	if _, err := os.Stat(malformed); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("malformed artifact survived recovery: %v", err)
	}
}

func TestArtifactCacheTierBudgetsAreIndependent(t *testing.T) {
	cache := openTestArtifactCache(t, t.TempDir(), 8, 8)
	defer cache.Close()
	sstData := []byte("sst-data")
	bloomData := []byte("bloom")
	sst := testArtifactDescriptor(ArtifactSST, "same-id", sstData)
	bloom := testArtifactDescriptor(ArtifactBloom, "same-id", bloomData)

	sstHandle, sstAdmission, err := cache.AdmitBytes(sst, sstData)
	if err != nil || sstAdmission != ArtifactAdmitted {
		t.Fatalf("SST admission=%d err=%v", sstAdmission, err)
	}
	bloomHandle, bloomAdmission, err := cache.AdmitBytes(bloom, bloomData)
	if err != nil || bloomAdmission != ArtifactAdmitted {
		t.Fatalf("Bloom admission=%d err=%v", bloomAdmission, err)
	}
	_ = sstHandle.Close()
	_ = bloomHandle.Close()
	if cache.Stats(ArtifactSST).ResidentBytes != int64(len(sstData)) ||
		cache.Stats(ArtifactBloom).ResidentBytes != int64(len(bloomData)) {
		t.Fatalf("tier stats SST=%+v Bloom=%+v", cache.Stats(ArtifactSST), cache.Stats(ArtifactBloom))
	}
}

func TestArtifactCacheVerifiedFillReplacesConflictingUnpinnedEntry(t *testing.T) {
	cache := openTestArtifactCache(t, t.TempDir(), 1<<20, 1<<20)
	defer cache.Close()
	oldData := []byte("old-bytes")
	newData := []byte("new-bytes")
	oldDesc := testArtifactDescriptor(ArtifactBloom, "same-sst-id", oldData)
	newDesc := testArtifactDescriptor(ArtifactBloom, "same-sst-id", newData)

	oldHandle, _, err := cache.AdmitBytes(oldDesc, oldData)
	if err != nil {
		t.Fatal(err)
	}
	_ = oldHandle.Close()
	newHandle, admission, err := cache.AdmitBytes(newDesc, newData)
	if err != nil || admission != ArtifactAdmitted {
		t.Fatalf("replacement admission=%d err=%v", admission, err)
	}
	if !bytes.Equal(newHandle.Bytes(), newData) {
		t.Fatalf("replacement bytes=%q", newHandle.Bytes())
	}
	_ = newHandle.Close()

	acquired, ok, err := cache.Acquire(newDesc)
	if err != nil || !ok || !bytes.Equal(acquired.Bytes(), newData) {
		t.Fatalf("acquire replacement ok=%t err=%v", ok, err)
	}
	_ = acquired.Close()
}

func TestArtifactCacheBoundsUnpinnedOpenMappings(t *testing.T) {
	dir := t.TempDir()
	cache, err := OpenArtifactCache(ArtifactCacheOptions{
		Dir: dir, SSTMaxBytes: 1 << 20, BloomMaxBytes: 1 << 20,
		MaxOpenEntries: 1, TouchInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()

	for index := range 3 {
		data := []byte(fmt.Sprintf("data-%d", index))
		desc := testArtifactDescriptor(ArtifactSST, fmt.Sprintf("sst-%d", index), data)
		handle, _, err := cache.AdmitBytes(desc, data)
		if err != nil {
			t.Fatal(err)
		}
		if err := handle.Close(); err != nil {
			t.Fatal(err)
		}
	}
	cache.mu.Lock()
	openEntries := cache.openEntries
	cache.mu.Unlock()
	if openEntries != 1 {
		t.Fatalf("open entries=%d want=1", openEntries)
	}
}

func TestArtifactCacheMappedEntryDoesNotRetainFileDescriptor(t *testing.T) {
	cache := openTestArtifactCache(t, t.TempDir(), 1<<20, 1<<20)
	defer cache.Close()
	data := []byte("mapped-without-open-fd")
	desc := testArtifactDescriptor(ArtifactSST, "no-retained-fd", data)
	handle, _, err := cache.AdmitBytes(desc, data)
	if err != nil {
		t.Fatal(err)
	}
	defer handle.Close()

	cache.mu.Lock()
	entry := cache.tiers[ArtifactSST].entries[artifactIDFor(desc.Key).digest]
	retainedFile := entry.file
	cache.mu.Unlock()
	if retainedFile != nil {
		t.Fatal("mmap entry retained its source file descriptor")
	}
}
