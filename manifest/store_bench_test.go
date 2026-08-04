package manifest

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	_ "gocloud.dev/blob/s3blob"
)

const manifestPolicyBenchmarkCommits = 4_096

const manifestPolicyLiveBenchmarkCommits = 1_024

const defaultManifestPolicyMinIOURL = "s3://isledb-bench?endpoint=http://localhost:9000&region=us-east-1&use_path_style=true&response_checksum_validation=when_required&request_checksum_calculation=when_required"

type manifestPolicyBenchmarkStorage struct {
	Storage
	PageStorage

	headWrites   int64
	headBytes    int64
	maxHeadBytes int64
	pageWrites   int64
	pageBytes    int64
}

func (s *manifestPolicyBenchmarkStorage) WriteCurrentCAS(ctx context.Context, data []byte, expectedETag string) (string, error) {
	s.headWrites++
	s.headBytes += int64(len(data))
	if int64(len(data)) > s.maxHeadBytes {
		s.maxHeadBytes = int64(len(data))
	}
	return s.Storage.WriteCurrentCAS(ctx, data, expectedETag)
}

func (s *manifestPolicyBenchmarkStorage) WritePage(ctx context.Context, level uint8, id string, data []byte) (string, error) {
	s.pageWrites++
	s.pageBytes += int64(len(data))
	return s.PageStorage.WritePage(ctx, level, id, data)
}

func (s *manifestPolicyBenchmarkStorage) resetMetrics() {
	s.headWrites = 0
	s.headBytes = 0
	s.maxHeadBytes = 0
	s.pageWrites = 0
	s.pageBytes = 0
}

func BenchmarkManifestActiveEntryLimit(b *testing.B) {
	for _, limit := range []int{32, 64, 128, 256, 1_024} {
		b.Run(fmt.Sprintf("limit=%d", limit), func(b *testing.B) {
			benchmarkManifestPolicy(b, limit, defaultPageFanout)
		})
	}
}

func BenchmarkManifestFrontierPolicy(b *testing.B) {
	policies := []struct {
		active int
		fanout int
	}{
		{active: 32, fanout: 32},
		{active: 32, fanout: 64},
		{active: 32, fanout: 1_024},
		{active: 64, fanout: 32},
		{active: 64, fanout: 64},
		{active: 64, fanout: 1_024},
		{active: 128, fanout: 32},
		{active: 128, fanout: 64},
		{active: 128, fanout: 1_024},
		{active: 1_024, fanout: 1_024},
	}
	for _, policy := range policies {
		name := fmt.Sprintf("active=%d/fanout=%d", policy.active, policy.fanout)
		b.Run(name, func(b *testing.B) {
			benchmarkManifestPolicy(b, policy.active, policy.fanout)
		})
	}
}

func BenchmarkManifestFrontierPolicyLiveMinIO(b *testing.B) {
	if os.Getenv("ISLEDB_MINIO_BENCH") != "1" {
		b.Skip("set ISLEDB_MINIO_BENCH=1 and create the isledb-bench bucket to run against local MinIO")
	}
	setManifestPolicyMinIOEnv(b, "AWS_ACCESS_KEY_ID", "minioadmin")
	setManifestPolicyMinIOEnv(b, "AWS_SECRET_ACCESS_KEY", "minioadmin")
	setManifestPolicyMinIOEnv(b, "AWS_REGION", "us-east-1")

	bucketURL := os.Getenv("ISLEDB_MINIO_BUCKET_URL")
	if bucketURL == "" {
		bucketURL = defaultManifestPolicyMinIOURL
	}
	policies := []struct {
		active int
		fanout int
	}{
		{active: 32, fanout: 32},
		{active: 64, fanout: 32},
		{active: 128, fanout: 32},
		{active: 1_024, fanout: 1_024},
	}
	for _, policy := range policies {
		name := fmt.Sprintf("active=%d/fanout=%d", policy.active, policy.fanout)
		b.Run(name, func(b *testing.B) {
			benchmarkManifestPolicyLiveMinIO(b, bucketURL, policy.active, policy.fanout)
		})
	}
}

func benchmarkManifestPolicy(b *testing.B, activeLimit, pageFanout int) {
	var (
		totalHeadWrites int64
		totalHeadBytes  int64
		maxHeadBytes    int64
		totalPageWrites int64
		totalPageBytes  int64
	)

	b.ReportAllocs()
	for run := 0; run < b.N; run++ {
		b.StopTimer()
		store := blobstore.NewMemory(fmt.Sprintf("manifest-policy-%d-%d-%d", activeLimit, pageFanout, run))
		backend := NewBlobStoreBackend(store)
		measured := &manifestPolicyBenchmarkStorage{
			Storage:     backend,
			PageStorage: backend,
		}
		manifestStore := NewStoreWithStorage(measured)
		manifestStore.activeEntryLimit = activeLimit
		manifestStore.pageFanout = pageFanout
		manifestStore.maxCurrentBytes = int(^uint(0) >> 1)

		ctx := context.Background()
		if _, err := manifestStore.Replay(ctx); err != nil {
			b.Fatalf("replay: %v", err)
		}
		token, err := manifestStore.ClaimWriter(ctx, "benchmark-writer")
		if err != nil {
			b.Fatalf("claim writer: %v", err)
		}
		measured.resetMetrics()
		b.StartTimer()

		for commit := 0; commit < manifestPolicyBenchmarkCommits; commit++ {
			sst := benchmarkSSTMeta(commit, token.Epoch)
			if _, err := manifestStore.AppendAddSSTableWithFence(ctx, sst); err != nil {
				b.Fatalf("append commit %d: %v", commit, err)
			}
		}

		b.StopTimer()
		totalHeadWrites += measured.headWrites
		totalHeadBytes += measured.headBytes
		totalPageWrites += measured.pageWrites
		totalPageBytes += measured.pageBytes
		if measured.maxHeadBytes > maxHeadBytes {
			maxHeadBytes = measured.maxHeadBytes
		}
		if err := store.Close(); err != nil {
			b.Fatalf("close store: %v", err)
		}
	}

	commits := float64(b.N * manifestPolicyBenchmarkCommits)
	metadataBytes := totalHeadBytes + totalPageBytes
	b.ReportMetric(commits/b.Elapsed().Seconds(), "commits/s")
	b.ReportMetric(float64(totalHeadBytes)/commits, "head_B/commit")
	b.ReportMetric(float64(maxHeadBytes), "head_max_B")
	b.ReportMetric(float64(totalHeadWrites)/commits, "head_puts/commit")
	b.ReportMetric(float64(metadataBytes)/commits, "metadata_B/commit")
	b.ReportMetric(float64(totalPageBytes)/commits, "page_B/commit")
	b.ReportMetric(float64(totalPageWrites)*1_000/commits, "page_puts/1k-commits")
}

func benchmarkManifestPolicyLiveMinIO(b *testing.B, bucketURL string, activeLimit, pageFanout int) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	var (
		totalHeadWrites int64
		totalHeadBytes  int64
		maxHeadBytes    int64
		totalPageWrites int64
		totalPageBytes  int64
	)
	b.ReportAllocs()
	for run := 0; run < b.N; run++ {
		b.StopTimer()
		prefix := fmt.Sprintf("bench/manifest-policy-%d-%d-%d-%d", activeLimit, pageFanout, time.Now().UnixNano(), run)
		store, err := blobstore.Open(ctx, bucketURL, prefix)
		if err != nil {
			b.Fatalf("open MinIO store: %v", err)
		}
		backend := NewBlobStoreBackend(store)
		measured := &manifestPolicyBenchmarkStorage{
			Storage:     backend,
			PageStorage: backend,
		}
		manifestStore := NewStoreWithStorage(measured)
		manifestStore.activeEntryLimit = activeLimit
		manifestStore.pageFanout = pageFanout
		manifestStore.maxCurrentBytes = int(^uint(0) >> 1)
		if _, err := manifestStore.Replay(ctx); err != nil {
			_ = store.Close()
			b.Fatalf("replay: %v", err)
		}
		token, err := manifestStore.ClaimWriter(ctx, "benchmark-writer")
		if err != nil {
			_ = store.Close()
			b.Fatalf("claim writer: %v", err)
		}
		measured.resetMetrics()
		b.StartTimer()

		for commit := 0; commit < manifestPolicyLiveBenchmarkCommits; commit++ {
			sst := benchmarkSSTMeta(commit, token.Epoch)
			if _, err := manifestStore.AppendAddSSTableWithFence(ctx, sst); err != nil {
				b.Fatalf("append commit %d: %v", commit, err)
			}
		}

		b.StopTimer()
		totalHeadWrites += measured.headWrites
		totalHeadBytes += measured.headBytes
		totalPageWrites += measured.pageWrites
		totalPageBytes += measured.pageBytes
		if measured.maxHeadBytes > maxHeadBytes {
			maxHeadBytes = measured.maxHeadBytes
		}
		if err := store.Close(); err != nil {
			b.Fatalf("close store: %v", err)
		}
	}

	commits := float64(b.N * manifestPolicyLiveBenchmarkCommits)
	metadataBytes := totalHeadBytes + totalPageBytes
	b.ReportMetric(commits/b.Elapsed().Seconds(), "commits/s")
	b.ReportMetric(float64(totalHeadBytes)/commits, "head_B/commit")
	b.ReportMetric(float64(maxHeadBytes), "head_max_B")
	b.ReportMetric(float64(metadataBytes)/commits, "metadata_B/commit")
	b.ReportMetric(float64(totalPageWrites)*1_000/commits, "page_puts/1k-commits")
}

func setManifestPolicyMinIOEnv(b *testing.B, key, value string) {
	b.Helper()
	if os.Getenv(key) == "" {
		b.Setenv(key, value)
	}
}

func benchmarkSSTMeta(commit int, epoch uint64) SSTMeta {
	seqLo := uint64(commit) * 16_384
	seqHi := seqLo + 16_383
	return SSTMeta{
		ID:        fmt.Sprintf("%020d-%020d.sst", seqLo, seqHi),
		Epoch:     epoch,
		SeqLo:     seqLo,
		SeqHi:     seqHi,
		MinKey:    []byte(fmt.Sprintf("key:%020d", seqLo)),
		MaxKey:    []byte(fmt.Sprintf("key:%020d", seqHi)),
		Size:      16 << 20,
		Checksum:  "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		Bloom:     BloomMeta{BitsPerKey: 10, K: 7, Offset: 16 << 20, Length: 64 << 10},
		CreatedAt: time.Unix(int64(commit), 0).UTC(),
		Level:     0,
	}
}
