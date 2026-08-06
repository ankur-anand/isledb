package isledb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/manifest"
)

const (
	multiProcessRoleEnv      = "ISLEDB_MULTIPROCESS_WORKER_ROLE"
	multiProcessBucketEnv    = "ISLEDB_MULTIPROCESS_BUCKET_URL"
	multiProcessPrefixEnv    = "ISLEDB_MULTIPROCESS_PREFIX"
	multiProcessDurationEnv  = "ISLEDB_MULTIPROCESS_WORKER_DURATION"
	multiProcessSoakEnv      = "ISLEDB_MULTIPROCESS_SOAK"
	multiProcessResultPrefix = "ISLEDB_WORKER_RESULT records="
)

func TestOperationalRecovery_MultiProcessWriterMaintenanceSoak(t *testing.T) {
	duration := 2 * time.Second
	if raw := os.Getenv(multiProcessSoakEnv); raw != "" {
		parsed, err := time.ParseDuration(raw)
		if err != nil || parsed <= 0 {
			t.Fatalf("invalid %s=%q", multiProcessSoakEnv, raw)
		}
		duration = parsed
	}

	ctx, cancel := context.WithTimeout(context.Background(), duration+45*time.Second)
	defer cancel()
	bucketURL := setupFakeS3BucketURL(t)
	prefix := fmt.Sprintf("operational-multiprocess-%d", time.Now().UnixNano())

	maintenanceCmd, maintenanceOutput := newMultiProcessWorkerCommand(
		ctx, "maintenance", bucketURL, prefix, duration+time.Second)
	writerCmd, writerOutput := newMultiProcessWorkerCommand(
		ctx, "writer", bucketURL, prefix, duration)

	if err := maintenanceCmd.Start(); err != nil {
		t.Fatalf("start maintenance process: %v", err)
	}
	if err := writerCmd.Start(); err != nil {
		_ = maintenanceCmd.Process.Kill()
		_ = maintenanceCmd.Wait()
		t.Fatalf("start writer process: %v", err)
	}

	writerErr := writerCmd.Wait()
	maintenanceErr := maintenanceCmd.Wait()
	if writerErr != nil {
		t.Fatalf("writer process: %v\n%s", writerErr, writerOutput.String())
	}
	if maintenanceErr != nil {
		t.Fatalf("maintenance process: %v\n%s", maintenanceErr, maintenanceOutput.String())
	}

	records, err := parseMultiProcessWriterRecords(writerOutput.String())
	if err != nil {
		t.Fatalf("parse writer process output: %v\n%s", err, writerOutput.String())
	}
	if records == 0 {
		t.Fatal("writer process committed no records")
	}

	store, err := blobstore.Open(ctx, bucketURL, prefix)
	if err != nil {
		t.Fatalf("open verification store: %v", err)
	}
	defer store.Close()
	drainMultiProcessMaintenance(t, ctx, store)

	headStore := manifest.NewStore(store)
	head, _, err := headStore.ReadMaintenanceHead(ctx)
	if err != nil {
		t.Fatalf("read final maintenance HEAD: %v", err)
	}
	if head == nil || head.Pending != nil {
		t.Fatalf("final maintenance HEAD=%+v, want no pending command", head)
	}

	reader := openReaderFromDBForTest(t, ctx, store, DefaultReaderOpenOptions(t.TempDir()))
	defer reader.Close()
	rows, err := reader.Scan(ctx, []byte("mp-key-"), []byte("mp-key."))
	if err != nil {
		t.Fatalf("scan final records: %v", err)
	}
	if len(rows) != records {
		t.Fatalf("final scan records=%d, want=%d", len(rows), records)
	}
	for i, row := range rows {
		wantKey := fmt.Sprintf("mp-key-%08d", i)
		wantValue := fmt.Sprintf("mp-value-%08d", i)
		if string(row.Key) != wantKey || string(row.Value) != wantValue {
			t.Fatalf("row %d=(%q,%q), want=(%q,%q)",
				i, row.Key, row.Value, wantKey, wantValue)
		}
	}

	manifestState := replayManifestForTest(t, ctx, store)
	if len(manifestState.Levels) == 0 {
		t.Fatalf("multi-process maintenance produced no compacted level: l0=%d", manifestState.L0SSTCount())
	}
	assertOperationalStorageHealthy(t, ctx, store)
	t.Logf("multi-process soak duration=%s records=%d l0=%d levels=%d",
		duration, records, manifestState.L0SSTCount(), len(manifestState.Levels))
}

func newMultiProcessWorkerCommand(
	ctx context.Context,
	role, bucketURL, prefix string,
	duration time.Duration,
) (*exec.Cmd, *bytes.Buffer) {
	cmd := exec.CommandContext(ctx, os.Args[0],
		"-test.run=^TestOperationalRecovery_MultiProcessWorker$",
		"-test.count=1",
	)
	cmd.Env = append(os.Environ(),
		multiProcessRoleEnv+"="+role,
		multiProcessBucketEnv+"="+bucketURL,
		multiProcessPrefixEnv+"="+prefix,
		multiProcessDurationEnv+"="+duration.String(),
	)
	output := &bytes.Buffer{}
	cmd.Stdout = output
	cmd.Stderr = output
	return cmd, output
}

func TestOperationalRecovery_MultiProcessWorker(t *testing.T) {
	role := os.Getenv(multiProcessRoleEnv)
	if role == "" {
		t.Skip("subprocess helper")
	}
	duration, err := time.ParseDuration(os.Getenv(multiProcessDurationEnv))
	if err != nil || duration <= 0 {
		t.Fatalf("invalid worker duration: %q", os.Getenv(multiProcessDurationEnv))
	}

	ctx, cancel := context.WithTimeout(context.Background(), duration+15*time.Second)
	defer cancel()
	store, err := blobstore.Open(ctx, os.Getenv(multiProcessBucketEnv), os.Getenv(multiProcessPrefixEnv))
	if err != nil {
		t.Fatalf("open worker store: %v", err)
	}
	defer store.Close()

	switch role {
	case "writer":
		runMultiProcessWriter(t, ctx, store, duration)
	case "maintenance":
		runMultiProcessMaintenance(t, ctx, store, duration)
	default:
		t.Fatalf("unknown worker role %q", role)
	}
}

func runMultiProcessWriter(t testing.TB, parent context.Context, store *blobstore.Store, duration time.Duration) {
	t.Helper()
	db, err := OpenDB(parent, store, DBOptions{})
	if err != nil {
		t.Fatalf("writer OpenDB: %v", err)
	}
	defer db.Close()

	opts := DefaultWriterOptions()
	opts.OwnerID = "multiprocess-writer"
	opts.Flush.Interval = 0
	opts.Maintenance.PollInterval = 5 * time.Millisecond
	opts.Memtable.TargetBytes = 4 << 10
	opts.SST.BlockBytes = 1024
	opts.SST.Compression = "none"
	writer, err := db.OpenWriter(parent, opts)
	if err != nil {
		t.Fatalf("writer OpenWriter: %v", err)
	}

	deadline := time.Now().Add(duration)
	records := 0
	for time.Now().Before(deadline) {
		for range 8 {
			key := fmt.Sprintf("mp-key-%08d", records)
			value := fmt.Sprintf("mp-value-%08d", records)
			if err := writer.Put(parent, []byte(key), []byte(value)); err != nil {
				t.Fatalf("writer Put(%d): %v", records, err)
			}
			records++
		}
		if err := writer.Flush(parent); err != nil {
			t.Fatalf("writer Flush(%d): %v", records, err)
		}
		time.Sleep(2 * time.Millisecond)
	}
	if err := writer.Close(parent); err != nil {
		t.Fatalf("writer Close: %v", err)
	}
	fmt.Printf("%s%d\n", multiProcessResultPrefix, records)
}

func runMultiProcessMaintenance(t testing.TB, parent context.Context, store *blobstore.Store, duration time.Duration) {
	t.Helper()
	db, err := OpenDB(parent, store, DBOptions{})
	if err != nil {
		t.Fatalf("maintenance OpenDB: %v", err)
	}
	defer db.Close()

	opts := DefaultMaintenanceOptions()
	opts.OwnerID = "multiprocess-maintenance"
	opts.Every = 5 * time.Millisecond
	opts.Compaction.L0SSTCount = 4
	opts.Compaction.BaseLevelBytes = 1 << 60
	opts.Compaction.TargetSSTBytes = 32 << 10
	opts.Compaction.BlockBytes = 1024
	opts.Compaction.Compression = "none"
	opts.GarbageCollection.GracePeriod = time.Nanosecond
	opts.GarbageCollection.DeleteBatchSize = manifest.MaxRetiredObjectsPerEntry
	maintenance, err := db.OpenMaintenance(parent, opts)
	if err != nil {
		t.Fatalf("maintenance OpenMaintenance: %v", err)
	}

	runCtx, cancel := context.WithTimeout(parent, duration)
	err = maintenance.Run(runCtx)
	cancel()
	if err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Fatalf("maintenance Run: %v", err)
	}
	closeCtx, closeCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer closeCancel()
	if err := maintenance.Close(closeCtx); err != nil {
		t.Fatalf("maintenance Close: %v", err)
	}
}

func drainMultiProcessMaintenance(t testing.TB, ctx context.Context, store *blobstore.Store) {
	t.Helper()
	db, err := OpenDB(ctx, store, DBOptions{})
	if err != nil {
		t.Fatalf("drain OpenDB: %v", err)
	}
	defer db.Close()

	writerOpts := DefaultWriterOptions()
	writerOpts.OwnerID = "multiprocess-drain-writer"
	writerOpts.Flush.Interval = 0
	writerOpts.SST.Compression = "none"
	writer, err := db.OpenWriter(ctx, writerOpts)
	if err != nil {
		t.Fatalf("drain OpenWriter: %v", err)
	}

	maintenanceOpts := DefaultMaintenanceOptions()
	maintenanceOpts.OwnerID = "multiprocess-drain-maintenance"
	maintenanceOpts.Compaction.L0SSTCount = 4
	maintenanceOpts.Compaction.BaseLevelBytes = 1 << 60
	maintenanceOpts.Compaction.TargetSSTBytes = 32 << 10
	maintenanceOpts.Compaction.BlockBytes = 1024
	maintenanceOpts.Compaction.Compression = "none"
	maintenanceOpts.GarbageCollection.GracePeriod = time.Nanosecond
	maintenanceOpts.GarbageCollection.DeleteBatchSize = manifest.MaxRetiredObjectsPerEntry
	maintenance, err := db.OpenMaintenance(ctx, maintenanceOpts)
	if err != nil {
		t.Fatalf("drain OpenMaintenance: %v", err)
	}

	driveMaintenanceToIdle(t, ctx, maintenance, writer)
	if err := maintenance.Close(ctx); err != nil {
		t.Fatalf("drain maintenance Close: %v", err)
	}
	if err := writer.Close(ctx); err != nil {
		t.Fatalf("drain writer Close: %v", err)
	}
}

func parseMultiProcessWriterRecords(output string) (int, error) {
	index := strings.LastIndex(output, multiProcessResultPrefix)
	if index < 0 {
		return 0, errors.New("worker result marker not found")
	}
	rest := output[index+len(multiProcessResultPrefix):]
	line, _, _ := strings.Cut(rest, "\n")
	records, err := strconv.Atoi(strings.TrimSpace(line))
	if err != nil {
		return 0, fmt.Errorf("parse record count %q: %w", line, err)
	}
	return records, nil
}
