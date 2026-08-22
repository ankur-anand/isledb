//go:build integration

package storageengine

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	isledb "github.com/ankur-anand/isledb"
)

const (
	storageWorkerRoleEnv      = "ISLEDB_STORAGE_TEST_ROLE"
	storageWorkerBucketEnv    = "ISLEDB_STORAGE_TEST_BUCKET_URL"
	storageWorkerPrefixEnv    = "ISLEDB_STORAGE_TEST_PREFIX"
	storageWorkerCacheDirEnv  = "ISLEDB_STORAGE_TEST_CACHE_DIR"
	storageWorkerFeedEnv      = "ISLEDB_STORAGE_TEST_CHANGE_FEED"
	storageWorkerEventPrefix  = "ISLEDB_STORAGE_EVENT "
	storageWorkerTestName     = "TestStorageEngineWorkerProcess"
	storageSentinelKey        = "sentinel/stable"
	storageSentinelValue      = "stable-value"
	storageGenerationKeyCount = 96
	storageMinimumGenerations = 4
	storageMaximumGenerations = 16
	storagePostCompactions    = 3
)

type storageWorkerCommand struct {
	Type       string `json:"type"`
	Generation int    `json:"generation,omitempty"`
	Target     int    `json:"target,omitempty"`
	Consumed   int    `json:"consumed,omitempty"`
	Cursor     string `json:"cursor,omitempty"`
}

type storageWorkerEvent struct {
	Role        string `json:"role"`
	Type        string `json:"type"`
	Generation  int    `json:"generation,omitempty"`
	Target      int    `json:"target,omitempty"`
	Checks      int    `json:"checks,omitempty"`
	Consumed    int    `json:"consumed,omitempty"`
	Cursor      string `json:"cursor,omitempty"`
	Payload     string `json:"payload,omitempty"`
	Sequence    uint64 `json:"sequence,omitempty"`
	State       string `json:"state,omitempty"`
	Selected    string `json:"selected,omitempty"`
	Jobs        int    `json:"jobs,omitempty"`
	InputSSTs   int    `json:"input_ssts,omitempty"`
	OutputSSTs  int    `json:"output_ssts,omitempty"`
	OutputBytes int64  `json:"output_bytes,omitempty"`
	Message     string `json:"message,omitempty"`
}

type storageProcessEvent struct {
	worker *storageWorkerProcess
	event  storageWorkerEvent
	err    error
	exited bool
}

type storageWorkerReporter struct {
	role string
	mu   sync.Mutex
}

func (r *storageWorkerReporter) emit(event storageWorkerEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	event.Role = r.role
	data, err := json.Marshal(event)
	if err != nil {
		fmt.Fprintf(os.Stderr, "encode storage integration event: %v\n", err)
		return
	}
	fmt.Printf("%s%s\n", storageWorkerEventPrefix, data)
}

// TestStorageEngineWorkerProcess is re-executed by the parent test with one
// role selected through the environment. Each role opens its own DB and thus
// has an independent address space, object-store client, fences, and cache.
func TestStorageEngineWorkerProcess(t *testing.T) {
	role := os.Getenv(storageWorkerRoleEnv)
	if role == "" {
		t.Skip("storage-engine worker helper")
	}
	bucketURL := os.Getenv(storageWorkerBucketEnv)
	prefix := os.Getenv(storageWorkerPrefixEnv)
	payload, err := storageChangeFeedPayload(os.Getenv(storageWorkerFeedEnv))
	if bucketURL == "" || prefix == "" || err != nil {
		t.Fatalf("worker %s invalid configuration: bucket=%t prefix=%t change_feed=%q error=%v",
			role, bucketURL != "", prefix != "", os.Getenv(storageWorkerFeedEnv), err)
	}

	reporter := &storageWorkerReporter{role: role}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	err = nil
	switch role {
	case "writer":
		err = runStorageWriterWorker(ctx, bucketURL, prefix, reporter, os.Stdin)
	case "maintenance":
		err = runStorageMaintenanceWorker(ctx, bucketURL, prefix, reporter, os.Stdin)
	case "reader-live", "reader-cold":
		cacheDir := os.Getenv(storageWorkerCacheDirEnv)
		if cacheDir == "" {
			err = errors.New("reader worker missing cache directory")
		} else {
			err = runStorageReaderWorker(ctx, bucketURL, prefix, cacheDir, reporter, os.Stdin)
		}
	case "change-live", "change-resume":
		err = runStorageChangeReaderWorker(ctx, bucketURL, prefix, payload, reporter, os.Stdin)
	default:
		err = fmt.Errorf("unknown storage worker role %q", role)
	}
	if err != nil {
		reporter.emit(storageWorkerEvent{Type: "error", Message: err.Error()})
		t.Fatal(err)
	}
}

func storageChangeFeedPayload(value string) (isledb.ChangeFeedPayload, error) {
	switch value {
	case isledb.ChangeFeedKeysOnly.String():
		return isledb.ChangeFeedKeysOnly, nil
	case isledb.ChangeFeedFullValues.String():
		return isledb.ChangeFeedFullValues, nil
	default:
		return 0, fmt.Errorf("unsupported change-feed payload %q", value)
	}
}

func openStorageWorkerDB(
	ctx context.Context,
	bucketURL string,
	prefix string,
) (*isledb.DB, error) {
	payload, err := storageChangeFeedPayload(os.Getenv(storageWorkerFeedEnv))
	if err != nil {
		return nil, err
	}
	return isledb.Open(ctx, bucketURL, isledb.DBOptions{
		Prefix: prefix,
		ChangeFeed: &isledb.ChangeFeedOptions{
			Payload: payload,
		},
	})
}

func runStorageWriterWorker(
	ctx context.Context,
	bucketURL string,
	prefix string,
	reporter *storageWorkerReporter,
	commands io.Reader,
) error {
	db, err := openStorageWorkerDB(ctx, bucketURL, prefix)
	if err != nil {
		return fmt.Errorf("open writer DB: %w", err)
	}
	dbClosed := false
	defer func() {
		if !dbClosed {
			_ = db.Close()
		}
	}()

	opts := isledb.DefaultWriterOptions()
	opts.OwnerID = "storage-integration-writer"
	// Separate processes do not share DB.maintenanceWake. A background flush
	// tick is therefore required for the Writer to poll maintenance/HEAD after
	// the mutation workload becomes idle.
	opts.Flush.Interval = 100 * time.Millisecond
	opts.Maintenance.PollInterval = 100 * time.Millisecond
	writer, err := db.OpenWriter(ctx, opts)
	if err != nil {
		return fmt.Errorf("open writer: %w", err)
	}
	writerClosed := false
	defer func() {
		if !writerClosed {
			closeCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			_ = writer.Close(closeCtx)
		}
	}()

	reporter.emit(storageWorkerEvent{Type: "ready"})
	decoder := json.NewDecoder(commands)
	for {
		var command storageWorkerCommand
		if err := decoder.Decode(&command); err != nil {
			if errors.Is(err, io.EOF) {
				return errors.New("writer command stream closed before close command")
			}
			return fmt.Errorf("decode writer command: %w", err)
		}
		switch command.Type {
		case "seed":
			if err := applyStorageMutations(ctx, writer, storageSeedMutations(), "seed"); err != nil {
				return err
			}
			if err := writer.Flush(ctx); err != nil {
				return fmt.Errorf("flush sentinel: %w", err)
			}
			reporter.emit(storageWorkerEvent{Type: "seed_flushed"})
		case "write_generation":
			if command.Generation <= 0 {
				return fmt.Errorf("invalid generation %d", command.Generation)
			}
			if err := applyStorageGeneration(ctx, writer, command.Generation); err != nil {
				return err
			}
			if err := writer.Flush(ctx); err != nil {
				return fmt.Errorf("flush generation %d: %w", command.Generation, err)
			}
			reporter.emit(storageWorkerEvent{
				Type:       "generation_flushed",
				Generation: command.Generation,
			})
		case "finish_writes":
			reporter.emit(storageWorkerEvent{Type: "writes_finished"})
		case "close":
			closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			err := writer.Close(closeCtx)
			cancel()
			if err != nil {
				return fmt.Errorf("close writer: %w", err)
			}
			writerClosed = true
			if err := db.Close(); err != nil {
				return fmt.Errorf("close writer DB: %w", err)
			}
			dbClosed = true
			reporter.emit(storageWorkerEvent{Type: "closed"})
			return nil
		default:
			return fmt.Errorf("unknown writer command %q", command.Type)
		}
	}
}

func runStorageMaintenanceWorker(
	ctx context.Context,
	bucketURL string,
	prefix string,
	reporter *storageWorkerReporter,
	commands io.Reader,
) error {
	db, err := openStorageWorkerDB(ctx, bucketURL, prefix)
	if err != nil {
		return fmt.Errorf("open maintenance DB: %w", err)
	}
	dbClosed := false
	defer func() {
		if !dbClosed {
			_ = db.Close()
		}
	}()

	var cycleSequence atomic.Uint64
	opts := isledb.DefaultMaintenanceOptions()
	opts.IdleInterval = 10 * time.Millisecond
	opts.SSTCompaction.L0TriggerSSTs = 3
	opts.SSTCompaction.BaseLevelBytes = 1 << 60
	opts.SSTCompaction.TargetSSTBytes = 64 << 10
	opts.OnCycle = func(stats isledb.MaintenanceStats) {
		sequence := cycleSequence.Add(1)
		reporter.emit(storageWorkerEvent{
			Type:        "maintenance_cycle",
			Sequence:    sequence,
			State:       stats.State.String(),
			Selected:    stats.Scheduling.Selected.String(),
			Jobs:        stats.SSTCompaction.Jobs,
			InputSSTs:   stats.SSTCompaction.InputSSTs,
			OutputSSTs:  stats.SSTCompaction.OutputSSTs,
			OutputBytes: stats.SSTCompaction.OutputBytes,
		})
	}
	opts.OnError = func(err error) {
		reporter.emit(storageWorkerEvent{Type: "error", Message: "maintenance cycle: " + err.Error()})
	}
	maintenance, err := db.OpenMaintenance(ctx, opts)
	if err != nil {
		return fmt.Errorf("open maintenance: %w", err)
	}
	maintenanceClosed := false
	defer func() {
		if !maintenanceClosed {
			closeCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			_ = maintenance.Close(closeCtx)
		}
	}()

	commandCh, commandErrCh := decodeStorageWorkerCommands(commands)
	runDone := make(chan error, 1)
	go func() {
		runDone <- maintenance.Run(ctx)
	}()
	reporter.emit(storageWorkerEvent{Type: "ready"})

	for {
		select {
		case command, ok := <-commandCh:
			if !ok {
				commandCh = nil
				continue
			}
			switch command.Type {
			case "barrier":
				reporter.emit(storageWorkerEvent{
					Type:     "maintenance_barrier",
					Sequence: cycleSequence.Load(),
				})
			case "close":
				closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				closeErr := maintenance.Close(closeCtx)
				cancel()
				maintenanceClosed = closeErr == nil
				runErr := <-runDone
				if closeErr != nil || runErr != nil {
					return errors.Join(
						wrapStorageError("close maintenance", closeErr),
						wrapStorageError("maintenance run", runErr),
					)
				}
				if err := db.Close(); err != nil {
					return fmt.Errorf("close maintenance DB: %w", err)
				}
				dbClosed = true
				reporter.emit(storageWorkerEvent{Type: "closed"})
				return nil
			default:
				return fmt.Errorf("unknown maintenance command %q", command.Type)
			}
		case err := <-commandErrCh:
			if err == nil {
				commandErrCh = nil
				continue
			}
			return fmt.Errorf("decode maintenance command: %w", err)
		case err := <-runDone:
			return fmt.Errorf("maintenance Run exited before close: %w", err)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func runStorageReaderWorker(
	ctx context.Context,
	bucketURL string,
	prefix string,
	cacheDir string,
	reporter *storageWorkerReporter,
	commands io.Reader,
) error {
	db, err := openStorageWorkerDB(ctx, bucketURL, prefix)
	if err != nil {
		return fmt.Errorf("open reader DB: %w", err)
	}
	dbClosed := false
	defer func() {
		if !dbClosed {
			_ = db.Close()
		}
	}()

	opts := isledb.DefaultReaderOpenOptions(cacheDir)
	reader, err := db.OpenReader(ctx, opts)
	if err != nil {
		return fmt.Errorf("open reader: %w", err)
	}
	readerClosed := false
	defer func() {
		if !readerClosed {
			_ = reader.Close()
		}
	}()

	commandCh, commandErrCh := decodeStorageWorkerCommands(commands)
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	checks := 0
	reporter.emit(storageWorkerEvent{Type: "ready"})

	for {
		select {
		case <-ticker.C:
			if err := verifyStorageSentinel(ctx, reader); err != nil {
				return err
			}
			checks++
			reporter.emit(storageWorkerEvent{Type: "sentinel_verified", Checks: checks})
		case command, ok := <-commandCh:
			if !ok {
				commandCh = nil
				continue
			}
			switch command.Type {
			case "verify_final":
				if command.Generation <= 0 {
					return fmt.Errorf("invalid final generation %d", command.Generation)
				}
				if err := verifyStorageState(ctx, reader, command.Generation); err != nil {
					return err
				}
				reporter.emit(storageWorkerEvent{
					Type:       "final_state_verified",
					Generation: command.Generation,
					Checks:     checks,
				})
			case "close":
				if err := reader.Close(); err != nil {
					return fmt.Errorf("close reader: %w", err)
				}
				readerClosed = true
				if err := db.Close(); err != nil {
					return fmt.Errorf("close reader DB: %w", err)
				}
				dbClosed = true
				reporter.emit(storageWorkerEvent{Type: "closed"})
				return nil
			default:
				return fmt.Errorf("unknown reader command %q", command.Type)
			}
		case err := <-commandErrCh:
			if err == nil {
				commandErrCh = nil
				continue
			}
			return fmt.Errorf("decode reader command: %w", err)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func runStorageChangeReaderWorker(
	ctx context.Context,
	bucketURL string,
	prefix string,
	payload isledb.ChangeFeedPayload,
	reporter *storageWorkerReporter,
	commands io.Reader,
) error {
	db, err := openStorageWorkerDB(ctx, bucketURL, prefix)
	if err != nil {
		return fmt.Errorf("open change-reader DB: %w", err)
	}
	dbClosed := false
	defer func() {
		if !dbClosed {
			_ = db.Close()
		}
	}()

	reader, err := db.OpenChangeReader(ctx)
	if err != nil {
		return fmt.Errorf("open change reader: %w", err)
	}
	readerClosed := false
	defer func() {
		if !readerClosed {
			_ = reader.Close()
		}
	}()
	bounds, err := reader.Bounds(ctx)
	if err != nil {
		return fmt.Errorf("read change bounds: %w", err)
	}
	if bounds.Payload != payload {
		return fmt.Errorf("change payload=%s, want=%s", bounds.Payload, payload)
	}

	cursor := isledb.ChangeCursor{}
	consumed := 0
	reporter.emit(storageWorkerEvent{Type: "ready", Payload: payload.String()})
	decoder := json.NewDecoder(commands)
	for {
		var command storageWorkerCommand
		if err := decoder.Decode(&command); err != nil {
			if errors.Is(err, io.EOF) {
				return errors.New("change-reader command stream closed before close command")
			}
			return fmt.Errorf("decode change-reader command: %w", err)
		}
		switch command.Type {
		case "resume":
			if command.Consumed < 0 || (command.Consumed > 0 && command.Cursor == "") {
				return fmt.Errorf(
					"invalid resume consumed=%d cursor=%q", command.Consumed, command.Cursor)
			}
			parsed, err := isledb.ParseChangeCursor(command.Cursor)
			if err != nil {
				return fmt.Errorf("parse resume cursor: %w", err)
			}
			cursor = parsed
			consumed = command.Consumed
			reporter.emit(storageWorkerEvent{
				Type:     "change_resumed",
				Consumed: consumed,
				Cursor:   cursor.String(),
				Payload:  payload.String(),
			})
		case "consume_until":
			if command.Target < consumed {
				return fmt.Errorf("change target=%d is behind consumed=%d", command.Target, consumed)
			}
			cursor, consumed, err = consumeStorageChanges(
				ctx, reader, payload, cursor, consumed, command.Target, func() {
					reporter.emit(storageWorkerEvent{
						Type:    "change_waiting",
						Target:  command.Target,
						Payload: payload.String(),
					})
				})
			if err != nil {
				return err
			}
			reporter.emit(storageWorkerEvent{
				Type:     "changes_consumed",
				Consumed: consumed,
				Cursor:   cursor.String(),
				Payload:  payload.String(),
			})
		case "verify_head":
			page, err := reader.Read(ctx, cursor, isledb.ChangeReadOptions{
				MaxChanges: 11,
				MaxBytes:   8 << 10,
			})
			if err != nil {
				return fmt.Errorf("verify change-feed head: %w", err)
			}
			if len(page.Changes) != 0 || !page.CaughtUp() {
				return fmt.Errorf(
					"change feed not caught up: changes=%d next=%q head=%q",
					len(page.Changes), page.Next.String(), page.Head.String())
			}
			bounds, err := reader.Bounds(ctx)
			if err != nil {
				return fmt.Errorf("verify final change bounds: %w", err)
			}
			if bounds.Payload != payload || page.Next != bounds.Head {
				return fmt.Errorf(
					"final change bounds payload=%s next=%q head=%q",
					bounds.Payload, page.Next.String(), bounds.Head.String())
			}
			reporter.emit(storageWorkerEvent{
				Type:     "change_feed_verified",
				Consumed: consumed,
				Cursor:   cursor.String(),
				Payload:  payload.String(),
			})
		case "close":
			if err := reader.Close(); err != nil {
				return fmt.Errorf("close change reader: %w", err)
			}
			readerClosed = true
			if err := db.Close(); err != nil {
				return fmt.Errorf("close change-reader DB: %w", err)
			}
			dbClosed = true
			reporter.emit(storageWorkerEvent{Type: "closed"})
			return nil
		default:
			return fmt.Errorf("unknown change-reader command %q", command.Type)
		}
	}
}

func consumeStorageChanges(
	ctx context.Context,
	reader *isledb.ChangeReader,
	payload isledb.ChangeFeedPayload,
	cursor isledb.ChangeCursor,
	consumed int,
	target int,
	onWaiting func(),
) (isledb.ChangeCursor, int, error) {
	expected := expectedStorageChanges(storageMaximumGenerations + storagePostCompactions)
	if target > len(expected) {
		return cursor, consumed, fmt.Errorf("change target=%d exceeds expected=%d", target, len(expected))
	}
	waitingReported := false
	for consumed < target {
		page, err := reader.Read(ctx, cursor, isledb.ChangeReadOptions{
			MaxChanges: 11,
			MaxBytes:   8 << 10,
		})
		if err != nil {
			return cursor, consumed, fmt.Errorf("read changes at %d: %w", consumed, err)
		}
		if consumed+len(page.Changes) > target {
			return cursor, consumed, fmt.Errorf(
				"change page crossed target: consumed=%d page=%d target=%d",
				consumed, len(page.Changes), target)
		}
		for _, change := range page.Changes {
			if err := verifyStorageChange(change, expected[consumed], consumed, payload); err != nil {
				return cursor, consumed, err
			}
			consumed++
		}
		encoded := page.Next.String()
		parsed, err := isledb.ParseChangeCursor(encoded)
		if err != nil {
			return cursor, consumed, fmt.Errorf("parse page cursor %q: %w", encoded, err)
		}
		cursor = parsed
		if consumed >= target {
			break
		}
		if len(page.Changes) == 0 && !page.CaughtUp() {
			return cursor, consumed, errors.New("change reader made no progress before head")
		}
		if page.CaughtUp() {
			if !waitingReported && onWaiting != nil {
				onWaiting()
				waitingReported = true
			}
			timer := time.NewTimer(20 * time.Millisecond)
			select {
			case <-timer.C:
			case <-ctx.Done():
				stopStorageProviderTimer(timer)
				return cursor, consumed, ctx.Err()
			}
		}
	}
	return cursor, consumed, nil
}

func verifyStorageChange(
	change isledb.Change,
	expected storageMutation,
	index int,
	payload isledb.ChangeFeedPayload,
) error {
	wantSequence := uint64(index + 1)
	if change.Sequence != wantSequence || change.Operation != expected.operation ||
		!bytes.Equal(change.Key, expected.key) {
		return fmt.Errorf(
			"change[%d]=(seq=%d op=%s key=%x), want=(seq=%d op=%s key=%x)",
			index, change.Sequence, change.Operation, change.Key,
			wantSequence, expected.operation, expected.key)
	}
	if expected.ttl > 0 {
		if change.ExpiresAt.IsZero() {
			return fmt.Errorf("change[%d] TTL expiration is missing", index)
		}
	} else if !change.ExpiresAt.IsZero() {
		return fmt.Errorf("change[%d] has unexpected expiration %s", index, change.ExpiresAt)
	}
	if payload == isledb.ChangeFeedKeysOnly {
		if change.HasValue || change.Value != nil {
			return fmt.Errorf(
				"keys-only change[%d] exposed has_value=%t value_bytes=%d",
				index, change.HasValue, len(change.Value))
		}
		return nil
	}
	if expected.operation == isledb.ChangeDelete {
		if change.HasValue || change.Value != nil {
			return fmt.Errorf("delete change[%d] exposed a value", index)
		}
		return nil
	}
	if !change.HasValue || change.Value == nil || !bytes.Equal(change.Value, expected.value) {
		return fmt.Errorf(
			"full-value change[%d] has_value=%t value=%x, want=%x",
			index, change.HasValue, change.Value, expected.value)
	}
	return nil
}

func decodeStorageWorkerCommands(input io.Reader) (<-chan storageWorkerCommand, <-chan error) {
	commands := make(chan storageWorkerCommand)
	errorsCh := make(chan error, 1)
	go func() {
		defer close(commands)
		defer close(errorsCh)
		decoder := json.NewDecoder(input)
		for {
			var command storageWorkerCommand
			if err := decoder.Decode(&command); err != nil {
				if !errors.Is(err, io.EOF) {
					errorsCh <- err
				}
				return
			}
			commands <- command
		}
	}()
	return commands, errorsCh
}

type storageMutation struct {
	operation isledb.ChangeOperation
	key       []byte
	value     []byte
	ttl       time.Duration
}

func storageSeedMutations() []storageMutation {
	return []storageMutation{
		{
			operation: isledb.ChangePut,
			key:       []byte(storageSentinelKey),
			value:     []byte(storageSentinelValue),
		},
		{
			operation: isledb.ChangePut,
			key:       []byte("ttl/stable"),
			value:     []byte("ttl-value"),
			ttl:       time.Hour,
		},
	}
}

func storageGenerationMutations(generation int) []storageMutation {
	mutations := make([]storageMutation, 0, storageGenerationKeyCount+20)
	if generation == 1 {
		for index, key := range storageOrderingKeys() {
			mutations = append(mutations, storageMutation{
				operation: isledb.ChangePut,
				key:       append([]byte(nil), key...),
				value:     fmt.Appendf(nil, "ordered-%02d", index),
			})
		}
	}
	for index := 0; index < storageGenerationKeyCount; index++ {
		key := storageGenerationKey(index)
		if storageGenerationDeletes(generation, index) {
			mutations = append(mutations, storageMutation{
				operation: isledb.ChangeDelete,
				key:       key,
			})
			continue
		}
		mutations = append(mutations, storageMutation{
			operation: isledb.ChangePut,
			key:       key,
			value:     storageGenerationValue(generation, index),
		})
	}

	// These mutations exercise sequence ordering both across flushed SSTs and
	// within one memtable. Only the final Put may remain visible.
	versionedKey := []byte("ordering/versioned")
	mutations = append(mutations,
		storageMutation{
			operation: isledb.ChangePut,
			key:       versionedKey,
			value:     fmt.Appendf(nil, "generation-%d-intermediate", generation),
		},
		storageMutation{operation: isledb.ChangeDelete, key: versionedKey},
		storageMutation{
			operation: isledb.ChangePut,
			key:       versionedKey,
			value:     fmt.Appendf(nil, "generation-%d-final", generation),
		},
	)
	// This key is repeatedly written and deleted. Its tombstone must prevent
	// every older value from being resurrected by compaction.
	tombstoneKey := []byte("ordering/tombstone")
	mutations = append(mutations,
		storageMutation{
			operation: isledb.ChangePut,
			key:       tombstoneKey,
			value:     fmt.Appendf(nil, "generation-%d-deleted", generation),
		},
		storageMutation{operation: isledb.ChangeDelete, key: tombstoneKey},
	)
	return mutations
}

func applyStorageGeneration(ctx context.Context, writer *isledb.Writer, generation int) error {
	return applyStorageMutations(
		ctx, writer, storageGenerationMutations(generation), fmt.Sprintf("generation %d", generation))
}

func applyStorageMutations(
	ctx context.Context,
	writer *isledb.Writer,
	mutations []storageMutation,
	label string,
) error {
	for _, mutation := range mutations {
		var err error
		switch mutation.operation {
		case isledb.ChangePut:
			if mutation.ttl > 0 {
				err = writer.PutWithTTL(ctx, mutation.key, mutation.value, mutation.ttl)
			} else {
				err = writer.Put(ctx, mutation.key, mutation.value)
			}
		case isledb.ChangeDelete:
			err = writer.Delete(ctx, mutation.key)
		default:
			err = fmt.Errorf("unsupported operation %s", mutation.operation)
		}
		if err != nil {
			return fmt.Errorf("%s %s %x: %w", label, mutation.operation, mutation.key, err)
		}
	}
	return nil
}

func storageOrderingKeys() [][]byte {
	return [][]byte{
		{0x00},
		{0x00, 0xff},
		{'a'},
		{'a', 0x00},
		{'a', 'a'},
		{'a', 0xff},
		{'b'},
		{0x7f},
		{0x80},
		{0xff},
	}
}

func storageGenerationKey(index int) []byte {
	return fmt.Appendf(nil, "records/%03d", index)
}

func storageGenerationDeletes(generation, index int) bool {
	return generation > 1 && (index+generation*3)%13 == 0
}

func storageGenerationValue(generation, index int) []byte {
	if (generation+index)%37 == 0 {
		return []byte{}
	}
	size := 256 + (index%5)*193
	value := make([]byte, 0, size)
	for block := 0; len(value) < size; block++ {
		digest := sha256.Sum256([]byte(
			strconv.Itoa(generation) + "/" + strconv.Itoa(index) + "/" + strconv.Itoa(block)))
		value = append(value, digest[:]...)
	}
	return value[:size]
}

func expectedStorageState(lastGeneration int) map[string][]byte {
	expected := make(map[string][]byte)
	applyExpectedStorageMutations(expected, storageSeedMutations())
	for generation := 1; generation <= lastGeneration; generation++ {
		applyExpectedStorageMutations(expected, storageGenerationMutations(generation))
	}
	return expected
}

func applyExpectedStorageMutations(expected map[string][]byte, mutations []storageMutation) {
	for _, mutation := range mutations {
		key := string(mutation.key)
		if mutation.operation == isledb.ChangeDelete {
			delete(expected, key)
			continue
		}
		expected[key] = append([]byte(nil), mutation.value...)
	}
}

func expectedStorageChanges(lastGeneration int) []storageMutation {
	expected := append([]storageMutation(nil), storageSeedMutations()...)
	for generation := 1; generation <= lastGeneration; generation++ {
		expected = append(expected, storageGenerationMutations(generation)...)
	}
	return expected
}

func verifyStorageSentinel(ctx context.Context, reader *isledb.Reader) error {
	if err := reader.Refresh(ctx); err != nil {
		return fmt.Errorf("refresh reader for sentinel: %w", err)
	}
	value, found, err := reader.Get(ctx, []byte(storageSentinelKey))
	if err != nil {
		return fmt.Errorf("read sentinel: %w", err)
	}
	if !found || !bytes.Equal(value, []byte(storageSentinelValue)) {
		return fmt.Errorf("sentinel value=%q found=%t, want %q", value, found, storageSentinelValue)
	}
	return nil
}

func verifyStorageState(ctx context.Context, reader *isledb.Reader, lastGeneration int) error {
	if err := reader.Refresh(ctx); err != nil {
		return fmt.Errorf("refresh final reader: %w", err)
	}
	expected := expectedStorageState(lastGeneration)
	for key, want := range expected {
		value, found, err := reader.Get(ctx, []byte(key))
		if err != nil {
			return fmt.Errorf("Get(%q): %w", key, err)
		}
		if !found || !bytes.Equal(value, want) {
			return fmt.Errorf("Get(%q) value=%x found=%t, want=%x", key, value, found, want)
		}
	}
	for index := 0; index < storageGenerationKeyCount; index++ {
		key := storageGenerationKey(index)
		if _, exists := expected[string(key)]; exists {
			continue
		}
		value, found, err := reader.Get(ctx, key)
		if err != nil {
			return fmt.Errorf("Get deleted key %q: %w", key, err)
		}
		if found {
			return fmt.Errorf("deleted key %q returned value %x", key, value)
		}
	}

	rows, err := reader.Scan(ctx, nil, nil)
	if err != nil {
		return fmt.Errorf("scan final state: %w", err)
	}
	keys := sortedStorageKeys(expected)
	if err := verifyStorageRows("full scan", rows, keys, expected); err != nil {
		return err
	}

	// Reader ranges have inclusive upper bounds. Use a binary upper bound that
	// includes the ordering fixtures prefixed by "a" without also admitting
	// the adjacent key "b".
	prefixMin := []byte{'a'}
	prefixMax := []byte{'a', 0xff}
	prefixRows, err := reader.Scan(ctx, prefixMin, prefixMax)
	if err != nil {
		return fmt.Errorf("scan prefix a: %w", err)
	}
	prefixKeys := storageKeysInRange(keys, prefixMin, prefixMax)
	if err := verifyStorageRows("prefix scan", prefixRows, prefixKeys, expected); err != nil {
		return err
	}

	boundedRows, err := reader.Scan(ctx, []byte{'a'}, []byte{'b'})
	if err != nil {
		return fmt.Errorf("scan [a,b]: %w", err)
	}
	boundedKeys := storageKeysInRange(keys, []byte{'a'}, []byte{'b'})
	if err := verifyStorageRows("bounded inclusive scan", boundedRows, boundedKeys, expected); err != nil {
		return err
	}

	const scanLimit = 7
	limitedRows, err := reader.ScanLimit(ctx, nil, nil, scanLimit)
	if err != nil {
		return fmt.Errorf("limited scan: %w", err)
	}
	if err := verifyStorageRows("limited scan", limitedRows, keys[:scanLimit], expected); err != nil {
		return err
	}
	if err := verifyStorageIterator(ctx, reader, keys, expected); err != nil {
		return err
	}
	if err := verifyStorageSeek(ctx, reader, []byte{'a', 0x00}, []byte{'a', 0x00}); err != nil {
		return err
	}
	if err := verifyStorageSeek(ctx, reader, []byte{'a', 0x01}, []byte{'a', 'a'}); err != nil {
		return err
	}
	if err := verifyStorageSeek(ctx, reader, []byte{0x81}, []byte{0xff}); err != nil {
		return err
	}
	if err := verifyStorageSeek(ctx, reader, []byte{0xff, 0xff}, nil); err != nil {
		return err
	}
	return verifyStorageBoundedIterator(ctx, reader, boundedKeys, expected)
}

func sortedStorageKeys(expected map[string][]byte) []string {
	keys := make([]string, 0, len(expected))
	for key := range expected {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare([]byte(keys[i]), []byte(keys[j])) < 0
	})
	return keys
}

func storageKeysInRange(keys []string, minKey, maxKey []byte) []string {
	result := make([]string, 0)
	for _, key := range keys {
		keyBytes := []byte(key)
		if minKey != nil && bytes.Compare(keyBytes, minKey) < 0 {
			continue
		}
		if maxKey != nil && bytes.Compare(keyBytes, maxKey) > 0 {
			continue
		}
		result = append(result, key)
	}
	return result
}

func verifyStorageRows(
	label string,
	rows []isledb.KV,
	keys []string,
	expected map[string][]byte,
) error {
	if len(rows) != len(keys) {
		gotKeys := make([]string, 0, len(rows))
		for _, row := range rows {
			gotKeys = append(gotKeys, fmt.Sprintf("%x", row.Key))
		}
		wantKeys := make([]string, 0, len(keys))
		for _, key := range keys {
			wantKeys = append(wantKeys, fmt.Sprintf("%x", []byte(key)))
		}
		return fmt.Errorf(
			"%s row count=%d, want=%d; keys=%v, want keys=%v",
			label, len(rows), len(keys), gotKeys, wantKeys)
	}
	for index, key := range keys {
		if index > 0 && bytes.Compare(rows[index-1].Key, rows[index].Key) >= 0 {
			return fmt.Errorf(
				"%s rows are not in strict byte order at %d: %x then %x",
				label, index, rows[index-1].Key, rows[index].Key)
		}
		if !bytes.Equal(rows[index].Key, []byte(key)) || !bytes.Equal(rows[index].Value, expected[key]) {
			return fmt.Errorf(
				"%s row %d=(%x,%x), want=(%x,%x)",
				label, index, rows[index].Key, rows[index].Value, []byte(key), expected[key])
		}
	}
	return nil
}

func verifyStorageIterator(
	ctx context.Context,
	reader *isledb.Reader,
	keys []string,
	expected map[string][]byte,
) error {
	iterator, err := reader.NewIterator(ctx, isledb.IteratorOptions{})
	if err != nil {
		return fmt.Errorf("open full iterator: %w", err)
	}
	rows := make([]isledb.KV, 0, len(keys))
	for iterator.Next() {
		rows = append(rows, isledb.KV{
			Key:   append([]byte(nil), iterator.Key()...),
			Value: append([]byte(nil), iterator.Value()...),
		})
	}
	iterErr := iterator.Err()
	closeErr := iterator.Close()
	if iterErr != nil || closeErr != nil {
		return errors.Join(
			wrapStorageError("iterate full keyspace", iterErr),
			wrapStorageError("close full iterator", closeErr),
		)
	}
	return verifyStorageRows("full iterator", rows, keys, expected)
}

func verifyStorageBoundedIterator(
	ctx context.Context,
	reader *isledb.Reader,
	keys []string,
	expected map[string][]byte,
) error {
	iterator, err := reader.NewIterator(ctx, isledb.IteratorOptions{
		MinKey: []byte{'a'},
		MaxKey: []byte{'b'},
	})
	if err != nil {
		return fmt.Errorf("open bounded iterator: %w", err)
	}
	rows := make([]isledb.KV, 0, len(keys))
	for iterator.Next() {
		rows = append(rows, isledb.KV{
			Key:   append([]byte(nil), iterator.Key()...),
			Value: append([]byte(nil), iterator.Value()...),
		})
	}
	iterErr := iterator.Err()
	closeErr := iterator.Close()
	if iterErr != nil || closeErr != nil {
		return errors.Join(
			wrapStorageError("iterate bounded keyspace", iterErr),
			wrapStorageError("close bounded iterator", closeErr),
		)
	}
	return verifyStorageRows("bounded iterator", rows, keys, expected)
}

func verifyStorageSeek(
	ctx context.Context,
	reader *isledb.Reader,
	target []byte,
	want []byte,
) error {
	iterator, err := reader.NewIterator(ctx, isledb.IteratorOptions{})
	if err != nil {
		return fmt.Errorf("open SeekGE(%x) iterator: %w", target, err)
	}
	found := iterator.SeekGE(target)
	iterErr := iterator.Err()
	var got []byte
	if found {
		got = append([]byte(nil), iterator.Key()...)
	}
	closeErr := iterator.Close()
	if iterErr != nil || closeErr != nil {
		return errors.Join(
			wrapStorageError(fmt.Sprintf("SeekGE(%x)", target), iterErr),
			wrapStorageError("close seek iterator", closeErr),
		)
	}
	if want == nil {
		if found {
			return fmt.Errorf("SeekGE(%x) found %x, want no key", target, got)
		}
		return nil
	}
	if !found || !bytes.Equal(got, want) {
		return fmt.Errorf("SeekGE(%x) found=%t key=%x, want=%x", target, found, got, want)
	}
	return nil
}

type storageLockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *storageLockedBuffer) Write(data []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(data)
}

func (b *storageLockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

type storageWorkerProcess struct {
	role      string
	command   *exec.Cmd
	stdin     io.WriteCloser
	writeMu   sync.Mutex
	stderr    storageLockedBuffer
	stdout    storageLockedBuffer
	exited    chan struct{}
	exitMu    sync.Mutex
	exitErr   error
	closeOnce sync.Once
}

func startStorageWorker(
	ctx context.Context,
	role string,
	bucketURL string,
	prefix string,
	cacheDir string,
	payload string,
	events chan<- storageProcessEvent,
) (*storageWorkerProcess, error) {
	executable, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("locate test executable: %w", err)
	}
	command := exec.CommandContext(
		ctx,
		executable,
		"-test.run=^"+storageWorkerTestName+"$",
		"-test.count=1",
		"-test.timeout=4m",
	)
	command.Env = storageWorkerEnvironment(os.Environ(), map[string]string{
		storageWorkerRoleEnv:     role,
		storageWorkerBucketEnv:   bucketURL,
		storageWorkerPrefixEnv:   prefix,
		storageWorkerCacheDirEnv: cacheDir,
		storageWorkerFeedEnv:     payload,
	})
	stdin, err := command.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("create %s stdin: %w", role, err)
	}
	stdout, err := command.StdoutPipe()
	if err != nil {
		_ = stdin.Close()
		return nil, fmt.Errorf("create %s stdout: %w", role, err)
	}
	worker := &storageWorkerProcess{
		role:    role,
		command: command,
		stdin:   stdin,
		exited:  make(chan struct{}),
	}
	command.Stderr = &worker.stderr
	if err := command.Start(); err != nil {
		_ = stdin.Close()
		return nil, fmt.Errorf("start %s worker: %w", role, err)
	}

	go worker.scanEvents(stdout, events)
	go func() {
		err := command.Wait()
		worker.exitMu.Lock()
		worker.exitErr = err
		worker.exitMu.Unlock()
		close(worker.exited)
		events <- storageProcessEvent{worker: worker, err: err, exited: true}
	}()
	return worker, nil
}

func (w *storageWorkerProcess) scanEvents(output io.Reader, events chan<- storageProcessEvent) {
	scanner := bufio.NewScanner(output)
	scanner.Buffer(make([]byte, 64<<10), 1<<20)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, storageWorkerEventPrefix) {
			_, _ = w.stdout.Write(append([]byte(line), '\n'))
			continue
		}
		var event storageWorkerEvent
		if err := json.Unmarshal([]byte(strings.TrimPrefix(line, storageWorkerEventPrefix)), &event); err != nil {
			events <- storageProcessEvent{worker: w, err: fmt.Errorf("decode %s event: %w", w.role, err)}
			continue
		}
		events <- storageProcessEvent{worker: w, event: event}
	}
	if err := scanner.Err(); err != nil {
		events <- storageProcessEvent{worker: w, err: fmt.Errorf("scan %s output: %w", w.role, err)}
	}
}

func (w *storageWorkerProcess) send(command storageWorkerCommand) error {
	w.writeMu.Lock()
	defer w.writeMu.Unlock()
	data, err := json.Marshal(command)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if _, err := w.stdin.Write(data); err != nil {
		return fmt.Errorf("send %s command %q: %w", w.role, command.Type, err)
	}
	return nil
}

func (w *storageWorkerProcess) stop() {
	w.closeOnce.Do(func() {
		select {
		case <-w.exited:
			return
		default:
		}
		_ = w.send(storageWorkerCommand{Type: "close"})
		select {
		case <-w.exited:
		case <-time.After(5 * time.Second):
			_ = w.command.Process.Kill()
			<-w.exited
		}
		_ = w.stdin.Close()
	})
}

func (w *storageWorkerProcess) diagnostic() string {
	w.exitMu.Lock()
	exitErr := w.exitErr
	w.exitMu.Unlock()
	return fmt.Sprintf(
		"role=%s exit=%v\nstdout:\n%s\nstderr:\n%s",
		w.role, exitErr, w.stdout.String(), w.stderr.String())
}

func storageWorkerEnvironment(base []string, replacements map[string]string) []string {
	environment := make([]string, 0, len(base)+len(replacements))
	for _, item := range base {
		key, _, ok := strings.Cut(item, "=")
		if ok {
			if _, replaced := replacements[key]; replaced {
				continue
			}
		}
		environment = append(environment, item)
	}
	for key, value := range replacements {
		environment = append(environment, key+"="+value)
	}
	return environment
}

type storageWorkflowState struct {
	ready                  map[string]bool
	closed                 map[string]bool
	verified               map[string]int
	changeConsumed         map[string]int
	changeCursor           map[string]string
	changePayload          map[string]string
	changeWaiting          map[string]int
	changeResumed          map[string]bool
	changeVerified         map[string]bool
	seedFlushed            bool
	writerGeneration       int
	writesFinished         bool
	readerChecks           int
	checksAtCompaction     int
	firstCompactionAt      int
	maintenanceJobs        int
	maintenanceInputSSTs   int
	maintenanceOutputSSTs  int
	maintenanceOutputBytes int64
	waitingForWriter       int
	drainBarrier           uint64
	drainIdleCycles        int
}

func newStorageWorkflowState() *storageWorkflowState {
	return &storageWorkflowState{
		ready:          make(map[string]bool),
		closed:         make(map[string]bool),
		verified:       make(map[string]int),
		changeConsumed: make(map[string]int),
		changeCursor:   make(map[string]string),
		changePayload:  make(map[string]string),
		changeWaiting:  make(map[string]int),
		changeResumed:  make(map[string]bool),
		changeVerified: make(map[string]bool),
	}
}

func (s *storageWorkflowState) observe(processEvent storageProcessEvent) error {
	if processEvent.err != nil {
		if processEvent.exited && processEvent.worker != nil && s.closed[processEvent.worker.role] {
			return nil
		}
		role := "unknown"
		if processEvent.worker != nil {
			role = processEvent.worker.role
		}
		return fmt.Errorf("%s worker event: %w", role, processEvent.err)
	}
	event := processEvent.event
	if event.Type == "error" {
		return fmt.Errorf("%s worker: %s", event.Role, event.Message)
	}
	switch event.Type {
	case "ready":
		s.ready[event.Role] = true
		if event.Payload != "" {
			s.changePayload[event.Role] = event.Payload
		}
	case "closed":
		s.closed[event.Role] = true
	case "generation_flushed":
		s.writerGeneration = event.Generation
	case "seed_flushed":
		s.seedFlushed = true
	case "writes_finished":
		s.writesFinished = true
		s.drainIdleCycles = 0
	case "sentinel_verified":
		if event.Role == "reader-live" && event.Checks > s.readerChecks {
			s.readerChecks = event.Checks
		}
	case "final_state_verified":
		s.verified[event.Role] = event.Generation
	case "change_waiting":
		s.changeWaiting[event.Role] = event.Target
	case "changes_consumed":
		s.changeConsumed[event.Role] = event.Consumed
		s.changeCursor[event.Role] = event.Cursor
		s.changePayload[event.Role] = event.Payload
	case "change_resumed":
		s.changeConsumed[event.Role] = event.Consumed
		s.changeCursor[event.Role] = event.Cursor
		s.changePayload[event.Role] = event.Payload
		s.changeResumed[event.Role] = true
	case "change_feed_verified":
		s.changeConsumed[event.Role] = event.Consumed
		s.changeCursor[event.Role] = event.Cursor
		s.changePayload[event.Role] = event.Payload
		s.changeVerified[event.Role] = true
	case "maintenance_barrier":
		s.drainBarrier = event.Sequence
		s.drainIdleCycles = 0
	case "maintenance_cycle":
		if event.State == isledb.MaintenanceWaitingForWriter.String() {
			s.waitingForWriter++
		}
		if event.Jobs > 0 {
			if s.maintenanceJobs == 0 {
				s.firstCompactionAt = s.writerGeneration
				s.checksAtCompaction = s.readerChecks
			}
			s.maintenanceJobs += event.Jobs
			s.maintenanceInputSSTs += event.InputSSTs
			s.maintenanceOutputSSTs += event.OutputSSTs
			s.maintenanceOutputBytes += event.OutputBytes
		}
		if s.writesFinished && s.drainBarrier > 0 && event.Sequence > s.drainBarrier &&
			event.State == isledb.MaintenanceIdle.String() &&
			event.Selected == isledb.MaintenanceTaskNone.String() {
			s.drainIdleCycles++
		} else if s.writesFinished && event.Sequence > s.drainBarrier {
			s.drainIdleCycles = 0
		}
	}
	return nil
}

func runStorageEngineProcessWorkflow(
	t *testing.T,
	bucketURL string,
	payload isledb.ChangeFeedPayload,
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	prefix := fmt.Sprintf("storage-engine-process/%d", time.Now().UnixNano())
	events := make(chan storageProcessEvent, 16<<10)
	state := newStorageWorkflowState()
	workers := make([]*storageWorkerProcess, 0, 6)
	start := func(role, cacheDir string) *storageWorkerProcess {
		t.Helper()
		worker, err := startStorageWorker(
			ctx, role, bucketURL, prefix, cacheDir, payload.String(), events)
		if err != nil {
			storageWorkflowFatal(t, workers, err)
		}
		workers = append(workers, worker)
		return worker
	}
	t.Cleanup(func() {
		for index := len(workers) - 1; index >= 0; index-- {
			workers[index].stop()
		}
	})

	writer := start("writer", "")
	storageAwait(t, ctx, workers, events, state, func() bool { return state.ready["writer"] })
	storageSend(t, workers, writer, storageWorkerCommand{Type: "seed"})
	storageAwait(t, ctx, workers, events, state, func() bool {
		return state.seedFlushed
	})
	changeReader := start("change-live", "")
	changeRole := changeReader.role
	storageAwait(t, ctx, workers, events, state, func() bool {
		return state.ready[changeRole] && state.changePayload[changeRole] == payload.String()
	})
	seedChangeCount := len(expectedStorageChanges(0))
	storageSend(t, workers, changeReader, storageWorkerCommand{
		Type:   "consume_until",
		Target: seedChangeCount,
	})
	storageAwait(t, ctx, workers, events, state, func() bool {
		return state.changeConsumed[changeRole] == seedChangeCount
	})

	liveReader := start("reader-live", t.TempDir())
	storageAwait(t, ctx, workers, events, state, func() bool { return state.ready["reader-live"] })
	storageAwait(t, ctx, workers, events, state, func() bool { return state.readerChecks > 0 })

	maintenance := start("maintenance", "")
	storageAwait(t, ctx, workers, events, state, func() bool { return state.ready["maintenance"] })

	generation := 0
	for generation < storageMaximumGenerations {
		generation++
		changeTarget := len(expectedStorageChanges(generation))
		storageSend(t, workers, changeReader, storageWorkerCommand{
			Type:   "consume_until",
			Target: changeTarget,
		})
		storageAwait(t, ctx, workers, events, state, func() bool {
			return state.changeWaiting[changeRole] == changeTarget
		})
		storageSend(t, workers, writer, storageWorkerCommand{
			Type:       "write_generation",
			Generation: generation,
		})
		wantGeneration := generation
		storageAwait(t, ctx, workers, events, state, func() bool {
			return state.writerGeneration >= wantGeneration &&
				state.changeConsumed[changeRole] == changeTarget
		})
		if generation >= storageMinimumGenerations && state.maintenanceJobs > 0 {
			break
		}
	}
	if state.maintenanceJobs == 0 {
		storageAwait(t, ctx, workers, events, state, func() bool {
			return state.maintenanceJobs > 0
		})
	}
	if state.firstCompactionAt <= 0 || state.firstCompactionAt > generation {
		storageWorkflowFatal(t, workers, fmt.Errorf(
			"compaction was not observed while writer remained active: generation=%d first_compaction_at=%d",
			generation, state.firstCompactionAt))
	}
	checkpointCursor := state.changeCursor[changeRole]
	checkpointConsumed := state.changeConsumed[changeRole]
	if checkpointCursor == "" || checkpointConsumed == 0 {
		storageWorkflowFatal(t, workers, fmt.Errorf(
			"missing change-feed checkpoint: consumed=%d cursor=%q",
			checkpointConsumed, checkpointCursor))
	}
	storageCloseWorker(t, ctx, workers, events, state, changeReader)
	changeReader = start("change-resume", "")
	changeRole = changeReader.role
	storageAwait(t, ctx, workers, events, state, func() bool {
		return state.ready[changeRole] && state.changePayload[changeRole] == payload.String()
	})
	storageSend(t, workers, changeReader, storageWorkerCommand{
		Type:     "resume",
		Consumed: checkpointConsumed,
		Cursor:   checkpointCursor,
	})
	storageAwait(t, ctx, workers, events, state, func() bool {
		return state.changeResumed[changeRole] &&
			state.changeConsumed[changeRole] == checkpointConsumed &&
			state.changeCursor[changeRole] == checkpointCursor
	})

	for index := 0; index < storagePostCompactions; index++ {
		generation++
		changeTarget := len(expectedStorageChanges(generation))
		storageSend(t, workers, changeReader, storageWorkerCommand{
			Type:   "consume_until",
			Target: changeTarget,
		})
		storageAwait(t, ctx, workers, events, state, func() bool {
			return state.changeWaiting[changeRole] == changeTarget
		})
		storageSend(t, workers, writer, storageWorkerCommand{
			Type:       "write_generation",
			Generation: generation,
		})
		wantGeneration := generation
		storageAwait(t, ctx, workers, events, state, func() bool {
			return state.writerGeneration >= wantGeneration &&
				state.changeConsumed[changeRole] == changeTarget
		})
	}
	storageAwait(t, ctx, workers, events, state, func() bool {
		return state.readerChecks > state.checksAtCompaction
	})

	storageSend(t, workers, writer, storageWorkerCommand{Type: "finish_writes"})
	storageAwait(t, ctx, workers, events, state, func() bool { return state.writesFinished })
	storageSend(t, workers, maintenance, storageWorkerCommand{Type: "barrier"})
	storageAwait(t, ctx, workers, events, state, func() bool { return state.drainBarrier > 0 })
	storageAwait(t, ctx, workers, events, state, func() bool { return state.drainIdleCycles >= 2 })
	finalChangeCount := len(expectedStorageChanges(generation))
	if state.changeConsumed[changeRole] != finalChangeCount {
		storageWorkflowFatal(t, workers, fmt.Errorf(
			"change count=%d, want=%d", state.changeConsumed[changeRole], finalChangeCount))
	}
	storageSend(t, workers, changeReader, storageWorkerCommand{Type: "verify_head"})
	storageAwait(t, ctx, workers, events, state, func() bool {
		return state.changeVerified[changeRole] &&
			state.changeConsumed[changeRole] == finalChangeCount
	})

	if state.waitingForWriter == 0 || state.maintenanceInputSSTs < 3 ||
		state.maintenanceOutputSSTs == 0 || state.maintenanceOutputBytes == 0 {
		storageWorkflowFatal(t, workers, fmt.Errorf(
			"incomplete maintenance evidence: jobs=%d input=%d output=%d bytes=%d waiting=%d",
			state.maintenanceJobs, state.maintenanceInputSSTs, state.maintenanceOutputSSTs,
			state.maintenanceOutputBytes, state.waitingForWriter))
	}

	storageSend(t, workers, liveReader, storageWorkerCommand{
		Type:       "verify_final",
		Generation: generation,
	})
	storageAwait(t, ctx, workers, events, state, func() bool {
		return state.verified["reader-live"] == generation
	})

	storageCloseWorker(t, ctx, workers, events, state, maintenance)
	storageCloseWorker(t, ctx, workers, events, state, changeReader)
	storageCloseWorker(t, ctx, workers, events, state, liveReader)
	storageCloseWorker(t, ctx, workers, events, state, writer)

	coldReader := start("reader-cold", t.TempDir())
	storageAwait(t, ctx, workers, events, state, func() bool { return state.ready["reader-cold"] })
	storageSend(t, workers, coldReader, storageWorkerCommand{
		Type:       "verify_final",
		Generation: generation,
	})
	storageAwait(t, ctx, workers, events, state, func() bool {
		return state.verified["reader-cold"] == generation
	})
	storageCloseWorker(t, ctx, workers, events, state, coldReader)

	t.Logf(
		"process integration payload=%s generations=%d changes=%d compaction_at=%d jobs=%d input_ssts=%d output_ssts=%d reader_checks=%d",
		payload, generation, finalChangeCount,
		state.firstCompactionAt, state.maintenanceJobs,
		state.maintenanceInputSSTs, state.maintenanceOutputSSTs, state.readerChecks)
}

func storageAwait(
	t *testing.T,
	ctx context.Context,
	workers []*storageWorkerProcess,
	events <-chan storageProcessEvent,
	state *storageWorkflowState,
	predicate func() bool,
) {
	t.Helper()
	for !predicate() {
		select {
		case event := <-events:
			if err := state.observe(event); err != nil {
				storageWorkflowFatal(t, workers, err)
			}
		case <-ctx.Done():
			storageWorkflowFatal(t, workers, fmt.Errorf("wait for integration state: %w", ctx.Err()))
		}
	}
}

func storageSend(
	t *testing.T,
	workers []*storageWorkerProcess,
	worker *storageWorkerProcess,
	command storageWorkerCommand,
) {
	t.Helper()
	if err := worker.send(command); err != nil {
		storageWorkflowFatal(t, workers, err)
	}
}

func storageCloseWorker(
	t *testing.T,
	ctx context.Context,
	workers []*storageWorkerProcess,
	events <-chan storageProcessEvent,
	state *storageWorkflowState,
	worker *storageWorkerProcess,
) {
	t.Helper()
	storageSend(t, workers, worker, storageWorkerCommand{Type: "close"})
	storageAwait(t, ctx, workers, events, state, func() bool { return state.closed[worker.role] })
	select {
	case <-worker.exited:
		worker.exitMu.Lock()
		err := worker.exitErr
		worker.exitMu.Unlock()
		if err != nil {
			storageWorkflowFatal(t, workers, fmt.Errorf("%s worker exit: %w", worker.role, err))
		}
	case <-ctx.Done():
		storageWorkflowFatal(t, workers, fmt.Errorf("wait for %s exit: %w", worker.role, ctx.Err()))
	}
}

func storageWorkflowFatal(t *testing.T, workers []*storageWorkerProcess, err error) {
	t.Helper()
	for _, worker := range workers {
		worker.stop()
	}
	var diagnostics strings.Builder
	for _, worker := range workers {
		diagnostics.WriteString("\n--- ")
		diagnostics.WriteString(worker.role)
		diagnostics.WriteString(" ---\n")
		diagnostics.WriteString(worker.diagnostic())
		diagnostics.WriteByte('\n')
	}
	t.Fatalf("storage-engine process integration: %v%s", err, diagnostics.String())
}

func wrapStorageError(operation string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s: %w", operation, err)
}

func stopStorageProviderTimer(timer *time.Timer) {
	if timer.Stop() {
		return
	}
	select {
	case <-timer.C:
	default:
	}
}
