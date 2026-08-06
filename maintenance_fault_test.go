package isledb

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal/manifest"
)

var errInjectedMailboxIO = errors.New("injected maintenance mailbox I/O failure")

type mailboxFaultPoint string

const (
	faultReadMaintenanceHead    mailboxFaultPoint = "read_maintenance_head"
	faultWriteMaintenanceBefore mailboxFaultPoint = "write_maintenance_head_before"
	faultWriteMaintenanceAfter  mailboxFaultPoint = "write_maintenance_head_after"
	faultReadCurrent            mailboxFaultPoint = "read_current"
	faultWriteCurrentBefore     mailboxFaultPoint = "write_current_before"
	faultWriteCurrentAfter      mailboxFaultPoint = "write_current_after"
)

// mailboxFaultStorage fails exactly one armed provider operation. "After"
// faults model a successful provider mutation whose response was lost.
type mailboxFaultStorage struct {
	*manifest.BlobStoreBackend

	mu    sync.Mutex
	armed mailboxFaultPoint
	fired bool
}

func (s *mailboxFaultStorage) arm(point mailboxFaultPoint) {
	s.mu.Lock()
	s.armed = point
	s.fired = false
	s.mu.Unlock()
}

func (s *mailboxFaultStorage) fail(point mailboxFaultPoint) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fired || s.armed != point {
		return false
	}
	s.fired = true
	return true
}

func (s *mailboxFaultStorage) assertFired(t testing.TB) {
	t.Helper()
	s.mu.Lock()
	fired, armed := s.fired, s.armed
	s.mu.Unlock()
	if !fired {
		t.Fatalf("mailbox fault %q did not fire", armed)
	}
}

func (s *mailboxFaultStorage) ReadMaintenanceHead(ctx context.Context) ([]byte, string, error) {
	if s.fail(faultReadMaintenanceHead) {
		return nil, "", errInjectedMailboxIO
	}
	return s.BlobStoreBackend.ReadMaintenanceHead(ctx)
}

func (s *mailboxFaultStorage) WriteMaintenanceHeadCAS(ctx context.Context, data []byte, expectedETag string) (string, error) {
	if s.fail(faultWriteMaintenanceBefore) {
		return "", errInjectedMailboxIO
	}
	etag, err := s.BlobStoreBackend.WriteMaintenanceHeadCAS(ctx, data, expectedETag)
	if err != nil {
		return "", err
	}
	if s.fail(faultWriteMaintenanceAfter) {
		return "", errInjectedMailboxIO
	}
	return etag, nil
}

func (s *mailboxFaultStorage) ReadCurrent(ctx context.Context) ([]byte, string, error) {
	if s.fail(faultReadCurrent) {
		return nil, "", errInjectedMailboxIO
	}
	return s.BlobStoreBackend.ReadCurrent(ctx)
}

func (s *mailboxFaultStorage) WriteCurrentCAS(ctx context.Context, data []byte, expectedETag string) (string, error) {
	if s.fail(faultWriteCurrentBefore) {
		return "", errInjectedMailboxIO
	}
	etag, err := s.BlobStoreBackend.WriteCurrentCAS(ctx, data, expectedETag)
	if err != nil {
		return "", err
	}
	if s.fail(faultWriteCurrentAfter) {
		return "", errInjectedMailboxIO
	}
	return etag, nil
}

func TestOperationalRecovery_MaintenanceMailboxFailureMatrix(t *testing.T) {
	tests := []struct {
		name  string
		phase string
		point mailboxFaultPoint
	}{
		{name: "claim read HEAD", phase: "claim", point: faultReadMaintenanceHead},
		{name: "claim HEAD CAS before provider", phase: "claim", point: faultWriteMaintenanceBefore},
		{name: "claim HEAD CAS lost response", phase: "claim", point: faultWriteMaintenanceAfter},
		{name: "stage read HEAD", phase: "stage", point: faultReadMaintenanceHead},
		{name: "stage HEAD CAS before provider", phase: "stage", point: faultWriteMaintenanceBefore},
		{name: "stage HEAD CAS lost response", phase: "stage", point: faultWriteMaintenanceAfter},
		{name: "apply read HEAD", phase: "apply", point: faultReadMaintenanceHead},
		{name: "apply read CURRENT", phase: "apply", point: faultReadCurrent},
		{name: "apply CURRENT CAS before provider", phase: "apply", point: faultWriteCurrentBefore},
		{name: "apply CURRENT CAS lost response", phase: "apply", point: faultWriteCurrentAfter},
		{name: "clear read HEAD", phase: "clear", point: faultReadMaintenanceHead},
		{name: "clear read CURRENT", phase: "clear", point: faultReadCurrent},
		{name: "clear HEAD CAS before provider", phase: "clear", point: faultWriteMaintenanceBefore},
		{name: "clear HEAD CAS lost response", phase: "clear", point: faultWriteMaintenanceAfter},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			testMaintenanceMailboxRecovery(t, test.phase, test.point)
		})
	}
}

func testMaintenanceMailboxRecovery(t testing.TB, phase string, point mailboxFaultPoint) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store := blobstore.NewMemory(fmt.Sprintf("mailbox-fault-%s-%s", phase, point))
	defer store.Close()
	storage := &mailboxFaultStorage{BlobStoreBackend: manifest.NewBlobStoreBackend(store)}
	db, err := openDB(ctx, store, dbOpenOptions{manifestStorage: storage})
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	defer db.Close()

	writerOpts := DefaultWriterOptions()
	writerOpts.OwnerID = "mailbox-fault-writer"
	writerOpts.Flush.Interval = 0
	writerOpts.SST.Compression = "none"
	writer, err := db.OpenWriter(ctx, writerOpts)
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}

	maintenanceOpts := DefaultMaintenanceOptions()
	maintenanceOpts.OwnerID = "mailbox-fault-maintenance"
	maintenanceOpts.Compaction.L0SSTCount = 1 << 20

	if phase == "claim" {
		storage.arm(point)
		if _, err := db.OpenMaintenance(ctx, maintenanceOpts); !errors.Is(err, errInjectedMailboxIO) {
			t.Fatalf("OpenMaintenance fault error=%v, want %v", err, errInjectedMailboxIO)
		}
		storage.assertFired(t)
	}

	maintenance, err := db.OpenMaintenance(ctx, maintenanceOpts)
	if err != nil {
		t.Fatalf("OpenMaintenance recovery: %v", err)
	}

	command := manifest.MaintenanceCommand{
		ID:              "mailbox-recovery-command",
		Kind:            manifest.MaintenanceCommandRetirementFloor,
		RetirementFloor: &manifest.AdvanceFloorCommand{Floor: 1},
	}

	if phase == "stage" {
		storage.arm(point)
		err := maintenance.stageCommand(ctx, command)
		if !errors.Is(err, errInjectedMailboxIO) {
			t.Fatalf("stageCommand fault error=%v, want %v", err, errInjectedMailboxIO)
		}
		storage.assertFired(t)
		if point != faultWriteMaintenanceAfter {
			if err := maintenance.stageCommand(ctx, command); err != nil {
				t.Fatalf("stageCommand retry: %v", err)
			}
		}
	} else {
		if err := maintenance.stageCommand(ctx, command); err != nil {
			t.Fatalf("stageCommand: %v", err)
		}
	}

	if phase == "apply" {
		storage.arm(point)
		if err := writer.Flush(ctx); !errors.Is(err, errInjectedMailboxIO) {
			t.Fatalf("writer Flush fault error=%v, want %v", err, errInjectedMailboxIO)
		}
		storage.assertFired(t)
	}
	if err := writer.Flush(ctx); err != nil {
		t.Fatalf("writer Flush recovery: %v", err)
	}

	if phase == "clear" {
		storage.arm(point)
		if _, err := maintenance.RunOnce(ctx); !errors.Is(err, errInjectedMailboxIO) {
			t.Fatalf("maintenance clear fault error=%v, want %v", err, errInjectedMailboxIO)
		}
		storage.assertFired(t)
	}
	if _, err := maintenance.RunOnce(ctx); err != nil {
		t.Fatalf("maintenance reconcile retry: %v", err)
	}

	head, _, err := db.manifestStore.ReadMaintenanceHead(ctx)
	if err != nil {
		t.Fatalf("ReadMaintenanceHead: %v", err)
	}
	if head == nil || head.Pending != nil {
		t.Fatalf("maintenance HEAD after recovery=%+v, want no pending command", head)
	}
	current, err := db.manifestStore.ReadCurrentData(ctx)
	if err != nil {
		t.Fatalf("ReadCurrentData: %v", err)
	}
	if current == nil || current.MaintenanceReceipt == nil ||
		current.MaintenanceReceipt.CommandID != command.ID ||
		current.MaintenanceReceipt.Status != manifest.MaintenanceStatusApplied {
		t.Fatalf("CURRENT maintenance receipt=%+v", current)
	}
	if current.RetirementLogStart < command.RetirementFloor.Floor {
		t.Fatalf("retirement_log_start=%d, want at least %d",
			current.RetirementLogStart, command.RetirementFloor.Floor)
	}

	if err := writer.Put(ctx, []byte("after-recovery"), []byte("visible")); err != nil {
		t.Fatalf("Put after recovery: %v", err)
	}
	if err := writer.Close(ctx); err != nil {
		t.Fatalf("writer Close: %v", err)
	}
	if err := maintenance.Close(ctx); err != nil {
		t.Fatalf("maintenance Close: %v", err)
	}

	reader := openReaderFromDBForTest(t, ctx, store, DefaultReaderOpenOptions(t.TempDir()))
	defer reader.Close()
	assertReaderValue(t, ctx, reader, "after-recovery", "visible", true)
}
