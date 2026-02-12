package manifest

import "context"

// GCMarkStorage persists GC coordination state with CAS semantics.
type GCMarkStorage interface {
	// LoadPendingDeleteMarks returns pending delete marks as (data, token, exists, err).
	// If none exist, it returns (nil, "", false, nil).
	LoadPendingDeleteMarks(ctx context.Context) ([]byte, string, bool, error)

	// StorePendingDeleteMarks writes pending delete marks using the latest token/exists from LoadPendingDeleteMarks.
	// Returns ErrPreconditionFailed on CAS conflict.
	StorePendingDeleteMarks(ctx context.Context, data []byte, matchToken string, exists bool) error

	// LoadGCCheckpoint returns checkpoint data as (data, token, exists, err).
	// If none exist, it returns (nil, "", false, nil).
	LoadGCCheckpoint(ctx context.Context) ([]byte, string, bool, error)

	// StoreGCCheckpoint writes checkpoint data using the latest token/exists from LoadGCCheckpoint.
	// Returns ErrPreconditionFailed on CAS conflict.
	StoreGCCheckpoint(ctx context.Context, data []byte, matchToken string, exists bool) error
}
