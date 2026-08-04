package manifest

import "context"

// GCCursorStorage persists the bounded deterministic-GC cursor with CAS
// semantics.
type GCCursorStorage interface {
	// LoadGCCursor returns cursor data as (data, token, exists, err).
	LoadGCCursor(ctx context.Context) ([]byte, string, bool, error)

	// StoreGCCursor writes cursor data using the token returned by LoadGCCursor.
	// It returns ErrPreconditionFailed on CAS conflict.
	StoreGCCursor(ctx context.Context, data []byte, matchToken string, exists bool) error
}
