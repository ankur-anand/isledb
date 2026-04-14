package isledb

import (
	"context"

	"github.com/ankur-anand/isledb/blobstore"
)

// CoordinatorOptions configures a shared refresh coordinator.
type CoordinatorOptions struct {
	ReaderOptions ReaderOpenOptions
}

// DefaultCoordinatorOptions returns sane defaults for CoordinatorOptions.
func DefaultCoordinatorOptions() CoordinatorOptions {
	return CoordinatorOptions{
		ReaderOptions: DefaultReaderOpenOptions(),
	}
}

// OpenCoordinator opens a shared refresh coordinator that publishes immutable
// read views over the latest loaded manifest state.
func OpenCoordinator(ctx context.Context, store *blobstore.Store, opts CoordinatorOptions) (*Coordinator, error) {
	reader, err := OpenReader(ctx, store, opts.ReaderOptions)
	if err != nil {
		return nil, err
	}

	coord := newCoordinator(reader)
	if err := coord.initCurrentView(); err != nil {
		_ = reader.Close()
		return nil, err
	}

	return coord, nil
}
