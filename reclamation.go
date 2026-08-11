package isledb

import (
	"context"
	"errors"

	"github.com/ankur-anand/isledb/blobstore"
)

func reclamationCancellation(ctx context.Context, err error) error {
	if ctx != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return nil
}

// objectDeleter is deliberately narrower than blobstore.Store. Physical
// reclamation must not gain write or CAS authority merely because it needs to
// delete an already-proven-safe object.
type objectDeleter interface {
	Delete(context.Context, string) error
	BatchDelete(context.Context, []string) error
}

// limitedObjectDeleter shares one request semaphore across all reclamation
// families. A batch delete consumes one slot because it is one provider
// request (or one bounded fallback operation in stores without native batch
// deletion).
type limitedObjectDeleter struct {
	base objectDeleter
	sem  chan struct{}
}

func newLimitedObjectDeleter(store *blobstore.Store, concurrency int) *limitedObjectDeleter {
	return newLimitedObjectDeleterFor(store, concurrency)
}

func newLimitedObjectDeleterFor(base objectDeleter, concurrency int) *limitedObjectDeleter {
	if concurrency <= 0 {
		concurrency = defaultReclaimDeleteConcurrency
	}
	return &limitedObjectDeleter{base: base, sem: make(chan struct{}, concurrency)}
}

func (d *limitedObjectDeleter) Delete(ctx context.Context, key string) error {
	if err := d.acquire(ctx); err != nil {
		return err
	}
	defer d.release()
	return d.base.Delete(ctx, key)
}

func (d *limitedObjectDeleter) BatchDelete(ctx context.Context, keys []string) error {
	if err := d.acquire(ctx); err != nil {
		return err
	}
	defer d.release()
	return d.base.BatchDelete(ctx, keys)
}

func (d *limitedObjectDeleter) acquire(ctx context.Context) error {
	select {
	case d.sem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (d *limitedObjectDeleter) release() {
	<-d.sem
}
