package isledb

import (
	"context"
	"errors"
	"sync"
	"time"
)

var (
	ErrNotManual    = errors.New("reader views are not in manual mode")
	ErrNextViewDone = errors.New("next view already published or discarded")
)

// SSTInfo describes one SST of a view.
type SSTInfo struct {
	ID             string
	Level          int
	Size           int64
	MinKey, MaxKey []byte
}

// NextView is a view the Reader has loaded and not yet published, handed to
// the application in Manual mode to prepare before reads switch to it.
type NextView struct {
	r         *Reader
	view      loadedView
	previous  uint64
	published *manifestState
	added     []SSTInfo
	removed   []SSTInfo

	mu   sync.Mutex
	done bool
}

// NextView returns a view the Reader has loaded that is newer than the
// published one, waiting for the Reader's next such load if none is
// pending. Each load is handed out at most once: after a view is taken,
// published or not, the next call waits for a later load. If nothing new was
// committed, that load is at the same position as the view handed out before,
// still newer than the published view, so it can be published. Manual mode
// only; otherwise it returns ErrNotManual.
func (r *Reader) NextView(ctx context.Context) (*NextView, error) {
	if !r.viewPolicy.Manual {
		return nil, ErrNotManual
	}
	for {
		// The lifecycle lock is held only to take a view, never while
		// waiting, or Close would wait on this call.
		done, err := r.beginRead()
		if err != nil {
			return nil, err
		}
		r.mu.Lock()
		if r.nextView != nil && r.nextLoads > r.handedLoads &&
			currentNextSeq(r.nextView.current) > r.viewSeq {
			r.handedLoads = r.nextLoads
			view, previous, published := *r.nextView, r.viewSeq, r.manifest
			r.mu.Unlock()
			done()
			// Manifests are immutable once loaded: compare them unlocked.
			return newNextView(r, view, previous, published), nil
		}
		ready := r.nextReady
		r.mu.Unlock()
		done()
		if ready == nil {
			return nil, ErrReaderClosed
		}
		select {
		case <-ready:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func newNextView(r *Reader, view loadedView, previous uint64, published *manifestState) *NextView {
	before, after := sstsByID(published), sstsByID(view.manifest)
	v := &NextView{r: r, view: view, previous: previous, published: published}
	for id, info := range after {
		if _, ok := before[id]; !ok {
			v.added = append(v.added, info)
		}
	}
	for id, info := range before {
		if _, ok := after[id]; !ok {
			v.removed = append(v.removed, info)
		}
	}
	return v
}

func sstsByID(m *manifestState) map[string]SSTInfo {
	ssts := map[string]SSTInfo{}
	if m == nil {
		return ssts
	}
	add := func(sst sstMetadata, level int) {
		ssts[sst.ID] = SSTInfo{
			ID: sst.ID, Level: level, Size: sst.Size,
			MinKey: append([]byte(nil), sst.MinKey...), MaxKey: append([]byte(nil), sst.MaxKey...),
		}
	}
	for _, sst := range m.L0SSTs {
		add(sst, 0)
	}
	for _, level := range m.Levels {
		for _, sst := range level.SSTs {
			add(sst, int(level.Number))
		}
	}
	return ssts
}

// Previous is the position of the view published when this one was handed
// out; Publish succeeds only while it is still published.
func (v *NextView) Previous() ViewPosition { return ViewPosition(v.previous) }

// Next is this view's position.
func (v *NextView) Next() ViewPosition { return ViewPosition(currentNextSeq(v.view.current)) }

// Version identifies this view's CURRENT, for logs and diagnostics.
func (v *NextView) Version() Version { return versionFromCurrent(v.view.current) }

// Added are the SSTs this view names that the published one did not; an SST
// moved between levels is in neither list.
func (v *NextView) Added() []SSTInfo { return append([]SSTInfo(nil), v.added...) }

// Removed are the SSTs the published view names that this one does not; an
// SST moved between levels is in neither list.
func (v *NextView) Removed() []SSTInfo { return append([]SSTInfo(nil), v.removed...) }

// Snapshot returns a snapshot of this view, as Reader.Snapshot does of the
// published one. It never refreshes. The caller closes it.
func (v *NextView) Snapshot() (*Snapshot, error) {
	if err := v.check(); err != nil {
		return nil, err
	}
	expiresAt := v.view.loadedAt.Add(v.view.current.PinnedViewAge())
	if !time.Now().Before(expiresAt) {
		return nil, ErrNextViewExpired
	}
	return newSnapshot(v.r, v.view.manifest, v.Version(), expiresAt), nil
}

// Prefetch caches the Added SSTs on local disk, as Reader.Prefetch does for
// the published view: All for every added SST, Range for those overlapping
// it, within MaxSSTs and MaxBytes, using only the disk cache's free space, so
// warming this view never evicts what reads are using. SSTs that do not fit
// load on demand once published.
func (v *NextView) Prefetch(ctx context.Context, opts PrefetchOptions) (PrefetchStats, error) {
	if err := validatePrefetchOptions(opts); err != nil {
		return PrefetchStats{}, err
	}
	if err := v.check(); err != nil {
		return PrefetchStats{}, err
	}
	r := v.r
	done, err := r.beginRead()
	if err != nil {
		return PrefetchStats{}, err
	}
	defer done()
	expiresAt := v.view.loadedAt.Add(v.view.current.PinnedViewAge())
	if !time.Now().Before(expiresAt) {
		return PrefetchStats{}, ErrNextViewExpired
	}

	only := make(map[string]struct{}, len(v.added))
	for _, sst := range v.added {
		only[sst.ID] = struct{}{}
	}
	selected, stats := r.selectSSTsToPrefetch(v.view.manifest, opts, only)
	return r.fetchPrefetchSSTs(ctx, selected, stats, opts.Concurrency, expiresAt, ErrNextViewExpired)
}

func sstMetadataOf(m *manifestState) []sstMetadata {
	if m == nil {
		return nil
	}
	ssts := append([]sstMetadata(nil), m.L0SSTs...)
	for _, level := range m.Levels {
		ssts = append(ssts, level.SSTs...)
	}
	return ssts
}

// Publish switches reads to this view if the published view is still at
// Previous; otherwise it returns ErrViewChanged, and ErrNextViewExpired if the
// view's pinned age has passed. Either way the view is then done.
func (v *NextView) Publish() error {
	v.mu.Lock()
	if v.done {
		v.mu.Unlock()
		return ErrNextViewDone
	}
	v.done = true
	v.mu.Unlock()
	return v.r.publishAt(v.view, v.previous)
}

// Discard marks the view done. It releases nothing, so calling it after a
// failed Publish is optional.
func (v *NextView) Discard() {
	v.mu.Lock()
	v.done = true
	v.mu.Unlock()
}

func (v *NextView) check() error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.done {
		return ErrNextViewDone
	}
	return nil
}
