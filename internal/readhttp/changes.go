package readhttp

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/ankur-anand/isledb"
)

const (
	// DefaultChangesLimit is the changes in a page when the request gives no
	// limit; MaxChangesLimit is the most it may ask for.
	DefaultChangesLimit = 1000
	MaxChangesLimit     = 10000
)

// Changes serves the change feed of one database. The feed's head is the
// reader's view: a consumer caught up with what this replica has loaded is
// answered from memory, so polling costs about one shared read of each new
// position per replica, however many consumers poll, and new changes show
// once the reader loads them.
type Changes struct {
	open func(context.Context) (*isledb.ChangeReader, error)
	// opening is held while the feed opens, and by Close: one open runs at
	// a time, each caller waits for it only as long as its context, and
	// Close waits for an open in progress, at most that caller's timeout.
	opening chan struct{}
	reader  atomic.Pointer[isledb.ChangeReader] // nil until a read first needs it
	closed  bool                                // under opening
}

// NewChanges serves the feed that open opens. It is opened on the first
// read that needs it, so a reader may start before the database has a feed.
func NewChanges(open func(context.Context) (*isledb.ChangeReader, error)) *Changes {
	return &Changes{open: open, opening: make(chan struct{}, 1)}
}

func (c *Changes) changeReader(ctx context.Context) (*isledb.ChangeReader, error) {
	if cr := c.reader.Load(); cr != nil {
		return cr, nil
	}
	select {
	case c.opening <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	defer func() { <-c.opening }()
	if c.closed {
		return nil, isledb.ErrChangeReaderClosed
	}
	if cr := c.reader.Load(); cr != nil {
		return cr, nil
	}
	cr, err := c.open(ctx)
	if err != nil {
		return nil, err
	}
	c.reader.Store(cr)
	return cr, nil
}

// loadedHead is the newest head the feed has loaded, or zero.
func (c *Changes) loadedHead() isledb.ChangeCursor {
	if cr := c.reader.Load(); cr != nil {
		return cr.LoadedHead()
	}
	return isledb.ChangeCursor{}
}

// Close closes the feed, once an open in progress ends.
func (c *Changes) Close() error {
	c.opening <- struct{}{}
	defer func() { <-c.opening }()
	c.closed = true
	if cr := c.reader.Load(); cr != nil {
		return cr.Close()
	}
	return nil
}

type changesRequest struct {
	from  string // a cursor, "oldest" or "head"
	limit int
}

func parseChanges(rawQuery string) (changesRequest, error) {
	q, err := url.ParseQuery(rawQuery)
	if err != nil {
		return changesRequest{}, errors.New("the query is not encoded correctly")
	}
	for _, name := range slices.Sorted(maps.Keys(q)) {
		switch name {
		case "from", "limit":
		default:
			return changesRequest{}, fmt.Errorf("unknown query parameter %q", name)
		}
		if len(q[name]) > 1 {
			return changesRequest{}, fmt.Errorf("%s given more than once", name)
		}
	}
	req := changesRequest{from: q.Get("from"), limit: DefaultChangesLimit}
	if req.from == "" {
		return changesRequest{}, errors.New("from is required: a cursor, oldest or head")
	}
	if q.Has("limit") {
		n, err := strconv.Atoi(q.Get("limit"))
		if err != nil || n < 1 || n > MaxChangesLimit {
			return changesRequest{}, fmt.Errorf("limit %q, want 1 to %d", q.Get("limit"), MaxChangesLimit)
		}
		req.limit = n
	}
	return req, nil
}

type changeItem struct {
	Sequence    uint64     `json:"sequence"`
	Revision    uint64     `json:"revision"`
	Op          string     `json:"op"`
	Key         *string    `json:"key,omitempty"`
	KeyBase64   []byte     `json:"key_base64,omitempty"`
	Value       *string    `json:"value,omitempty"`
	ValueBase64 []byte     `json:"value_base64,omitempty"`
	ExpiresAt   *time.Time `json:"expires_at,omitempty"`
}

type changesResponse struct {
	Changes  []changeItem `json:"changes"`
	Next     string       `json:"next"`
	CaughtUp bool         `json:"caught_up"`
}

type cursorExpiredResponse struct {
	errorResponse
	Oldest string `json:"oldest,omitempty"`
}

func newChangeItem(ch isledb.Change) changeItem {
	item := changeItem{Sequence: ch.Sequence, Revision: uint64(ch.Revision), Op: "put"}
	if ch.Operation == isledb.ChangeDelete {
		item.Op = "delete"
	}
	item.Key, item.KeyBase64 = textOrBase64(ch.Key)
	if ch.HasValue {
		item.Value, item.ValueBase64 = textOrBase64(ch.Value)
	}
	if !ch.ExpiresAt.IsZero() {
		at := ch.ExpiresAt.UTC()
		item.ExpiresAt = &at
	}
	return item
}

// serveChanges answers one page of the feed. A cursor at the head of the
// reader's view, or at the newer head the feed has loaded, is answered
// without a read: this replica has nothing newer for it.
func (h *Handler) serveChanges(w http.ResponseWriter, r *http.Request, req changesRequest) {
	ctx, cancel := context.WithTimeout(r.Context(), h.readTimeout)
	defer cancel()
	head, err := h.reader.ChangeHead(ctx)
	if errors.Is(err, isledb.ErrChangeFeedDisabled) {
		feedOff(w)
		return
	}
	if err != nil {
		readFailed(w, err)
		return
	}
	if req.from == "head" {
		writeJSON(w, http.StatusOK, changesResponse{Changes: []changeItem{}, Next: head.String(), CaughtUp: true})
		return
	}
	var cursor isledb.ChangeCursor // zero starts at the oldest change kept
	if req.from != "oldest" {
		if cursor, err = isledb.ParseChangeCursor(req.from); err != nil {
			writeError(w, http.StatusBadRequest, ErrorBadRequest, "from is not a cursor from this feed, oldest or head")
			return
		}
		// The feed's loaded head can be past the view's, from a page read on
		// a newer CURRENT: a consumer there is echoed, so it never moves back.
		// A cursor between the two heads is read: the feed's loaded view has
		// changes after it, and serves them without reading CURRENT. One past
		// anything loaded goes to storage, which can refuse it.
		loaded := h.changes.loadedHead()
		if cursor.Compare(head) == 0 || cursor.Compare(loaded) == 0 && loaded.Compare(head) > 0 {
			writeJSON(w, http.StatusOK, changesResponse{Changes: []changeItem{}, Next: cursor.String(), CaughtUp: true})
			return
		}
	}
	cr, err := h.changes.changeReader(ctx)
	if errors.Is(err, isledb.ErrChangeFeedDisabled) {
		feedOff(w)
		return
	}
	if err != nil {
		readFailed(w, err)
		return
	}
	page, err := cr.Read(ctx, cursor, isledb.ChangeReadOptions{MaxChanges: req.limit, MaxBytes: MaxPageBytes})
	switch {
	case errors.Is(err, isledb.ErrChangeCursorExpired):
		resp := cursorExpiredResponse{errorResponse: errorResponse{Error: ErrorCursorExpired,
			Message: "changes after this cursor are no longer kept: restart from oldest, or rebuild from a scan"}}
		if bounds, err := cr.Bounds(ctx); err == nil {
			resp.Oldest = bounds.Oldest.String()
		}
		writeJSON(w, http.StatusGone, resp)
		return
	case errors.Is(err, isledb.ErrInvalidChangeCursor):
		writeError(w, http.StatusBadRequest, ErrorBadRequest, "from is not a cursor from this feed, oldest or head")
		return
	case errors.Is(err, isledb.ErrChangeFeedDisabled):
		feedOff(w)
		return
	case err != nil:
		readFailed(w, err)
		return
	}
	resp := changesResponse{Changes: make([]changeItem, 0, len(page.Changes)), Next: page.Next.String(), CaughtUp: page.CaughtUp()}
	for _, ch := range page.Changes {
		resp.Changes = append(resp.Changes, newChangeItem(ch))
	}
	writeJSON(w, http.StatusOK, resp)
}

func feedOff(w http.ResponseWriter) {
	writeError(w, http.StatusNotFound, ErrorChangeFeedOff, "the database has no change feed")
}
