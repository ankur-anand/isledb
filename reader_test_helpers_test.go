package isledb

import (
	"context"
	"testing"

	"github.com/ankur-anand/isledb/blobstore"
)

func openReaderFromDBForTest(tb testing.TB, ctx context.Context, store *blobstore.Store, opts ReaderOpenOptions) *Reader {
	tb.Helper()

	db, err := openDB(ctx, store, dbOpenOptions{})
	if err != nil {
		tb.Fatalf("OpenDB for reader: %v", err)
	}
	tb.Cleanup(func() { _ = db.Close() })

	reader, err := db.OpenReader(ctx, opts)
	if err != nil {
		tb.Fatalf("DB.OpenReader: %v", err)
	}
	return reader
}
