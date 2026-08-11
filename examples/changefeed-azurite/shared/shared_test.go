package shared

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/ankur-anand/isledb"
)

func TestCursorRoundTripAndReplace(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "consumer.cursor")
	first := mustCursor(t, "cf1_AAAAAAAAAAEAAAAAAAAAAg")
	second := mustCursor(t, "cf1_AAAAAAAAAAMAAAAAAAAABA")

	if err := SaveCursor(path, first); err != nil {
		t.Fatalf("save first cursor: %v", err)
	}
	if err := SaveCursor(path, second); err != nil {
		t.Fatalf("replace cursor: %v", err)
	}
	got, err := LoadCursor(path)
	if err != nil {
		t.Fatalf("load cursor: %v", err)
	}
	if got.String() != second.String() {
		t.Fatalf("cursor=%q, want=%q", got.String(), second.String())
	}
}

func TestLoadCursorMissingAndMalformed(t *testing.T) {
	missing, err := LoadCursor(filepath.Join(t.TempDir(), "missing"))
	if err != nil {
		t.Fatalf("load missing cursor: %v", err)
	}
	if !missing.IsZero() {
		t.Fatalf("missing cursor=%q, want zero", missing.String())
	}

	path := filepath.Join(t.TempDir(), "bad.cursor")
	if err := os.WriteFile(path, []byte("not-a-cursor"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadCursor(path); !errors.Is(err, isledb.ErrInvalidChangeCursor) {
		t.Fatalf("load malformed error=%v, want ErrInvalidChangeCursor", err)
	}
}

func mustCursor(t *testing.T, value string) isledb.ChangeCursor {
	t.Helper()
	cursor, err := isledb.ParseChangeCursor(value)
	if err != nil {
		t.Fatalf("parse test cursor: %v", err)
	}
	return cursor
}
