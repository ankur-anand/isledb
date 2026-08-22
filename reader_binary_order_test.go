package isledb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/ankur-anand/isledb/blobstore"
)

func TestReaderBinaryKeyOrdering(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("reader-binary-order")
	defer store.Close()

	manifestStore := newManifestStore(store, nil)
	writerOptions := DefaultWriterOptions()
	writerOptions.Flush.Interval = 0
	writer, err := newWriter(ctx, store, manifestStore, writerOptions)
	if err != nil {
		t.Fatalf("newWriter: %v", err)
	}
	defer writer.close(ctx)

	keys := binaryOrderingTestKeys()
	values := make(map[string][]byte, len(keys))
	// Insert in reverse order so the test also exercises memtable/SST sorting,
	// rather than merely preserving already-sorted input.
	for index := len(keys) - 1; index >= 0; index-- {
		value := fmt.Appendf(nil, "value-%02d", index)
		values[string(keys[index])] = value
		if err := writer.put(ctx, keys[index], value); err != nil {
			t.Fatalf("put key %x: %v", keys[index], err)
		}
	}
	if err := writer.flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	readerOptions := defaultReaderOptions()
	readerOptions.CacheDir = t.TempDir()
	reader, err := newReader(ctx, store, readerOptions)
	if err != nil {
		t.Fatalf("newReader: %v", err)
	}
	defer reader.Close()

	rows, err := reader.Scan(ctx, nil, nil)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	assertBinaryOrderingRows(t, rows, keys, values)

	bounded, err := reader.Scan(ctx, []byte{'a'}, []byte{'b'})
	if err != nil {
		t.Fatalf("Scan [a,b]: %v", err)
	}
	assertBinaryOrderingRows(t, bounded, keys[2:7], values)

	iterator, err := reader.NewIterator(ctx, IteratorOptions{})
	if err != nil {
		t.Fatalf("NewIterator: %v", err)
	}
	var iterRows []KV
	for iterator.Next() {
		iterRows = append(iterRows, KV{
			Key:   append([]byte(nil), iterator.Key()...),
			Value: append([]byte(nil), iterator.Value()...),
		})
	}
	iterErr := iterator.Err()
	closeErr := iterator.Close()
	if iterErr != nil || closeErr != nil {
		t.Fatalf("iterate: %v", errors.Join(iterErr, closeErr))
	}
	assertBinaryOrderingRows(t, iterRows, keys, values)

	seekCases := []struct {
		name   string
		target []byte
		want   []byte
	}{
		{name: "exact-null", target: []byte{0x00}, want: []byte{0x00}},
		{name: "binary-gap", target: []byte{0x00, 0x01}, want: []byte{0x00, 0xff}},
		{name: "embedded-null", target: []byte{'a', 0x00}, want: []byte{'a', 0x00}},
		{name: "prefix-gap", target: []byte{'a', 0x01}, want: []byte{'a', 'a'}},
		{name: "high-bit-gap", target: []byte{0x81}, want: []byte{0xff}},
		{name: "past-end", target: []byte{0xff, 0xff}},
	}
	for _, testCase := range seekCases {
		t.Run("seek/"+testCase.name, func(t *testing.T) {
			iterator, err := reader.NewIterator(ctx, IteratorOptions{})
			if err != nil {
				t.Fatalf("NewIterator: %v", err)
			}
			found := iterator.SeekGE(testCase.target)
			var got []byte
			if found {
				got = append([]byte(nil), iterator.Key()...)
			}
			iterErr := iterator.Err()
			closeErr := iterator.Close()
			if iterErr != nil || closeErr != nil {
				t.Fatalf("SeekGE(%x): %v", testCase.target, errors.Join(iterErr, closeErr))
			}
			if testCase.want == nil {
				if found {
					t.Fatalf("SeekGE(%x)=%x, want exhausted", testCase.target, got)
				}
				return
			}
			if !found || !bytes.Equal(got, testCase.want) {
				t.Fatalf("SeekGE(%x) found=%t key=%x, want=%x",
					testCase.target, found, got, testCase.want)
			}
		})
	}
}

func binaryOrderingTestKeys() [][]byte {
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

func assertBinaryOrderingRows(
	t *testing.T,
	rows []KV,
	keys [][]byte,
	values map[string][]byte,
) {
	t.Helper()
	if len(rows) != len(keys) {
		t.Fatalf("row count=%d, want=%d; rows=%x keys=%x", len(rows), len(keys), rows, keys)
	}
	for index, key := range keys {
		if index > 0 && bytes.Compare(rows[index-1].Key, rows[index].Key) >= 0 {
			t.Fatalf("keys not strictly ordered at %d: %x then %x",
				index, rows[index-1].Key, rows[index].Key)
		}
		if !bytes.Equal(rows[index].Key, key) ||
			!bytes.Equal(rows[index].Value, values[string(key)]) {
			t.Fatalf("row[%d]=(%x,%x), want=(%x,%x)",
				index, rows[index].Key, rows[index].Value, key, values[string(key)])
		}
	}
}
