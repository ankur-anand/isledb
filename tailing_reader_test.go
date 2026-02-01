package isledb

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
)

func TestTailingReader_AutoRefresh(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("")

	wOpts := DefaultWriterOptions()
	wOpts.FlushInterval = 0
	w, err := newWriter(ctx, store, wOpts)
	if err != nil {
		t.Fatalf("newWriter failed: %v", err)
	}
	defer w.close()

	if err := w.put([]byte("key:001"), []byte("value:001")); err != nil {
		t.Fatalf("put failed: %v", err)
	}
	if err := w.flush(ctx); err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	var refreshCount atomic.Int32
	trOpts := TailingReaderOptions{
		RefreshInterval: 50 * time.Millisecond,
		OnRefresh: func() {
			refreshCount.Add(1)
		},
		ReaderOptions: ReaderOptions{
			CacheDir: t.TempDir(),
		},
	}
	tr, err := newTailingReader(ctx, store, trOpts)
	if err != nil {
		t.Fatalf("newTailingReader failed: %v", err)
	}
	defer tr.Close()

	tr.Start()

	val, found, err := tr.Get(ctx, []byte("key:001"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !found || string(val) != "value:001" {
		t.Errorf("Expected value:001, got %s (found=%v)", val, found)
	}

	if err := w.put([]byte("key:002"), []byte("value:002")); err != nil {
		t.Fatalf("put failed: %v", err)
	}
	if err := w.flush(ctx); err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	time.Sleep(150 * time.Millisecond)

	val, found, err = tr.Get(ctx, []byte("key:002"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !found || string(val) != "value:002" {
		t.Errorf("Expected value:002, got %s (found=%v)", val, found)
	}

	if refreshCount.Load() == 0 {
		t.Error("OnRefresh should have been called")
	}
}

func TestTailingReader_Tail(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	store := blobstore.NewMemory("")

	wOpts := DefaultWriterOptions()
	wOpts.FlushInterval = 0
	w, err := newWriter(ctx, store, wOpts)
	if err != nil {
		t.Fatalf("newWriter failed: %v", err)
	}
	defer w.close()

	trOpts := TailingReaderOptions{
		RefreshInterval: 20 * time.Millisecond,
		ReaderOptions: ReaderOptions{
			CacheDir: t.TempDir(),
		},
	}
	tr, err := newTailingReader(ctx, store, trOpts)
	if err != nil {
		t.Fatalf("newTailingReader failed: %v", err)
	}
	defer tr.Close()

	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("wal:%03d", i)
		value := fmt.Sprintf("entry:%03d", i)
		if err := w.put([]byte(key), []byte(value)); err != nil {
			t.Fatalf("put failed: %v", err)
		}
	}
	if err := w.flush(ctx); err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	received := make([]KV, 0)
	var mu sync.Mutex
	tailCtx, tailCancel := context.WithCancel(ctx)

	go func() {
		tr.Tail(tailCtx, TailOptions{
			MinKey:       []byte("wal:"),
			MaxKey:       []byte("wal:~"),
			PollInterval: 20 * time.Millisecond,
		}, func(kv KV) error {
			mu.Lock()
			received = append(received, KV{
				Key:   append([]byte(nil), kv.Key...),
				Value: append([]byte(nil), kv.Value...),
			})
			mu.Unlock()
			return nil
		})
	}()

	time.Sleep(100 * time.Millisecond)

	for i := 5; i < 10; i++ {
		key := fmt.Sprintf("wal:%03d", i)
		value := fmt.Sprintf("entry:%03d", i)
		if err := w.put([]byte(key), []byte(value)); err != nil {
			t.Fatalf("put failed: %v", err)
		}
	}
	if err := w.flush(ctx); err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	time.Sleep(150 * time.Millisecond)

	tailCancel()
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	count := len(received)
	mu.Unlock()

	if count != 10 {
		t.Errorf("Expected 10 entries, got %d", count)
	}
}

func TestTailingReader_TailChannel(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	store := blobstore.NewMemory("")

	wOpts := DefaultWriterOptions()
	wOpts.FlushInterval = 0
	w, err := newWriter(ctx, store, wOpts)
	if err != nil {
		t.Fatalf("newWriter failed: %v", err)
	}
	defer w.close()

	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("stream:%03d", i)
		value := fmt.Sprintf("data:%03d", i)
		if err := w.put([]byte(key), []byte(value)); err != nil {
			t.Fatalf("put failed: %v", err)
		}
	}
	if err := w.flush(ctx); err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	tr, err := newTailingReader(ctx, store, TailingReaderOptions{
		RefreshInterval: 20 * time.Millisecond,
		ReaderOptions: ReaderOptions{
			CacheDir: t.TempDir(),
		},
	})
	if err != nil {
		t.Fatalf("newTailingReader failed: %v", err)
	}
	defer tr.Close()

	tailCtx, tailCancel := context.WithCancel(ctx)
	ch, errCh := tr.TailChannel(tailCtx, TailOptions{
		MinKey:       []byte("stream:"),
		MaxKey:       []byte("stream:~"),
		PollInterval: 20 * time.Millisecond,
	})

	received := 0
	timeout := time.After(500 * time.Millisecond)

loop:
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				break loop
			}
			received++
			if received >= 3 {
				tailCancel()
			}
		case <-timeout:
			tailCancel()
			break loop
		}
	}

	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("Unexpected error: %v", err)
		}
	default:
	}

	if received < 3 {
		t.Errorf("Expected at least 3 entries, got %d", received)
	}
}

func TestTailingReader_StartAfterKey(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("")

	wOpts := DefaultWriterOptions()
	wOpts.FlushInterval = 0
	w, err := newWriter(ctx, store, wOpts)
	if err != nil {
		t.Fatalf("newWriter failed: %v", err)
	}
	defer w.close()

	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("log:%03d", i)
		value := fmt.Sprintf("entry:%03d", i)
		if err := w.put([]byte(key), []byte(value)); err != nil {
			t.Fatalf("put failed: %v", err)
		}
	}
	if err := w.flush(ctx); err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	tr, err := newTailingReader(ctx, store, TailingReaderOptions{
		RefreshInterval: 50 * time.Millisecond,
		ReaderOptions: ReaderOptions{
			CacheDir: t.TempDir(),
		},
	})
	if err != nil {
		t.Fatalf("newTailingReader failed: %v", err)
	}
	defer tr.Close()

	tailCtx, tailCancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer tailCancel()

	var received []string
	tr.Tail(tailCtx, TailOptions{
		MinKey:        []byte("log:"),
		MaxKey:        []byte("log:~"),
		StartAfterKey: []byte("log:004"),
		PollInterval:  50 * time.Millisecond,
	}, func(kv KV) error {
		received = append(received, string(kv.Key))
		if len(received) >= 5 {
			return context.Canceled
		}
		return nil
	})

	if len(received) != 5 {
		t.Errorf("Expected 5 entries, got %d: %v", len(received), received)
	}
	if len(received) > 0 && received[0] != "log:005" {
		t.Errorf("First entry should be log:005, got %s", received[0])
	}
}

func TestIncrementKey(t *testing.T) {
	tests := []struct {
		input    []byte
		expected []byte
	}{
		{[]byte("a"), []byte("b")},
		{[]byte("abc"), []byte("abd")},
		{[]byte{0xFF}, []byte{0x00, 0x00}},
		{[]byte{0x01, 0xFF}, []byte{0x02, 0x00}},
		{[]byte("key:000"), []byte("key:001")},
	}

	for _, tt := range tests {
		result := incrementKey(tt.input)
		if string(result) != string(tt.expected) {
			t.Errorf("incrementKey(%q) = %q, want %q", tt.input, result, tt.expected)
		}
	}
}
