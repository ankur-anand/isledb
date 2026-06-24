package isledb

import (
	"bytes"
	"testing"
)

func TestIncrementKey_AllFF(t *testing.T) {
	in := []byte{0xFF, 0xFF}
	got := incrementKey(in)
	want := []byte{0xFF, 0xFF, 0x00}

	if !bytes.Equal(got, want) {
		t.Fatalf("incrementKey(all-ff): got %v want %v", got, want)
	}

	if !bytes.Equal(in, []byte{0xFF, 0xFF}) {
		t.Fatalf("incrementKey should not modify input, got %v", in)
	}
}

func TestIncrementKey(t *testing.T) {
	tests := []struct {
		input []byte
		want  []byte
	}{
		{[]byte("a"), []byte("b")},
		{[]byte("abc"), []byte("abd")},
		{[]byte{0xFF}, []byte{0xFF, 0x00}},
		{[]byte{0x01, 0xFF}, []byte{0x02, 0x00}},
		{[]byte("key:000"), []byte("key:001")},
	}

	for _, tt := range tests {
		got := incrementKey(tt.input)
		if !bytes.Equal(got, tt.want) {
			t.Fatalf("incrementKey(%q): got=%q want=%q", tt.input, got, tt.want)
		}
	}
}
