package internal

import "testing"

func TestOverlapsRange(t *testing.T) {
	tests := []struct {
		name string
		aMin []byte
		aMax []byte
		bMin []byte
		bMax []byte
		want bool
	}{
		{
			name: "invalid a range",
			aMin: nil,
			aMax: []byte("z"),
			bMin: []byte("a"),
			bMax: []byte("z"),
			want: false,
		},
		{
			name: "unbounded query overlaps",
			aMin: []byte("a"),
			aMax: []byte("z"),
			bMin: nil,
			bMax: nil,
			want: true,
		},
		{
			name: "only upper bound overlaps",
			aMin: []byte("a"),
			aMax: []byte("m"),
			bMin: nil,
			bMax: []byte("b"),
			want: true,
		},
		{
			name: "only upper bound does not overlap",
			aMin: []byte("c"),
			aMax: []byte("m"),
			bMin: nil,
			bMax: []byte("b"),
			want: false,
		},
		{
			name: "only lower bound overlaps",
			aMin: []byte("d"),
			aMax: []byte("m"),
			bMin: []byte("m"),
			bMax: nil,
			want: true,
		},
		{
			name: "only lower bound does not overlap",
			aMin: []byte("a"),
			aMax: []byte("l"),
			bMin: []byte("m"),
			bMax: nil,
			want: false,
		},
		{
			name: "bounded overlap",
			aMin: []byte("d"),
			aMax: []byte("k"),
			bMin: []byte("h"),
			bMax: []byte("z"),
			want: true,
		},
		{
			name: "bounded disjoint",
			aMin: []byte("a"),
			aMax: []byte("f"),
			bMin: []byte("g"),
			bMax: []byte("z"),
			want: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := OverlapsRange(tc.aMin, tc.aMax, tc.bMin, tc.bMax)
			if got != tc.want {
				t.Fatalf("OverlapsRange() = %v, want %v", got, tc.want)
			}
		})
	}
}
