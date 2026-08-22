package manifest

import (
	"strings"
	"testing"
)

const testArtifactChecksum = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func validArtifactSSTMeta() SSTMeta {
	return SSTMeta{
		ID:       "sst-a",
		Size:     1024,
		Checksum: testArtifactChecksum,
		Bloom: BloomMeta{
			BitsPerKey: 10,
			K:          6,
			Offset:     1024,
			Length:     128,
			Checksum:   testArtifactChecksum,
		},
	}
}

func TestManifestValidateLevelsValidatesArtifacts(t *testing.T) {
	valid := validArtifactSSTMeta()
	tests := []struct {
		name string
		edit func(*SSTMeta)
		want string
	}{
		{name: "valid"},
		{name: "missing SST checksum", edit: func(sst *SSTMeta) {
			sst.Checksum = ""
		}, want: "invalid SST checksum"},
		{name: "malformed SST checksum", edit: func(sst *SSTMeta) {
			sst.Checksum = "sha256:not-a-digest"
		}, want: "invalid SST checksum"},
		{name: "missing Bloom checksum", edit: func(sst *SSTMeta) {
			sst.Bloom.Checksum = ""
		}, want: "invalid Bloom checksum"},
		{name: "Bloom outside payload boundary", edit: func(sst *SSTMeta) {
			sst.Bloom.Offset++
		}, want: "Bloom offset"},
		{name: "negative Bloom length", edit: func(sst *SSTMeta) {
			sst.Bloom.Length = -1
		}, want: "invalid Bloom length"},
		{name: "absent Bloom", edit: func(sst *SSTMeta) {
			sst.Bloom = BloomMeta{Offset: sst.Size}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sst := valid
			if test.edit != nil {
				test.edit(&sst)
			}
			err := (&Manifest{L0SSTs: []SSTMeta{sst}}).validateLevels(true)
			if test.want == "" {
				if err != nil {
					t.Fatalf("validateLevels: %v", err)
				}
				return
			}
			if !strings.Contains(err.Error(), test.want) || !strings.Contains(err.Error(), sst.ID) {
				t.Fatalf("validateLevels error=%v want error containing %q and SST ID", err, test.want)
			}
		})
	}
}

func TestManifestValidateLevelsChecksSortedLevelArtifacts(t *testing.T) {
	sst := validArtifactSSTMeta()
	sst.ID = "level-sst"
	sst.Level = 2
	sst.Bloom.Checksum = ""
	err := (&Manifest{Levels: []Level{{Number: 2, SSTs: []SSTMeta{sst}}}}).validateLevels(true)
	if !strings.Contains(err.Error(), `L2 SST "level-sst"`) {
		t.Fatalf("validateLevels error=%v", err)
	}
}
