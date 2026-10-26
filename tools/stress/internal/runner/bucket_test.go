package runner

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// The signing examples of the Amazon S3 API reference, "Signature
// Calculations for the Authorization Header".
func TestSignMatchesAWSExamples(t *testing.T) {
	c := &s3Lister{region: "us-east-1", access: "AKIAIOSFODNN7EXAMPLE", secret: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"}
	at := time.Date(2013, 5, 24, 0, 0, 0, 0, time.UTC)
	for _, ex := range []struct {
		url, rng, want string
	}{
		{"https://examplebucket.s3.amazonaws.com/test.txt", "bytes=0-9", "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"},
		{"https://examplebucket.s3.amazonaws.com/?max-keys=2&prefix=J", "", "34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670ed5711ef69dc6f7"},
	} {
		req, _ := http.NewRequest(http.MethodGet, ex.url, nil)
		if ex.rng != "" {
			req.Header.Set("Range", ex.rng)
		}
		c.sign(req, at)
		if got := req.Header.Get("Authorization"); !strings.HasSuffix(got, "Signature="+ex.want) {
			t.Errorf("%s: %s, want signature %s", ex.url, got, ex.want)
		}
	}
}

func TestDirectoryListingByKind(t *testing.T) {
	root := t.TempDir()
	for path, size := range map[string]int{
		"stress/run/sstable/00/a.sst":   100,
		"stress/run/sstable/01/b.sst":   50,
		"stress/run/manifest/CURRENT":   5,
		"stress/run/manifest/gc/x":      3,
		"stress/run/changes/0/c":        7,
		"stress/other/sstable/00/z.sst": 1000,
	} {
		p := filepath.Join(root, filepath.FromSlash(path))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, make([]byte, size), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	l, why := newLister(StorageSpec{BucketURL: "file://" + root})
	if l == nil {
		t.Fatal(why)
	}
	u, err := listUsage(context.Background(), l, "stress/run")
	if err != nil {
		t.Fatal(err)
	}
	if u.Bytes["sst"] != 150 || u.Objects["sst"] != 2 || u.Bytes["manifest"] != 5 || u.Bytes["gc"] != 3 || u.Bytes["change_feed"] != 7 {
		t.Fatalf("usage %+v", u)
	}
	if u, err := listUsage(context.Background(), l, "stress/none"); err != nil || len(u.Objects) != 0 {
		t.Fatalf("a prefix with nothing: %+v %v", u, err)
	}
	if l, why := newLister(StorageSpec{BucketURL: "gs://b"}); l != nil || why == "" {
		t.Fatal("gs:// listing should be refused with a reason")
	}
}
