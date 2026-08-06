package isledb

import "testing"

func TestReaderOpenOptionsMapsBlobVerification(t *testing.T) {
	opts := DefaultReaderOpenOptions(t.TempDir())
	opts.VerifyBlobsOnRead = true

	internal, err := readerOptionsFromPublic(opts)
	if err != nil {
		t.Fatalf("readerOptionsFromPublic: %v", err)
	}
	if !internal.ValueStorageConfig.VerifyBlobsOnRead {
		t.Fatal("VerifyBlobsOnRead was not propagated to internal value storage")
	}
}
