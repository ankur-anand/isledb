package isledb

// SSTHashSigner signs SST hash bytes for integrity verification.
type SSTHashSigner interface {
	Algorithm() string
	KeyID() string
	SignHash(hash []byte) ([]byte, error)
}

// SSTSignature stores the signature metadata for an SST.
type SSTSignature struct {
	Algorithm string
	KeyID     string
	Hash      string
	Signature []byte
}
