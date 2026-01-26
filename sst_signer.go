package isledb

// SSTHashSigner signs SST hash bytes for integrity verification.
type SSTHashSigner interface {
	Algorithm() string
	KeyID() string
	SignHash(hash []byte) ([]byte, error)
}
