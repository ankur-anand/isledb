package isledb

// SSTHashSigner signs SST hash bytes for integrity verification.
type SSTHashSigner interface {
	Algorithm() string
	KeyID() string
	SignHash(hash []byte) ([]byte, error)
}

// SSTHashVerifier verifies SST hash signatures for integrity checks.
type SSTHashVerifier interface {
	VerifyHash(hash []byte, sig SSTSignature) error
}
