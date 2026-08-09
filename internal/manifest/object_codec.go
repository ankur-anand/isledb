package manifest

import (
	"encoding/binary"
	"fmt"
	"math"
	"sync"

	"github.com/klauspost/compress/zstd"
)

const (
	manifestObjectHeaderBytes = 16
	manifestObjectVersion     = 1
	manifestObjectCodecZstd   = 1

	manifestObjectKindSnapshot = 1
	manifestObjectKindPage     = 2

	maxManifestPageRawBytes     = 32 << 20
	maxManifestSnapshotRawBytes = 512 << 20
)

var manifestObjectMagic = [4]byte{'I', 'S', 'L', 'M'}

var manifestObjectEncoderPool = sync.Pool{New: func() any {
	encoder, err := zstd.NewWriter(nil,
		zstd.WithEncoderConcurrency(1),
		zstd.WithEncoderCRC(false),
		zstd.WithEncoderLevel(zstd.SpeedFastest),
		zstd.WithZeroFrames(true),
	)
	if err != nil {
		panic(fmt.Errorf("manifest zstd encoder: %w", err))
	}
	return encoder
}}

var manifestObjectDecoderPool = sync.Pool{New: func() any {
	decoder, err := zstd.NewReader(nil,
		zstd.WithDecoderConcurrency(1),
		zstd.WithDecodeAllCapLimit(true),
	)
	if err != nil {
		panic(fmt.Errorf("manifest zstd decoder: %w", err))
	}
	return decoder
}}

func encodeManifestObject(raw []byte, kind byte, maxRawBytes uint64) ([]byte, error) {
	if len(raw) == 0 {
		return nil, fmt.Errorf("%w: empty immutable manifest object", ErrInvalidManifest)
	}
	if uint64(len(raw)) > maxRawBytes {
		return nil, fmt.Errorf("%w: immutable manifest raw bytes=%d limit=%d", ErrInvalidManifest, len(raw), maxRawBytes)
	}
	if kind != manifestObjectKindSnapshot && kind != manifestObjectKindPage {
		return nil, fmt.Errorf("%w: immutable manifest kind=%d", ErrInvalidManifest, kind)
	}

	encoded := make([]byte, manifestObjectHeaderBytes)
	encoder := manifestObjectEncoderPool.Get().(*zstd.Encoder)
	encoded = encoder.EncodeAll(raw, encoded)
	manifestObjectEncoderPool.Put(encoder)

	copy(encoded[:4], manifestObjectMagic[:])
	encoded[4] = manifestObjectVersion
	encoded[5] = kind
	encoded[6] = manifestObjectCodecZstd
	encoded[7] = 0
	binary.BigEndian.PutUint64(encoded[8:manifestObjectHeaderBytes], uint64(len(raw)))
	return encoded, nil
}

func decodeManifestObject(encoded []byte, wantKind byte, maxRawBytes uint64) ([]byte, error) {
	rawBytes, err := validateManifestObjectHeader(encoded, wantKind, maxRawBytes)
	if err != nil {
		return nil, err
	}
	if rawBytes > uint64(math.MaxInt) {
		return nil, fmt.Errorf("%w: immutable manifest raw bytes=%d exceed platform limit", ErrInvalidManifest, rawBytes)
	}

	destination := make([]byte, 0, int(rawBytes))
	decoder := manifestObjectDecoderPool.Get().(*zstd.Decoder)
	decoded, decodeErr := decoder.DecodeAll(encoded[manifestObjectHeaderBytes:], destination)
	manifestObjectDecoderPool.Put(decoder)
	if decodeErr != nil {
		return nil, fmt.Errorf("%w: decompress immutable manifest object: %v", ErrInvalidManifest, decodeErr)
	}
	if uint64(len(decoded)) != rawBytes {
		return nil, fmt.Errorf("%w: immutable manifest decoded bytes=%d want=%d", ErrInvalidManifest, len(decoded), rawBytes)
	}
	return decoded, nil
}

func manifestObjectRawBytes(encoded []byte, wantKind byte, maxRawBytes uint64) (uint64, error) {
	return validateManifestObjectHeader(encoded, wantKind, maxRawBytes)
}

func validateManifestObjectHeader(encoded []byte, wantKind byte, maxRawBytes uint64) (uint64, error) {
	if len(encoded) <= manifestObjectHeaderBytes {
		return 0, fmt.Errorf("%w: immutable manifest object is truncated", ErrInvalidManifest)
	}
	if encoded[0] != manifestObjectMagic[0] || encoded[1] != manifestObjectMagic[1] ||
		encoded[2] != manifestObjectMagic[2] || encoded[3] != manifestObjectMagic[3] {
		return 0, fmt.Errorf("%w: immutable manifest object magic mismatch", ErrInvalidManifest)
	}
	if encoded[4] != manifestObjectVersion {
		return 0, fmt.Errorf("%w: immutable manifest object version=%d", ErrInvalidManifest, encoded[4])
	}
	if encoded[5] != wantKind {
		return 0, fmt.Errorf("%w: immutable manifest object kind=%d want=%d", ErrInvalidManifest, encoded[5], wantKind)
	}
	if encoded[6] != manifestObjectCodecZstd {
		return 0, fmt.Errorf("%w: immutable manifest object codec=%d", ErrInvalidManifest, encoded[6])
	}
	if encoded[7] != 0 {
		return 0, fmt.Errorf("%w: immutable manifest object flags=%d", ErrInvalidManifest, encoded[7])
	}
	rawBytes := binary.BigEndian.Uint64(encoded[8:manifestObjectHeaderBytes])
	if rawBytes == 0 || rawBytes > maxRawBytes {
		return 0, fmt.Errorf("%w: immutable manifest raw bytes=%d limit=%d", ErrInvalidManifest, rawBytes, maxRawBytes)
	}
	return rawBytes, nil
}
