package isledb

import "github.com/ankur-anand/isledb/manifest"

const MaxKeySize = 8 * 1024

type Manifest = manifest.Manifest
type TieredConfig = manifest.TieredConfig
type SSTMeta = manifest.SSTMeta
type BloomMeta = manifest.BloomMeta
type SSTSignature = manifest.SSTSignature
type SortedRun = manifest.SortedRun
type CompactionLogPayload = manifest.CompactionLogPayload

func DefaultTieredConfig() TieredConfig {
	return manifest.DefaultTieredConfig()
}
