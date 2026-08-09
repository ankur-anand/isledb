package manifest

import (
	"encoding/json"
	"fmt"
	"time"
)

const (
	LayoutVersion = 2
	CurrentFormat = "isledb-manifest-v2"

	CommitPageTypeLeaf  = "commit_l00"
	CommitPageTypeIndex = "commit_index"

	DefaultMaxPinnedViewAge = time.Hour
	retirementSafetyMargin  = time.Minute
)

func EncodeCommitPage(p *CommitPage) ([]byte, error) {
	if p == nil {
		return nil, fmt.Errorf("%w: nil manifest page", ErrInvalidManifest)
	}
	raw, err := json.Marshal(p)
	if err != nil {
		return nil, err
	}
	return encodeManifestObject(raw, manifestObjectKindPage, maxManifestPageRawBytes)
}

func DecodeCommitPage(data []byte) (*CommitPage, error) {
	raw, err := decodeManifestObject(data, manifestObjectKindPage, maxManifestPageRawBytes)
	if err != nil {
		return nil, err
	}
	var p CommitPage
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, err
	}
	if p.LayoutVersion != LayoutVersion {
		return nil, fmt.Errorf("%w: manifest page layout version=%d", ErrInvalidManifest, p.LayoutVersion)
	}
	return &p, nil
}

func validatePageRef(ref PageRef) error {
	if err := validateManifestObjectRef(ref.ObjectRef, manifestObjectKindPage); err != nil {
		return err
	}
	if ref.Count == 0 || ref.SeqHi < ref.SeqLo {
		return fmt.Errorf("%w: invalid manifest page reference path=%q range=[%d,%d] count=%d",
			ErrInvalidManifest, ref.Path, ref.SeqLo, ref.SeqHi, ref.Count)
	}
	return nil
}

func normalizeCurrent(c *Current) {
	if c == nil {
		return
	}
	if c.LayoutVersion == 0 {
		c.LayoutVersion = LayoutVersion
	}
	if c.Format == "" {
		c.Format = CurrentFormat
	}
	if c.NextEpoch == 0 {
		c.NextEpoch = 1
	}
	if c.MaxPinnedViewAge == 0 {
		c.MaxPinnedViewAge = DefaultMaxPinnedViewAge
	}
	if !c.ChangeFeedEnabled && c.ChangeFeedLogStart == 0 {
		c.ChangeFeedLogStart = c.LogSeqStart
	}
}

func (c *Current) PinnedViewAge() time.Duration {
	if c == nil || c.MaxPinnedViewAge == 0 {
		return DefaultMaxPinnedViewAge
	}
	return c.MaxPinnedViewAge
}
