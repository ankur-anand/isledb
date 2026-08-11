package manifest

import (
	"context"
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

// InspectCommitPage validates an immutable page object and reconstructs the
// exact reference readers would use for it. Lifecycle maintenance uses this
// instead of duplicating envelope, checksum, and structural validation.
func InspectCommitPage(path string, data []byte) (PageRef, *CommitPage, error) {
	page, err := DecodeCommitPage(data)
	if err != nil {
		return PageRef{}, nil, err
	}
	if err := validateCommitPage(page, path); err != nil {
		return PageRef{}, nil, err
	}
	object, err := newManifestObjectRef(path, data, manifestObjectKindPage, page.CreatedAt)
	if err != nil {
		return PageRef{}, nil, err
	}
	return PageRef{
		ObjectRef: object,
		Level:     page.Level,
		SeqLo:     page.SeqLo,
		SeqHi:     page.SeqHi,
		Count:     page.Count,
	}, page, nil
}

// IsPageReachable determines whether candidate is in CURRENT's immutable page
// graph. It follows at most one child per frontier root because page ranges at
// each index level are non-overlapping. The returned read count is useful for
// bounding and testing lifecycle cost.
func (s *Store) IsPageReachable(ctx context.Context, current *Current, candidate PageRef) (bool, int, error) {
	if err := validatePageRef(candidate); err != nil {
		return false, 0, err
	}
	if current == nil {
		return false, 0, nil
	}
	reads := 0
	for _, root := range current.IndexFrontier {
		if root.SeqHi < candidate.SeqLo || root.SeqLo > candidate.SeqHi || root.Level < candidate.Level {
			continue
		}
		if root.Level == candidate.Level {
			equal, err := matchingPageRef(root, candidate)
			if err != nil {
				return false, reads, err
			}
			if equal {
				return true, reads, nil
			}
			if root.SeqLo != candidate.SeqLo || root.SeqHi != candidate.SeqHi {
				return false, reads, fmt.Errorf("%w: candidate page range [%d,%d] partially overlaps peer %q [%d,%d]",
					ErrInvalidManifest, candidate.SeqLo, candidate.SeqHi, root.Path, root.SeqLo, root.SeqHi)
			}
			continue
		}
		if root.SeqLo > candidate.SeqLo || root.SeqHi < candidate.SeqHi {
			return false, reads, fmt.Errorf("%w: candidate page range [%d,%d] partially overlaps root %q [%d,%d]",
				ErrInvalidManifest, candidate.SeqLo, candidate.SeqHi, root.Path, root.SeqLo, root.SeqHi)
		}
		reachable, pageReads, err := s.pageReachableFrom(ctx, root, candidate)
		reads += pageReads
		if err != nil {
			return false, reads, err
		}
		if reachable {
			return true, reads, nil
		}
	}
	return false, reads, nil
}

func (s *Store) pageReachableFrom(ctx context.Context, ref, candidate PageRef) (bool, int, error) {
	if ref.Level <= candidate.Level {
		equal, err := matchingPageRef(ref, candidate)
		return equal, 0, err
	}
	pages, ok := s.storage.(PageStorage)
	if !ok {
		return false, 0, fmt.Errorf("manifest page storage unsupported")
	}
	data, err := pages.ReadPage(ctx, ref.Path)
	if err != nil {
		return false, 1, err
	}
	if err := verifyManifestObjectRef(data, ref.ObjectRef, manifestObjectKindPage); err != nil {
		return false, 1, err
	}
	page, err := DecodeCommitPage(data)
	if err != nil {
		return false, 1, err
	}
	if page.Level != ref.Level || page.SeqLo != ref.SeqLo || page.SeqHi != ref.SeqHi || page.Count != ref.Count {
		return false, 1, fmt.Errorf("%w: manifest page ref mismatch path=%q", ErrInvalidManifest, ref.Path)
	}
	if err := validateCommitPage(page, ref.Path); err != nil {
		return false, 1, err
	}
	for _, child := range page.Children {
		if child.SeqHi < candidate.SeqLo || child.SeqLo > candidate.SeqHi || child.Level < candidate.Level {
			continue
		}
		if child.Level == candidate.Level {
			equal, err := matchingPageRef(child, candidate)
			if err == nil && !equal && (child.SeqLo != candidate.SeqLo || child.SeqHi != candidate.SeqHi) {
				err = fmt.Errorf("%w: candidate page range [%d,%d] partially overlaps peer %q [%d,%d]",
					ErrInvalidManifest, candidate.SeqLo, candidate.SeqHi, child.Path, child.SeqLo, child.SeqHi)
			}
			return equal, 1, err
		}
		if child.SeqLo > candidate.SeqLo || child.SeqHi < candidate.SeqHi {
			return false, 1, fmt.Errorf("%w: candidate page range [%d,%d] partially overlaps child %q [%d,%d]",
				ErrInvalidManifest, candidate.SeqLo, candidate.SeqHi, child.Path, child.SeqLo, child.SeqHi)
		}
		reachable, reads, err := s.pageReachableFrom(ctx, child, candidate)
		return reachable, reads + 1, err
	}
	return false, 1, nil
}

func matchingPageRef(live, candidate PageRef) (bool, error) {
	if live.Path != candidate.Path {
		return false, nil
	}
	if live != candidate {
		return false, fmt.Errorf("%w: page identity mismatch path=%q", ErrInvalidManifest, live.Path)
	}
	return true, nil
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

// ValidatePageRef validates the complete immutable identity and sequence
// metadata carried by a page reference.
func ValidatePageRef(ref PageRef) error {
	return validatePageRef(ref)
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
