package manifest

import (
	"encoding/json"
)

const (
	LayoutVersion = 1
	CurrentFormat = "isledb-manifest-v1"

	CommitPageTypeLeaf  = "commit_l00"
	CommitPageTypeIndex = "commit_index"
)

func EncodeCommitPage(p *CommitPage) ([]byte, error) {
	return json.Marshal(p)
}

func DecodeCommitPage(data []byte) (*CommitPage, error) {
	var p CommitPage
	if err := json.Unmarshal(data, &p); err != nil {
		return nil, err
	}
	return &p, nil
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
	if c.ChangeFeedLogStart == 0 {
		c.ChangeFeedLogStart = c.LogSeqStart
	}
}
