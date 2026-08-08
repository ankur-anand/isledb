package isledb

import (
	"fmt"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal/manifest"
)

func retiredSSTObjects(store *blobstore.Store, m *manifestState, ids []string) ([]manifest.RetiredObject, error) {
	unique := uniqueSSTIDs(ids)
	if len(unique) > manifest.MaxRetiredObjectsPerEntry {
		return nil, fmt.Errorf("%w: count=%d max=%d", manifest.ErrInvalidRetirement, len(unique), manifest.MaxRetiredObjectsPerEntry)
	}
	retired := make([]manifest.RetiredObject, 0, len(unique))
	for _, id := range unique {
		meta := m.LookupSST(id)
		if meta == nil {
			return nil, fmt.Errorf("%w: sst id=%q is not live", manifest.ErrInvalidRetirement, id)
		}
		retired = append(retired, manifest.RetiredObject{
			Kind: manifest.RetiredObjectSST,
			ID:   id,
			Key:  store.SSTPath(id),
			Size: meta.Size,
		})
	}
	return retired, nil
}
