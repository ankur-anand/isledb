package isledb

import (
	"fmt"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal/manifest"
)

func retiredSSTObjects(store *blobstore.Store, m *manifestState, ids []string, gracePeriod time.Duration) ([]manifest.RetiredObject, error) {
	unique := uniqueSSTIDs(ids)
	if len(unique) > manifest.MaxRetiredObjectsPerEntry {
		return nil, fmt.Errorf("%w: count=%d max=%d", manifest.ErrInvalidRetirement, len(unique), manifest.MaxRetiredObjectsPerEntry)
	}
	if gracePeriod < 0 {
		gracePeriod = 0
	}
	notBefore := time.Now().UTC().Add(gracePeriod)
	retired := make([]manifest.RetiredObject, 0, len(unique))
	for _, id := range unique {
		meta := m.LookupSST(id)
		if meta == nil {
			return nil, fmt.Errorf("%w: sst id=%q is not live", manifest.ErrInvalidRetirement, id)
		}
		retired = append(retired, manifest.RetiredObject{
			Kind:      manifest.RetiredObjectSST,
			ID:        id,
			Key:       store.SSTPath(id),
			Size:      meta.Size,
			NotBefore: notBefore,
		})
	}
	return retired, nil
}
