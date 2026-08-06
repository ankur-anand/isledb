package isledb

import (
	"context"

	"github.com/ankur-anand/isledb/manifest"
)

type maintenanceCommandStager func(context.Context, manifest.MaintenanceCommand) error
