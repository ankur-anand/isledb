package shared

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

const (
	defaultAccessKey = "minioadmin"
	defaultSecretKey = "minioadmin"
	defaultRegion    = "us-east-1"
)

// Account is the value written and read by the example services.
type Account struct {
	ID        int       `json:"id"`
	Name      string    `json:"name"`
	Revision  uint64    `json:"revision"`
	UpdatedAt time.Time `json:"updated_at"`
}

func ConfigureEnvironment() error {
	for key, value := range map[string]string{
		"AWS_ACCESS_KEY_ID":     defaultAccessKey,
		"AWS_SECRET_ACCESS_KEY": defaultSecretKey,
		"AWS_REGION":            defaultRegion,
	} {
		if os.Getenv(key) == "" {
			if err := os.Setenv(key, value); err != nil {
				return fmt.Errorf("set %s: %w", key, err)
			}
		}
	}
	return nil
}

func BucketURL() string {
	return Getenv("ISLEDB_MINIO_URL", "s3://isledb?endpoint=http://localhost:9000&region=us-east-1&use_path_style=true")
}

func DatabasePrefix() string {
	return Getenv("ISLEDB_PREFIX", "services")
}

func Getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func AccountKey(id int) []byte {
	return fmt.Appendf(nil, "accounts/%06d", id)
}

func DecodeAccount(value []byte) (Account, error) {
	var account Account
	err := json.Unmarshal(value, &account)
	return account, err
}
