package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"time"

	"github.com/ankur-anand/isledb"
	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/examples/eventhub-minio/shared"
	_ "gocloud.dev/blob/s3blob"
)

func main() {
	var (
		bucketURL      = flag.String("bucket-url", getenvDefault("ISLEDB_MINIO_URL", shared.DefaultBucketURL("isledb-eventhub")), "S3/MinIO bucket URL")
		prefix         = flag.String("prefix", getenvDefault("ISLEDB_PREFIX", "eventhub"), "IsleDB prefix")
		topic          = flag.String("topic", getenvDefault("ISLEDB_TOPIC", "orders"), "Event topic")
		cacheDir       = flag.String("cache-dir", filepath.Join(os.TempDir(), "isledb-eventhub-cache"), "Reader cache directory")
		checkpointPath = flag.String("checkpoint", filepath.Join(os.TempDir(), "isledb-eventhub", "consumer.checkpoint"), "Checkpoint file path")
		pollInterval   = flag.Duration("poll-interval", 500*time.Millisecond, "Reader refresh poll interval")
		ensureBucket   = flag.Bool("ensure-bucket", true, "Create MinIO bucket if missing")
	)
	flag.Parse()

	shared.EnsureAWSDefaults()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	if *ensureBucket {
		if err := shared.EnsureS3Bucket(ctx, *bucketURL); err != nil {
			log.Fatalf("ensure bucket: %v", err)
		}
	}

	if err := os.MkdirAll(*cacheDir, 0755); err != nil {
		log.Fatalf("create cache dir: %v", err)
	}

	lastKey, err := shared.LoadLastKey(*checkpointPath)
	if err != nil {
		log.Fatalf("load checkpoint: %v", err)
	}

	store, err := blobstore.Open(ctx, *bucketURL, *prefix)
	if err != nil {
		log.Fatalf("open blob store: %v", err)
	}
	defer store.Close()

	reader, err := isledb.OpenReader(ctx, store, isledb.ReaderOpenOptions{
		CacheDir: *cacheDir,
	})
	if err != nil {
		log.Fatalf("open reader: %v", err)
	}
	defer reader.Close()

	eventPrefix := fmt.Sprintf("events/%s/", *topic)
	log.Printf("consumer started bucket_url=%s prefix=%s topic=%s start_after=%q", *bucketURL, *prefix, *topic, string(lastKey))

	ticker := time.NewTicker(*pollInterval)
	defer ticker.Stop()

	for {
		if err := reader.Refresh(ctx); err != nil {
			log.Fatalf("refresh reader: %v", err)
		}

		minKey := []byte(eventPrefix)
		if len(lastKey) > 0 {
			minKey = nextKey(lastKey)
		}
		rows, err := reader.ScanLimit(ctx, minKey, shared.PrefixUpperBound(eventPrefix), 0)
		if err != nil {
			log.Fatalf("scan events: %v", err)
		}
		for _, kv := range rows {
			var ev shared.Event
			if err := json.Unmarshal(kv.Value, &ev); err != nil {
				log.Printf("consume key=%s raw=%s", kv.Key, kv.Value)
			} else {
				log.Printf("consume seq=%d id=%s topic=%s producer=%s payload=%s at=%s key=%s",
					ev.Seq, ev.ID, ev.Topic, ev.Producer, ev.Payload, ev.At.Format(time.RFC3339Nano), kv.Key)
			}
			lastKey = append(lastKey[:0], kv.Key...)
			if err := shared.SaveLastKey(*checkpointPath, lastKey); err != nil {
				log.Fatalf("save checkpoint: %v", err)
			}
		}

		select {
		case <-ticker.C:
		case <-ctx.Done():
			log.Printf("consumer stopped")
			return
		}
	}
}

func getenvDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func nextKey(key []byte) []byte {
	out := append([]byte(nil), key...)
	for i := len(out) - 1; i >= 0; i-- {
		if out[i] < 0xFF {
			out[i]++
			return out[:i+1]
		}
	}
	return append(out, 0x00)
}
