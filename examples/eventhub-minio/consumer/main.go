package main

import (
	"context"
	"encoding/json"
	"errors"
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
		pollInterval   = flag.Duration("poll-interval", 500*time.Millisecond, "Tail poll interval")
		refreshEvery   = flag.Duration("refresh-interval", 500*time.Millisecond, "Tailing reader refresh interval")
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

	tr, err := isledb.OpenTailingReader(ctx, store, isledb.TailingReaderOpenOptions{
		RefreshInterval: *refreshEvery,
		ReaderOptions: isledb.ReaderOpenOptions{
			CacheDir: *cacheDir,
		},
	})
	if err != nil {
		log.Fatalf("open tailing reader: %v", err)
	}
	defer tr.Close()

	if err := tr.Start(); err != nil {
		log.Fatalf("start tailing reader: %v", err)
	}

	eventPrefix := fmt.Sprintf("events/%s/", *topic)
	log.Printf("consumer started bucket_url=%s prefix=%s topic=%s start_after=%q", *bucketURL, *prefix, *topic, string(lastKey))

	err = tr.Tail(ctx, isledb.TailOptions{
		MinKey:        []byte(eventPrefix),
		MaxKey:        shared.PrefixUpperBound(eventPrefix),
		StartAfterKey: lastKey,
		PollInterval:  *pollInterval,
	}, func(kv isledb.KV) error {
		var ev shared.Event
		if err := json.Unmarshal(kv.Value, &ev); err != nil {
			log.Printf("consume key=%s raw=%s", kv.Key, kv.Value)
		} else {
			log.Printf("consume seq=%d id=%s topic=%s producer=%s payload=%s at=%s key=%s",
				ev.Seq, ev.ID, ev.Topic, ev.Producer, ev.Payload, ev.At.Format(time.RFC3339Nano), kv.Key)
		}
		return shared.SaveLastKey(*checkpointPath, kv.Key)
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("tail failed: %v", err)
	}
	log.Printf("consumer stopped")
}

func getenvDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
