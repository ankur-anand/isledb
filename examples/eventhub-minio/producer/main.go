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
	"strings"
	"time"

	"github.com/ankur-anand/isledb"
	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/examples/eventhub-minio/shared"
	_ "gocloud.dev/blob/s3blob"
)

const producerStateVersion = 1

type producerState struct {
	Version    int       `json:"version"`
	ProducerID string    `json:"producer_id,omitempty"`
	LastSeq    uint64    `json:"last_seq"`
	UpdatedAt  time.Time `json:"updated_at"`
}

func main() {
	var (
		bucketURL    = flag.String("bucket-url", getenvDefault("ISLEDB_MINIO_URL", shared.DefaultBucketURL("isledb-eventhub")), "S3/MinIO bucket URL")
		prefix       = flag.String("prefix", getenvDefault("ISLEDB_PREFIX", "eventhub"), "IsleDB prefix")
		topic        = flag.String("topic", getenvDefault("ISLEDB_TOPIC", "orders"), "Event topic")
		producerID   = flag.String("producer-id", getenvDefault("ISLEDB_PRODUCER_ID", "producer-1"), "Producer identifier")
		count        = flag.Int("count", 100, "Number of events to publish; 0 means run forever")
		interval     = flag.Duration("interval", 250*time.Millisecond, "Delay between events")
		flushEvery   = flag.Int("flush-every", 20, "Flush after N events")
		payloadPrefx = flag.String("payload-prefix", "event", "Payload prefix")
		ensureBucket = flag.Bool("ensure-bucket", true, "Create MinIO bucket if missing")
		stateFile    = flag.String("state-file", getenvDefault("ISLEDB_PRODUCER_STATE", ""), "Path to local producer state file")
	)
	flag.Parse()

	if *flushEvery <= 0 {
		log.Fatal("flush-every must be > 0")
	}

	shared.EnsureAWSDefaults()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	if *stateFile == "" {
		*stateFile = defaultProducerStatePath(*producerID)
	}

	lastSeq, err := loadProducerState(*stateFile, *producerID)
	if err != nil {
		log.Fatalf("load producer state: %v", err)
	}

	if *ensureBucket {
		if err := shared.EnsureS3Bucket(ctx, *bucketURL); err != nil {
			log.Fatalf("ensure bucket: %v", err)
		}
	}

	store, err := blobstore.Open(ctx, *bucketURL, *prefix)
	if err != nil {
		log.Fatalf("open blob store: %v", err)
	}
	defer store.Close()

	db, err := isledb.OpenDB(ctx, store, isledb.DBOptions{})
	if err != nil {
		log.Fatalf("open db: %v", err)
	}
	defer db.Close()

	writer, err := db.OpenWriter(ctx, isledb.DefaultWriterOptions())
	if err != nil {
		log.Fatalf("open writer: %v", err)
	}
	defer writer.Close()

	seq := lastSeq
	published := 0
	pending := 0
	log.Printf("producer started bucket_url=%s prefix=%s topic=%s producer_id=%s state_file=%s resume_seq=%d",
		*bucketURL, *prefix, *topic, *producerID, *stateFile, lastSeq)

	for *count == 0 || published < *count {
		select {
		case <-ctx.Done():
			if pending > 0 {
				if err := writer.Flush(context.Background()); err != nil {
					log.Fatalf("flush on shutdown: %v", err)
				}
			}
			log.Printf("producer stopped after %d events", seq)
			return
		default:
		}

		seq++
		if err := saveProducerState(*stateFile, *producerID, seq); err != nil {
			log.Fatalf("save producer state: %v", err)
		}

		now := time.Now().UTC()
		event := shared.Event{
			Seq:      seq,
			ID:       fmt.Sprintf("%s-%020d", *producerID, seq),
			Topic:    *topic,
			Producer: *producerID,
			Payload:  fmt.Sprintf("%s-%020d", *payloadPrefx, seq),
			At:       now,
		}
		value, err := json.Marshal(event)
		if err != nil {
			log.Fatalf("marshal event: %v", err)
		}
		key := shared.EventKey(*topic, now, seq)
		if err := writer.Put([]byte(key), value); err != nil {
			log.Fatalf("put event: %v", err)
		}
		published++
		pending++

		if pending >= *flushEvery {
			if err := writer.Flush(ctx); err != nil {
				log.Fatalf("flush: %v", err)
			}
			log.Printf("published seq=%d key=%s", seq, key)
			pending = 0
		}

		if *interval > 0 {
			timer := time.NewTimer(*interval)
			select {
			case <-ctx.Done():
				timer.Stop()
			case <-timer.C:
			}
		}
	}

	if pending > 0 {
		if err := writer.Flush(ctx); err != nil {
			log.Fatalf("final flush: %v", err)
		}
	}
	log.Printf("producer completed total_events=%d", seq)
}

func getenvDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func defaultProducerStatePath(producerID string) string {
	id := strings.TrimSpace(producerID)
	if id == "" {
		id = "producer"
	}
	id = strings.ReplaceAll(id, "/", "_")
	return filepath.Join(os.TempDir(), "isledb-eventhub", fmt.Sprintf("%s.state.json", id))
}

func loadProducerState(path, producerID string) (uint64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}

	var state producerState
	if err := json.Unmarshal(data, &state); err != nil {
		return 0, fmt.Errorf("decode producer state: %w", err)
	}
	if state.ProducerID != "" && state.ProducerID != producerID {
		return 0, fmt.Errorf("state producer_id mismatch: file=%q requested=%q", state.ProducerID, producerID)
	}
	return state.LastSeq, nil
}

func saveProducerState(path, producerID string, seq uint64) error {
	if path == "" {
		return nil
	}

	state := producerState{
		Version:    producerStateVersion,
		ProducerID: producerID,
		LastSeq:    seq,
		UpdatedAt:  time.Now().UTC(),
	}
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}

	tmpPath := fmt.Sprintf("%s.tmp.%d", path, time.Now().UnixNano())
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return err
	}
	return os.Rename(tmpPath, path)
}
