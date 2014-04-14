package bench

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/boltdb/bolt"
)

const (
	BenchReadMode            = "read"
	BenchWriteMode           = "write"
	BenchSequentialTraversal = "sequential"
	BenchRandomTraversal     = "random"
)

type Benchmark struct {
	db     *bolt.DB
	config *Config
}

func New(db *bolt.DB, config *Config) *Benchmark {
	b := new(Benchmark)
	b.db = db
	b.config = config
	return b
}

func (bm *Benchmark) Run(b *testing.B) {

	// Read buckets and keys before benchmark begins so we don't knew the
	// results.
	buckets, err := buckets(bm.db)
	if err != nil {
		b.Fatalf("error: %+v", err)
	}
	bucketsWithKeys := make(map[string][]string)
	for _, bucket := range buckets {
		keys, err := keys(bm.db, bucket)
		if err != nil {
			b.Fatalf("error: %+v", err)
		}
		bucketsWithKeys[bucket] = keys
	}

	b.ResetTimer()

	// Keep running a fixed number of parallel reads until we run out of time.
	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		for j := 0; j < bm.config.Parallelism; j++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := bm.readBuckets(b, bm.db, bucketsWithKeys); err != nil {
					b.Fatalf("error: %+v", err)
				}
			}()
		}
		wg.Wait()
	}
}

// Run benchmark(s) for each of the given buckets.
func (bm *Benchmark) readBuckets(b *testing.B, db *bolt.DB, bucketsWithKeys map[string][]string) error {
	return db.View(func(tx *bolt.Tx) error {
		bucketsCount := len(bucketsWithKeys)
		count := 0
		for bucket, keys := range bucketsWithKeys {
			bucket := tx.Bucket([]byte(bucket))
			if err := bm.readKeys(b, bucket, keys); err != nil {
				return err
			}
			count++
		}
		if count != bucketsCount {
			return errors.New(fmt.Sprintf("wrong count: %d; expected: %d", count, bucketsCount))
		}
		return nil
	})
}

func (bm *Benchmark) readKeys(b *testing.B, bucket *bolt.Bucket, keys []string) error {
	c := bucket.Cursor()
	keysCount := len(keys)
	count := 0
	for k, _ := c.First(); k != nil; k, _ = c.Next() {
		count++
	}
	if count != keysCount {
		return errors.New(fmt.Sprintf("wrong count: %d; expected: %d", count, keysCount))
	}
	return nil
}

func buckets(db *bolt.DB) ([]string, error) {
	buckets := []string{}
	err := db.View(func(tx *bolt.Tx) error {
		// Iterate over each bucket.
		return tx.ForEach(func(name []byte, _ *bolt.Bucket) error {
			buckets = append(buckets, string(name))
			return nil
		})
	})
	return buckets, err
}

func keys(db *bolt.DB, bucket string) ([]string, error) {
	keys := []string{}
	err := db.View(func(tx *bolt.Tx) error {
		// Find bucket.
		b := tx.Bucket([]byte(bucket))
		if b == nil {
			return errors.New(fmt.Sprintf("bucket %+v not found", b))
		}

		// Iterate over each key.
		return b.ForEach(func(key, _ []byte) error {
			keys = append(keys, string(key))
			return nil
		})
	})
	return keys, err
}
