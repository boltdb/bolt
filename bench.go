package bolt

import (
	"errors"
	"fmt"
	"sync"
	"testing"
)

const (
	BenchReadMode            = "read"
	BenchWriteMode           = "write"
	BenchSequentialTraversal = "sequential"
	BenchRandomTraversal     = "random"
)

type Benchmark struct {
	db               *DB
	ReadWriteMode    string
	TraversalPattern string
	Parallelism      int
}

func NewBenchmark(db *DB, readWriteMode, traversalPattern string, parallelism int) *Benchmark {
	return &Benchmark{db, readWriteMode, traversalPattern, parallelism}
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
		for j := 0; j < bm.Parallelism; j++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := bm.runBuckets(b, bm.db, bucketsWithKeys); err != nil {
					b.Fatalf("error: %+v", err)
				}
			}()
		}
		wg.Wait()
	}
}

// Run benchmark(s) for each of the given buckets.
func (bm *Benchmark) runBuckets(b *testing.B, db *DB, bucketsWithKeys map[string][]string) error {
	return db.View(func(tx *Tx) error {
		bucketsCount := len(bucketsWithKeys)
		count := 0
		for bucket, keys := range bucketsWithKeys {
			bucket := tx.Bucket([]byte(bucket))
			if err := bm.runKeys(b, bucket, keys); err != nil {
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

func (bm *Benchmark) runKeys(b *testing.B, bucket *Bucket, keys []string) error {
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

func buckets(db *DB) ([]string, error) {
	buckets := []string{}
	err := db.View(func(tx *Tx) error {
		// Iterate over each bucket.
		return tx.ForEach(func(name []byte, _ *Bucket) error {
			buckets = append(buckets, string(name))
			return nil
		})
	})
	return buckets, err
}

func keys(db *DB, bucket string) ([]string, error) {
	keys := []string{}
	err := db.View(func(tx *Tx) error {
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
