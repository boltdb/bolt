package bolt

import (
	"errors"
	"fmt"
	"os"
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
	InputPath        string
	ReadWriteMode    string
	TraversalPattern string
	Parallelism      int
}

func NewBenchmark(inputPath, readWriteMode, traversalPattern string, parallelism int) *Benchmark {
	return &Benchmark{inputPath, readWriteMode, traversalPattern, parallelism}
}

func (bm *Benchmark) Run(b *testing.B) {

	// Open the database.
	db, err := Open(bm.InputPath, 0600)
	if err != nil {
		b.Fatalf("error: %+v", err)
		return
	}
	defer db.Close()

	buckets, err := buckets(db, bm.InputPath)
	if err != nil {
		b.Fatalf("error: %+v", err)
	}

	b.ResetTimer()

	// Keep running a fixed number of parallel reads until we run out of time.
	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		for j := 0; j < bm.Parallelism; j++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := bm.runBuckets(b, db, buckets); err != nil {
					b.Fatalf("error: %+v", err)
				}
			}()
		}
		wg.Wait()
	}
}

// Run benchmark(s) for each of the given buckets.
func (bm *Benchmark) runBuckets(b *testing.B, db *DB, buckets []string) error {
	return db.View(func(tx *Tx) error {
		bucketsCount := len(buckets)
		for _, bucket := range buckets {
			c := tx.Bucket([]byte(bucket)).Cursor()
			count := 0
			for k, _ := c.First(); k != nil; k, _ = c.Next() {
				count++
			}
			if count != bucketsCount {
				return errors.New(fmt.Sprintf("wrong count: %d; expected: %d", count, bucketsCount))
			}
		}
		return nil
	})
}

func buckets(db *DB, path string) ([]string, error) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil, err
	}

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
