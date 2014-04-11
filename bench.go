package bolt

import (
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
		panic(err)
		return
	}
	defer db.Close()

	b.ResetTimer()

	// Keep running a fixed number of parallel reads until we run out of time.
	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		for j := 0; j < bm.Parallelism; j++ {
			wg.Add(1)
			go func() {
				if bm.TraversalPattern == BenchRandomTraversal {
					// Perform all reads in random order.
					// indexes := rand.Perm(total)
				} else {
					// Perform all reads in sequential order.
				}
				wg.Done()
			}()
		}
		wg.Wait()
	}
}
