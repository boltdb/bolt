package main

import (
	"testing"

	"github.com/boltdb/bolt"
)

// Import converts an exported database dump into a new database.
// parallelism: integer representing number of concurrent reads/writes
// readWriteMode: 'read' or 'write'
// traversalPattern: 'sequentrial' or 'random'
func Bench(inputPath string, readWriteMode string, traversalPattern string, parallelism int) {

	// cursor/sequential reads
	// random reads

	// sequential writes
	// random writes

	// reading from many buckets
	// writing to many buckets

	// read from many paths
	// writing to many paths

	// bucket size/messages
	// bucket depth

	// concurrency

	// chart/graph

	// profile

	// benchmarks for getting all keys

	// Open the database.
	db, err := bolt.Open(inputPath, 0600)
	if err != nil {
		fatalf("error: %+v", err)
		return
	}
	defer db.Close()

	b := bolt.NewBenchmark(db, readWriteMode, traversalPattern, parallelism)

	result := testing.Benchmark(b.Run)

	println(result)
}
