package main

import (
	"testing"

	"github.com/boltdb/bolt"
	"github.com/boltdb/bolt/bench"
)

// Import converts an exported database dump into a new database.
// readWriteMode: 'read' or 'write'
// traversalPattern: 'sequentrial' or 'random'
// parallelism: integer representing number of concurrent reads/writes
func Bench(inputPath string, readWriteMode string, traversalPattern string, parallelism int) {

	// Open the database.
	db, err := bolt.Open(inputPath, 0600)
	if err != nil {
		fatalf("error: %+v", err)
		return
	}
	defer db.Close()

	b := bench.New(db, &bench.Config{
		ReadWriteMode:    readWriteMode,
		TraversalPattern: traversalPattern,
		Parallelism:      parallelism,
	})

	println(testing.Benchmark(b.Run))
}
