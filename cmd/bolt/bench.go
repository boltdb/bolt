package main

import (
	"os"

	"github.com/boltdb/bolt"
)

// Run benchmarks on a given dataset.
func Bench() {
	path := "bench"
	if _, err := os.Stat(path); os.IsNotExist(err) {
		fatal(err)
		return
	}

	db, err := bolt.Open(path, 0600)
	if err != nil {
		fatal(err)
		return
	}
	defer db.Close()

	bucketName := "widgets"
	key := "key1"
	value := "value1"

	err = db.Update(func(tx *bolt.Tx) error {
		// Find bucket.
		b := tx.Bucket(bucketName)
		if b == nil {
			fatalf("bucket not found: %s", bucketName)
			return nil
		}

		// Set value for a given key.
		return b.Put([]byte(key), []byte(value))
	})
	if err != nil {
		fatal(err)
		return
	}
}
