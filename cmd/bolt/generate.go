package main

import (
	"fmt"

	"github.com/boltdb/bolt"
)

// Generate data for benchmarks.
func Generate(destPath string, numBuckets, numItems int) {

	// Open the database.
	db, err := bolt.Open(destPath, 0600)
	if err != nil {
		fatalf("open db:", err)
		return
	}
	defer db.Close()

	for bucketIndex := 0; bucketIndex < numBuckets; bucketIndex++ {
		bucketName := fmt.Sprintf("bucket%03d", bucketIndex)

		err = db.Update(func(tx *bolt.Tx) error {

			// Create the bucket if it doesn't exist.
			if err := tx.CreateBucketIfNotExists([]byte(bucketName)); err != nil {
				fatalf("create bucket: %s", err)
				return nil
			}

			// Find bucket.
			b := tx.Bucket([]byte(bucketName))
			if b == nil {
				fatalf("bucket not found: %s", bucketName)
				return nil
			}

			for i := 0; i < numItems; i++ {
				key := fmt.Sprintf("key%03d", i)
				value := fmt.Sprintf("value%03d", i)

				// Set value for a given key.
				if err := b.Put([]byte(key), []byte(value)); err != nil {
					return err
				}
			}

			return nil
		})
	}
	if err != nil {
		fatal(err)
		return
	}
}
