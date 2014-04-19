package main

import (
	"os"

	"github.com/boltdb/bolt"
)

// Set sets the value for a given key in a bucket.
func Set(path, name, key, value string) {
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

	err = db.Update(func(tx *bolt.Tx) error {

		// Find bucket.
		b := tx.Bucket([]byte(name))
		if b == nil {
			fatalf("bucket not found: %s", name)
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
