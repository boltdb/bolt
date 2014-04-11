package main

import (
	"errors"
	"fmt"
	"os"

	"github.com/boltdb/bolt"
)

// Keys retrieves a list of keys for a given bucket.
func Keys(path, name string) {
	keys, err := keys(path, name)

	if err != nil {
		fatal(err)
		return
	}

	for _, key := range keys {
		println(key)
	}
}

func keys(path, name string) ([]string, error) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil, err
	}

	db, err := bolt.Open(path, 0600)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	keys := []string{}

	err = db.View(func(tx *bolt.Tx) error {
		// Find bucket.
		b := tx.Bucket([]byte(name))
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
