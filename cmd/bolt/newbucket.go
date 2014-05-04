package main

import (
	"os"

	"github.com/boltdb/bolt"
)

// NewBucket initiates a new bucket.
func NewBucket(path, name string) {
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
		_, err := tx.CreateBucket([]byte(name))
		fatal(err)
		return err
	})
	if err != nil {
		fatal(err)
		return
	}
	return
}
