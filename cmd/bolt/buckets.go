package main

import (
	"os"

	"github.com/boltdb/bolt"
)

// Buckets prints a list of all buckets.
func Buckets(path string) {
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

	err = db.View(func(tx *bolt.Tx) error {
		for _, b := range tx.Buckets() {
			println(b.Name())
		}
		return nil
	})
	if err != nil {
		fatal(err)
		return
	}
}
