package main

import (
	"os"

	"github.com/boltdb/bolt"
)

// Check performs a consistency check on the database and prints any errors found.
func Check(path string) {
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

	// Perform consistency check.
	if err := db.Check(); err != nil {
		if errors, ok := err.(bolt.ErrorList); ok {
			for _, err := range errors {
				println(err)
			}
		}
		fatalln(err)
		return
	}
	println("OK")
}
