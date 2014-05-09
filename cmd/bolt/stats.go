package main

import (
	"os"

	"github.com/boltdb/bolt"
)

// Keys retrieves a list of keys for a given bucket.
func Stats(path, name string) {
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
		// Find bucket.
		b := tx.Bucket([]byte(name))
		if b == nil {
			fatalf("bucket not found: %s", name)
			return nil
		}

		// Iterate over each key.
		s := b.Stats()
		println("Page count statistics")
		printf("\tNumber of logical branch pages: %d\n", s.BranchPageN)
		printf("\tNumber of physical branch overflow pages: %d\n", s.BranchOverflowN)
		printf("\tNumber of logical leaf pages: %d\n", s.LeafPageN)
		printf("\tNumber of physical leaf overflow pages: %d\n", s.LeafOverflowN)

		println("Tree statistics")
		printf("\tNumber of keys/value pairs: %d\n", s.KeyN)
		printf("\tNumber of levels in B+tree: %d\n", s.Depth)

		println("Page size utilization")
		printf("\tBytes allocated for physical branch pages: %d\n", s.BranchAlloc)
		printf("\tBytes actually used for branch data: %d\n", s.BranchInuse)
		printf("\tBytes allocated for physical leaf pages: %d\n", s.LeafAlloc)
		printf("\tBytes actually used for leaf data: %d\n", s.LeafInuse)
		return nil
	})
	if err != nil {
		fatal(err)
		return
	}
}
