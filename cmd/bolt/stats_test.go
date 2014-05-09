package main_test

import (
	"testing"

	"github.com/boltdb/bolt"
	. "github.com/boltdb/bolt/cmd/bolt"
	"github.com/stretchr/testify/assert"
)

func TestStats(t *testing.T) {
	SetTestMode(true)
	open(func(db *bolt.DB, path string) {
		db.Update(func(tx *bolt.Tx) error {
			tx.CreateBucket([]byte("foo"))
			tx.CreateBucket([]byte("bar"))
			tx.CreateBucket([]byte("baz"))
			return nil
		})
		db.Close()
		output := run("stats", path, "b")
		assert.Equal(t, "Aggregate statistics for 2 buckets\n\n"+
			"Page count statistics\n"+
			"\tNumber of logical branch pages: 0\n"+
			"\tNumber of physical branch overflow pages: 0\n"+
			"\tNumber of logical leaf pages: 2\n"+
			"\tNumber of physical leaf overflow pages: 0\n"+
			"Tree statistics\n"+
			"\tNumber of keys/value pairs: 0\n"+
			"\tNumber of levels in B+tree: 0\n"+
			"Page size utilization\n"+
			"\tBytes allocated for physical branch pages: 0\n"+
			"\tBytes actually used for branch data: 0\n"+
			"\tBytes allocated for physical leaf pages: 8192\n"+
			"\tBytes actually used for leaf data: 0", output)
	})
}
