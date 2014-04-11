package main_test

import (
	"testing"

	"github.com/boltdb/bolt"
	. "github.com/boltdb/bolt/cmd/bolt"
	"github.com/stretchr/testify/assert"
)

// Ensure that a value can be set from the CLI.
func TestSet(t *testing.T) {
	SetTestMode(true)
	open(func(db *bolt.DB, path string) {
		db.Update(func(tx *bolt.Tx) error {
			tx.CreateBucket([]byte("widgets"))
			return nil
		})
		db.Close()
		assert.Equal(t, "", run("set", path, "widgets", "foo", "bar"))
		assert.Equal(t, "bar", run("get", path, "widgets", "foo"))
	})
}

// Ensure that an error is reported if the database is not found.
func TestSetDBNotFound(t *testing.T) {
	SetTestMode(true)
	output := run("set", "no/such/db", "widgets", "foo", "bar")
	assert.Equal(t, "stat no/such/db: no such file or directory", output)
}

// Ensure that an error is reported if the bucket is not found.
func TestSetBucketNotFound(t *testing.T) {
	SetTestMode(true)
	open(func(db *bolt.DB, path string) {
		db.Close()
		output := run("set", path, "widgets", "foo", "bar")
		assert.Equal(t, "bucket not found: widgets", output)
	})
}
