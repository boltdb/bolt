package main_test

import (
	"testing"

	"github.com/boltdb/bolt"
	. "github.com/boltdb/bolt/cmd/bolt"
	"github.com/stretchr/testify/assert"
)

// Ensure that a list of keys can be retrieved for a given bucket.
func TestKeys(t *testing.T) {
	SetTestMode(true)
	open(func(db *bolt.DB) {
		db.Update(func(tx *bolt.Tx) error {
			tx.CreateBucket("widgets")
			tx.Bucket("widgets").Put([]byte("0002"), []byte(""))
			tx.Bucket("widgets").Put([]byte("0001"), []byte(""))
			tx.Bucket("widgets").Put([]byte("0003"), []byte(""))
			return nil
		})
		output := run("keys", db.Path(), "widgets")
		assert.Equal(t, "0001\n0002\n0003", output)
	})
}

// Ensure that an error is reported if the database is not found.
func TestKeysDBNotFound(t *testing.T) {
	SetTestMode(true)
	output := run("keys", "no/such/db", "widgets")
	assert.Equal(t, "stat no/such/db: no such file or directory", output)
}

// Ensure that an error is reported if the bucket is not found.
func TestKeysBucketNotFound(t *testing.T) {
	SetTestMode(true)
	open(func(db *bolt.DB) {
		output := run("keys", db.Path(), "widgets")
		assert.Equal(t, "bucket not found: widgets", output)
	})
}
