package main_test

import (
	"testing"

	"github.com/boltdb/bolt"
	. "github.com/boltdb/bolt/cmd/bolt"
	"github.com/stretchr/testify/assert"
)

// Ensure that a value can be retrieved from the CLI.
func TestGet(t *testing.T) {
	SetTestMode(true)
	open(func(db *bolt.DB, path string) {
		db.Update(func(tx *bolt.Tx) error {
			tx.CreateBucket([]byte("widgets"))
			tx.Bucket([]byte("widgets")).Put([]byte("foo"), []byte("bar"))
			return nil
		})
		db.Close()
		output := run("get", path, "widgets", "foo")
		assert.Equal(t, "bar", output)
	})
}

// Ensure that an error is reported if the database is not found.
func TestGetDBNotFound(t *testing.T) {
	SetTestMode(true)
	output := run("get", "no/such/db", "widgets", "foo")
	assert.Equal(t, "stat no/such/db: no such file or directory", output)
}

// Ensure that an error is reported if the bucket is not found.
func TestGetBucketNotFound(t *testing.T) {
	SetTestMode(true)
	open(func(db *bolt.DB, path string) {
		db.Close()
		output := run("get", path, "widgets", "foo")
		assert.Equal(t, "bucket not found: widgets", output)
	})
}

// Ensure that an error is reported if the key is not found.
func TestGetKeyNotFound(t *testing.T) {
	SetTestMode(true)
	open(func(db *bolt.DB, path string) {
		db.Update(func(tx *bolt.Tx) error {
			_, err := tx.CreateBucket([]byte("widgets"))
			return err
		})
		db.Close()
		output := run("get", path, "widgets", "foo")
		assert.Equal(t, "key not found: foo", output)
	})
}
