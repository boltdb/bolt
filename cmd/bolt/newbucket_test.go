package main_test

import (
	"testing"

	"github.com/boltdb/bolt"
	. "github.com/boltdb/bolt/cmd/bolt"
	"github.com/stretchr/testify/assert"
)

// Ensure that a list of keys can be retrieved for a given bucket.
func TestNewBucket(t *testing.T) {
	SetTestMode(true)
	open(func(db *bolt.DB, path string) {
		db.Close()
		run("newbucket", path, "some_test")
		run("newbucket", path, "another_test")

		output := run("buckets", path)
		assert.Equal(t, "<nil>\n<nil>\nanother_test\nsome_test", output)
	})
}

// Ensure that an error is reported if the database is not found.
func TestNewBucketDBNotFound(t *testing.T) {
	SetTestMode(true)
	output := run("keys", "no/such/db", "widgets")
	assert.Equal(t, "stat no/such/db: no such file or directory", output)
}
