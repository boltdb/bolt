package bolt

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// Ensure that a bucket can be created and retrieved.
func TestTransactionCreateBucket(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		txn, _ := db.RWTransaction()
		err := txn.CreateBucket("foo")
		if assert.NoError(t, err) {
			assert.NotNil(t, txn.Bucket("foo"))
		}
	})
}

// Ensure that an existing bucket cannot be created.
func TestTransactionCreateExistingBucket(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		txn, _ := db.RWTransaction()
		txn.CreateBucket("foo")
		err := txn.CreateBucket("foo")
		assert.Equal(t, err, BucketAlreadyExistsError)
	})
}
