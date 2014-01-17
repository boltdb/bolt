package bolt

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// Ensure that a bucket can be created and retrieved.
func TestTransactionCreateBucket(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		txn, _ := db.Transaction(false)
		b, err := txn.CreateBucket("foo", false)
		if assert.NoError(t, err) && assert.NotNil(t, b) {
			b2, err := txn.Bucket("foo")
			assert.NoError(t, err)
			assert.Equal(t, b, b2)
		}
	})
}

// Ensure that a user-created transaction cannot be used to create a bucket.
func TestTransactionInvalidCreateBucket(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		txn := new(Transaction)
		_, err := txn.CreateBucket("foo", false)
		assert.Equal(t, err, InvalidTransactionError)
	})
}

// Ensure that an existing bucket cannot be created.
func TestTransactionCreateExistingBucket(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		txn, _ := db.Transaction(false)
		txn.CreateBucket("foo", false)
		_, err := txn.CreateBucket("foo", false)
		assert.Equal(t, err, BucketAlreadyExistsError)
	})
}
