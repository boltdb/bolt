package bolt

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Ensure that a RWTransaction can be retrieved.
func TestRWTransaction(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		txn, err := db.RWTransaction()
		assert.NotNil(t, txn)
		assert.NoError(t, err)
	})
}

// Ensure that a bucket can be created and retrieved.
func TestTransactionCreateBucket(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		// Create a bucket.
		err := db.CreateBucket("widgets")
		assert.NoError(t, err)

		// Read the bucket through a separate transaction.
		b, err := db.Bucket("widgets")
		assert.NotNil(t, b)
		assert.NoError(t, err)
	})
}

// Ensure that a bucket cannot be created twice.
func TestTransactionRecreateBucket(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		// Create a bucket.
		err := db.CreateBucket("widgets")
		assert.NoError(t, err)

		// Create the same bucket again.
		err = db.CreateBucket("widgets")
		assert.Equal(t, err, &Error{"bucket already exists", nil})
	})
}

// Ensure that a bucket is created with a non-blank name.
func TestTransactionCreateBucketWithoutName(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		err := db.CreateBucket("")
		assert.Equal(t, err, &Error{"bucket name cannot be blank", nil})
	})
}

// Ensure that a bucket name is not too long.
func TestTransactionCreateBucketWithLongName(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		err := db.CreateBucket(strings.Repeat("X", 255))
		assert.NoError(t, err)

		err = db.CreateBucket(strings.Repeat("X", 256))
		assert.Equal(t, err, &Error{"bucket name too long", nil})
	})
}
