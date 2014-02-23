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
		assert.Equal(t, txn.DB(), db)
	})
}

// Ensure that opening a RWTransaction while the DB is closed returns an error.
func TestRWTransactionOpenWithClosedDB(t *testing.T) {
	withDB(func(db *DB, path string) {
		txn, err := db.RWTransaction()
		assert.Equal(t, err, ErrDatabaseNotOpen)
		assert.Nil(t, txn)
	})
}

// Ensure that retrieving all buckets returns writable buckets.
func TestRWTransactionBuckets(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.CreateBucket("widgets")
		db.CreateBucket("woojits")
		db.Do(func(txn *RWTransaction) error {
			buckets := txn.Buckets()
			assert.Equal(t, len(buckets), 2)
			assert.Equal(t, buckets[0].Name(), "widgets")
			assert.Equal(t, buckets[1].Name(), "woojits")
			buckets[0].Put([]byte("foo"), []byte("0000"))
			buckets[1].Put([]byte("bar"), []byte("0001"))
			return nil
		})
		v, _ := db.Get("widgets", []byte("foo"))
		assert.Equal(t, v, []byte("0000"))
		v, _ = db.Get("woojits", []byte("bar"))
		assert.Equal(t, v, []byte("0001"))
	})
}

// Ensure that a bucket can be created and retrieved.
func TestRWTransactionCreateBucket(t *testing.T) {
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

// Ensure that a bucket can be created if it doesn't already exist.
func TestRWTransactionCreateBucketIfNotExists(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		assert.NoError(t, db.CreateBucketIfNotExists("widgets"))
		assert.NoError(t, db.CreateBucketIfNotExists("widgets"))

		// Read the bucket through a separate transaction.
		b, err := db.Bucket("widgets")
		assert.NotNil(t, b)
		assert.NoError(t, err)
	})
}

// Ensure that a bucket cannot be created twice.
func TestRWTransactionRecreateBucket(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		// Create a bucket.
		err := db.CreateBucket("widgets")
		assert.NoError(t, err)

		// Create the same bucket again.
		err = db.CreateBucket("widgets")
		assert.Equal(t, err, ErrBucketExists)
	})
}

// Ensure that a bucket is created with a non-blank name.
func TestRWTransactionCreateBucketWithoutName(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		err := db.CreateBucket("")
		assert.Equal(t, err, ErrBucketNameRequired)
	})
}

// Ensure that a bucket name is not too long.
func TestRWTransactionCreateBucketWithLongName(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		err := db.CreateBucket(strings.Repeat("X", 255))
		assert.NoError(t, err)

		err = db.CreateBucket(strings.Repeat("X", 256))
		assert.Equal(t, err, ErrBucketNameTooLarge)
	})
}

// Ensure that a bucket can be deleted.
func TestRWTransactionDeleteBucket(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		// Create a bucket and add a value.
		db.CreateBucket("widgets")
		db.Put("widgets", []byte("foo"), []byte("bar"))

		// Delete the bucket and make sure we can't get the value.
		assert.NoError(t, db.DeleteBucket("widgets"))
		value, err := db.Get("widgets", []byte("foo"))
		assert.Equal(t, err, ErrBucketNotFound)
		assert.Nil(t, value)

		// Create the bucket again and make sure there's not a phantom value.
		assert.NoError(t, db.CreateBucket("widgets"))
		value, err = db.Get("widgets", []byte("foo"))
		assert.NoError(t, err)
		assert.Nil(t, value)
	})
}

// Ensure that an error is returned when deleting from a bucket that doesn't exist.
func TestRWTransactionDeleteBucketNotFound(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		err := db.DeleteBucket("widgets")
		assert.Equal(t, err, ErrBucketNotFound)
	})
}
