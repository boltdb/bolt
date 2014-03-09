package bolt

import (
	"math/rand"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Ensure that a RWTx can be retrieved.
func TestRWTx(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		txn, err := db.RWTx()
		assert.NotNil(t, txn)
		assert.NoError(t, err)
		assert.Equal(t, txn.DB(), db)
	})
}

// Ensure that opening a RWTx while the DB is closed returns an error.
func TestRWTxOpenWithClosedDB(t *testing.T) {
	withDB(func(db *DB, path string) {
		txn, err := db.RWTx()
		assert.Equal(t, err, ErrDatabaseNotOpen)
		assert.Nil(t, txn)
	})
}

// Ensure that retrieving all buckets returns writable buckets.
func TestRWTxBuckets(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.CreateBucket("widgets")
		db.CreateBucket("woojits")
		db.Do(func(txn *RWTx) error {
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
func TestRWTxCreateBucket(t *testing.T) {
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
func TestRWTxCreateBucketIfNotExists(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		assert.NoError(t, db.CreateBucketIfNotExists("widgets"))
		assert.NoError(t, db.CreateBucketIfNotExists("widgets"))
		assert.Equal(t, db.CreateBucketIfNotExists(""), ErrBucketNameRequired)

		// Read the bucket through a separate transaction.
		b, err := db.Bucket("widgets")
		assert.NotNil(t, b)
		assert.NoError(t, err)
	})
}

// Ensure that a bucket cannot be created twice.
func TestRWTxRecreateBucket(t *testing.T) {
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
func TestRWTxCreateBucketWithoutName(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		err := db.CreateBucket("")
		assert.Equal(t, err, ErrBucketNameRequired)
	})
}

// Ensure that a bucket name is not too long.
func TestRWTxCreateBucketWithLongName(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		err := db.CreateBucket(strings.Repeat("X", 255))
		assert.NoError(t, err)

		err = db.CreateBucket(strings.Repeat("X", 256))
		assert.Equal(t, err, ErrBucketNameTooLarge)
	})
}

// Ensure that a bucket can be deleted.
func TestRWTxDeleteBucket(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		// Create a bucket and add a value.
		db.CreateBucket("widgets")
		db.Put("widgets", []byte("foo"), []byte("bar"))

		b, _ := db.Bucket("widgets")

		// Delete the bucket and make sure we can't get the value.
		assert.NoError(t, db.DeleteBucket("widgets"))
		value, err := db.Get("widgets", []byte("foo"))
		assert.Equal(t, err, ErrBucketNotFound)
		assert.Nil(t, value)

		// Verify that the bucket's page is free.
		assert.Equal(t, db.freelist.all(), []pgid{b.root})

		// Create the bucket again and make sure there's not a phantom value.
		assert.NoError(t, db.CreateBucket("widgets"))
		value, err = db.Get("widgets", []byte("foo"))
		assert.NoError(t, err)
		assert.Nil(t, value)
	})
}

// Ensure that an error is returned when deleting from a bucket that doesn't exist.
func TestRWTxDeleteBucketNotFound(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		err := db.DeleteBucket("widgets")
		assert.Equal(t, err, ErrBucketNotFound)
	})
}

// Benchmark the performance of bulk put transactions in random order.
func BenchmarkRWTxPutRandom(b *testing.B) {
	indexes := rand.Perm(b.N)
	value := []byte(strings.Repeat("0", 64))
	withOpenDB(func(db *DB, path string) {
		db.CreateBucket("widgets")
		var txn *RWTx
		var bucket *Bucket
		for i := 0; i < b.N; i++ {
			if i%1000 == 0 {
				if txn != nil {
					txn.Commit()
				}
				txn, _ = db.RWTx()
				bucket = txn.Bucket("widgets")
			}
			bucket.Put([]byte(strconv.Itoa(indexes[i])), value)
		}
		txn.Commit()
	})
}

// Benchmark the performance of bulk put transactions in sequential order.
func BenchmarkRWTxPutSequential(b *testing.B) {
	value := []byte(strings.Repeat("0", 64))
	withOpenDB(func(db *DB, path string) {
		db.CreateBucket("widgets")
		db.Do(func(txn *RWTx) error {
			bucket := txn.Bucket("widgets")
			for i := 0; i < b.N; i++ {
				bucket.Put([]byte(strconv.Itoa(i)), value)
			}
			return nil
		})
	})
}
