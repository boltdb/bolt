package bolt

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// Ensure that the database can retrieve a list of buckets.
func TestTransactionBuckets(t *testing.T) {
	t.Skip("pending") // TODO(benbjohnson)
}

// Ensure that a Transaction can retrieve a bucket.
func TestTransactionBucketMissing(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.CreateBucket("widgets")
		b, err := db.Bucket("widgets")
		assert.NoError(t, err)
		if assert.NotNil(t, b) {
			assert.Equal(t, "widgets", b.Name())
		}
	})
}

// Ensure that a Transaction retrieving a non-existent key returns nil.
func TestTransactionGetMissing(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.CreateBucket("widgets")
		db.Put("widgets", []byte("foo"), []byte("bar"))
		value, err := db.Get("widgets", []byte("no_such_key"))
		assert.NoError(t, err)
		assert.Nil(t, value)
	})
}
