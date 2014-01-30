package bolt

import (
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
		txn, _ := db.RWTransaction()
		err := txn.CreateBucket("widgets")
		assert.NoError(t, err)

		// Commit the transaction.
		err = txn.Commit()
		assert.NoError(t, err)

		/*
		// Open a separate read-only transaction.
		rtxn, err := db.Transaction()
		assert.NotNil(t, txn)
		assert.NoError(t, err)

		b, err := rtxn.Bucket("widgets")
		assert.NoError(t, err)
		if assert.NotNil(t, b) {
			assert.Equal(t, b.Name(), "widgets")
		}
		*/
	})
}

// Ensure that an existing bucket cannot be created.
func TestTransactionCreateExistingBucket(t *testing.T) {
	t.Skip("pending")
}
