package bolt

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Ensure that committing a closed transaction returns an error.
func TestTx_Commit_Closed(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		tx, _ := db.Begin(true)
		tx.CreateBucket([]byte("foo"))
		assert.NoError(t, tx.Commit())
		assert.Equal(t, tx.Commit(), ErrTxClosed)
	})
}

// Ensure that rolling back a closed transaction returns an error.
func TestTx_Rollback_Closed(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		tx, _ := db.Begin(true)
		assert.NoError(t, tx.Rollback())
		assert.Equal(t, tx.Rollback(), ErrTxClosed)
	})
}

// Ensure that committing a read-only transaction returns an error.
func TestTx_Commit_ReadOnly(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		tx, _ := db.Begin(false)
		assert.Equal(t, tx.Commit(), ErrTxNotWritable)
	})
}

// Ensure that creating a bucket with a read-only transaction returns an error.
func TestTx_CreateBucket_ReadOnly(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.View(func(tx *Tx) error {
			assert.Equal(t, tx.CreateBucket([]byte("foo")), ErrTxNotWritable)
			return nil
		})
	})
}

// Ensure that creating a bucket on a closed transaction returns an error.
func TestTx_CreateBucket_Closed(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		tx, _ := db.Begin(true)
		tx.Commit()
		assert.Equal(t, tx.CreateBucket([]byte("foo")), ErrTxClosed)
	})
}

// Ensure that a Tx can retrieve a bucket.
func TestTx_Bucket(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.Update(func(tx *Tx) error {
			tx.CreateBucket([]byte("widgets"))
			b := tx.Bucket([]byte("widgets"))
			assert.NotNil(t, b)
			return nil
		})
	})
}

// Ensure that a Tx retrieving a non-existent key returns nil.
func TestTx_Get_Missing(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.Update(func(tx *Tx) error {
			tx.CreateBucket([]byte("widgets"))
			tx.Bucket([]byte("widgets")).Put([]byte("foo"), []byte("bar"))
			value := tx.Bucket([]byte("widgets")).Get([]byte("no_such_key"))
			assert.Nil(t, value)
			return nil
		})
	})
}

// Ensure that a bucket can be created and retrieved.
func TestTx_CreateBucket(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		// Create a bucket.
		db.Update(func(tx *Tx) error {
			assert.NoError(t, tx.CreateBucket([]byte("widgets")))
			return nil
		})

		// Read the bucket through a separate transaction.
		db.View(func(tx *Tx) error {
			b := tx.Bucket([]byte("widgets"))
			assert.NotNil(t, b)
			return nil
		})
	})
}

// Ensure that a bucket can be created if it doesn't already exist.
func TestTx_CreateBucketIfNotExists(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.Update(func(tx *Tx) error {
			assert.NoError(t, tx.CreateBucketIfNotExists([]byte("widgets")))
			assert.NoError(t, tx.CreateBucketIfNotExists([]byte("widgets")))
			assert.Equal(t, ErrBucketNameRequired, tx.CreateBucketIfNotExists([]byte{}))
			assert.Equal(t, ErrBucketNameRequired, tx.CreateBucketIfNotExists(nil))
			return nil
		})

		// Read the bucket through a separate transaction.
		db.View(func(tx *Tx) error {
			b := tx.Bucket([]byte("widgets"))
			assert.NotNil(t, b)
			return nil
		})
	})
}

// Ensure that a bucket cannot be created twice.
func TestTx_CreateBucket_Exists(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		// Create a bucket.
		db.Update(func(tx *Tx) error {
			assert.NoError(t, tx.CreateBucket([]byte("widgets")))
			return nil
		})

		// Create the same bucket again.
		db.Update(func(tx *Tx) error {
			assert.Equal(t, ErrBucketExists, tx.CreateBucket([]byte("widgets")))
			return nil
		})
	})
}

// Ensure that a bucket is created with a non-blank name.
func TestTx_CreateBucket_NameRequired(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.Update(func(tx *Tx) error {
			assert.Equal(t, ErrBucketNameRequired, tx.CreateBucket(nil))
			return nil
		})
	})
}

// Ensure that a bucket can be deleted.
func TestTx_DeleteBucket(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		// Create a bucket and add a value.
		db.Update(func(tx *Tx) error {
			tx.CreateBucket([]byte("widgets"))
			tx.Bucket([]byte("widgets")).Put([]byte("foo"), []byte("bar"))
			return nil
		})

		// Save root page id.
		var root pgid
		db.View(func(tx *Tx) error {
			root = tx.Bucket([]byte("widgets")).root
			return nil
		})

		// Delete the bucket and make sure we can't get the value.
		db.Update(func(tx *Tx) error {
			assert.NoError(t, tx.DeleteBucket([]byte("widgets")))
			assert.Nil(t, tx.Bucket([]byte("widgets")))
			return nil
		})

		db.Update(func(tx *Tx) error {
			// Verify that the bucket's page is free.
			assert.Equal(t, []pgid{7, 6, root, 2}, db.freelist.all())

			// Create the bucket again and make sure there's not a phantom value.
			assert.NoError(t, tx.CreateBucket([]byte("widgets")))
			assert.Nil(t, tx.Bucket([]byte("widgets")).Get([]byte("foo")))
			return nil
		})
	})
}

// Ensure that deleting a bucket on a closed transaction returns an error.
func TestTx_DeleteBucket_Closed(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		tx, _ := db.Begin(true)
		tx.Commit()
		assert.Equal(t, tx.DeleteBucket([]byte("foo")), ErrTxClosed)
	})
}

// Ensure that deleting a bucket with a read-only transaction returns an error.
func TestTx_DeleteBucket_ReadOnly(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.View(func(tx *Tx) error {
			assert.Equal(t, tx.DeleteBucket([]byte("foo")), ErrTxNotWritable)
			return nil
		})
	})
}

// Ensure that nothing happens when deleting a bucket that doesn't exist.
func TestTx_DeleteBucket_NotFound(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.Update(func(tx *Tx) error {
			assert.Equal(t, ErrBucketNotFound, tx.DeleteBucket([]byte("widgets")))
			return nil
		})
	})
}

// Ensure that Tx commit handlers are called after a transaction successfully commits.
func TestTx_OnCommit(t *testing.T) {
	var x int
	withOpenDB(func(db *DB, path string) {
		db.Update(func(tx *Tx) error {
			tx.OnCommit(func() { x += 1 })
			tx.OnCommit(func() { x += 2 })
			return tx.CreateBucket([]byte("widgets"))
		})
	})
	assert.Equal(t, 3, x)
}

// Ensure that Tx commit handlers are NOT called after a transaction rolls back.
func TestTx_OnCommit_Rollback(t *testing.T) {
	var x int
	withOpenDB(func(db *DB, path string) {
		db.Update(func(tx *Tx) error {
			tx.OnCommit(func() { x += 1 })
			tx.OnCommit(func() { x += 2 })
			tx.CreateBucket([]byte("widgets"))
			return errors.New("rollback this commit")
		})
	})
	assert.Equal(t, 0, x)
}

// Benchmark the performance iterating over a cursor.
func BenchmarkTxCursor(b *testing.B) {
	var total = 50000
	indexes := rand.Perm(total)
	value := []byte(strings.Repeat("0", 100))

	warn("X", b.N)
	withOpenDB(func(db *DB, path string) {
		// Write data to bucket.
		db.Update(func(tx *Tx) error {
			tx.CreateBucket([]byte("widgets"))
			bucket := tx.Bucket([]byte("widgets"))
			for i := 0; i < total; i++ {
				bucket.Put([]byte(fmt.Sprintf("%016d", indexes[i])), value)
			}
			return nil
		})
		b.ResetTimer()

		// Iterate over bucket using cursor.
		for i := 0; i < b.N; i++ {
			db.View(func(tx *Tx) error {
				count := 0
				c := tx.Bucket([]byte("widgets")).Cursor()
				for k, _ := c.First(); k != nil; k, _ = c.Next() {
					count++
				}
				if count != total {
					b.Fatalf("wrong count: %d; expected: %d", count, total)
				}
				return nil
			})
		}
	})
}

// Benchmark the performance of bulk put transactions in random order.
func BenchmarkTxPutRandom(b *testing.B) {
	indexes := rand.Perm(b.N)
	value := []byte(strings.Repeat("0", 64))
	withOpenDB(func(db *DB, path string) {
		db.Update(func(tx *Tx) error {
			return tx.CreateBucket([]byte("widgets"))
		})
		var tx *Tx
		var bucket *Bucket
		for i := 0; i < b.N; i++ {
			if i%1000 == 0 {
				if tx != nil {
					tx.Commit()
				}
				tx, _ = db.Begin(true)
				bucket = tx.Bucket([]byte("widgets"))
			}
			bucket.Put([]byte(strconv.Itoa(indexes[i])), value)
		}
		tx.Commit()
	})
}

// Benchmark the performance of bulk put transactions in sequential order.
func BenchmarkTxPutSequential(b *testing.B) {
	value := []byte(strings.Repeat("0", 64))
	withOpenDB(func(db *DB, path string) {
		db.Update(func(tx *Tx) error {
			return tx.CreateBucket([]byte("widgets"))
		})
		db.Update(func(tx *Tx) error {
			bucket := tx.Bucket([]byte("widgets"))
			for i := 0; i < b.N; i++ {
				bucket.Put([]byte(strconv.Itoa(i)), value)
			}
			return nil
		})
	})
}

func ExampleTx_Rollback() {
	// Open the database.
	db, _ := Open(tempfile(), 0666)
	defer os.Remove(db.Path())
	defer db.Close()

	// Create a bucket.
	db.Update(func(tx *Tx) error {
		return tx.CreateBucket([]byte("widgets"))
	})

	// Set a value for a key.
	db.Update(func(tx *Tx) error {
		return tx.Bucket([]byte("widgets")).Put([]byte("foo"), []byte("bar"))
	})

	// Update the key but rollback the transaction so it never saves.
	tx, _ := db.Begin(true)
	b := tx.Bucket([]byte("widgets"))
	b.Put([]byte("foo"), []byte("baz"))
	tx.Rollback()

	// Ensure that our original value is still set.
	db.View(func(tx *Tx) error {
		value := tx.Bucket([]byte("widgets")).Get([]byte("foo"))
		fmt.Printf("The value for 'foo' is still: %s\n", string(value))
		return nil
	})

	// Output:
	// The value for 'foo' is still: bar
}
