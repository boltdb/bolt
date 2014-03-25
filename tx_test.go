package bolt

import (
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"testing/quick"

	"github.com/stretchr/testify/assert"
)

// Ensure that committing a closed transaction returns an error.
func TestTxCommitClosed(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		tx, _ := db.Begin(true)
		tx.CreateBucket("foo")
		assert.NoError(t, tx.Commit())
		assert.Equal(t, tx.Commit(), ErrTxClosed)
	})
}

// Ensure that rolling back a closed transaction returns an error.
func TestTxRollbackClosed(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		tx, _ := db.Begin(true)
		assert.NoError(t, tx.Rollback())
		assert.Equal(t, tx.Rollback(), ErrTxClosed)
	})
}

// Ensure that committing a read-only transaction returns an error.
func TestTxCommitReadOnly(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		tx, _ := db.Begin(false)
		assert.Equal(t, tx.Commit(), ErrTxNotWritable)
	})
}

// Ensure that the database can retrieve a list of buckets.
func TestTxBuckets(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.Update(func(tx *Tx) error {
			tx.CreateBucket("foo")
			tx.CreateBucket("bar")
			tx.CreateBucket("baz")
			buckets := tx.Buckets()
			if assert.Equal(t, len(buckets), 3) {
				assert.Equal(t, buckets[0].Name(), "bar")
				assert.Equal(t, buckets[1].Name(), "baz")
				assert.Equal(t, buckets[2].Name(), "foo")
			}
			return nil
		})
	})
}

// Ensure that creating a bucket with a read-only transaction returns an error.
func TestTxCreateBucketReadOnly(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.View(func(tx *Tx) error {
			assert.Equal(t, tx.CreateBucket("foo"), ErrTxNotWritable)
			return nil
		})
	})
}

// Ensure that creating a bucket on a closed transaction returns an error.
func TestTxCreateBucketClosed(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		tx, _ := db.Begin(true)
		tx.Commit()
		assert.Equal(t, tx.CreateBucket("foo"), ErrTxClosed)
	})
}

// Ensure that a Tx can retrieve a bucket.
func TestTxBucket(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.Update(func(tx *Tx) error {
			tx.CreateBucket("widgets")
			b := tx.Bucket("widgets")
			if assert.NotNil(t, b) {
				assert.Equal(t, "widgets", b.Name())
			}
			return nil
		})
	})
}

// Ensure that a Tx retrieving a non-existent key returns nil.
func TestTxGetMissing(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.Update(func(tx *Tx) error {
			tx.CreateBucket("widgets")
			tx.Bucket("widgets").Put([]byte("foo"), []byte("bar"))
			value := tx.Bucket("widgets").Get([]byte("no_such_key"))
			assert.Nil(t, value)
			return nil
		})
	})
}

// Ensure that retrieving all buckets returns writable buckets.
func TestTxWritableBuckets(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.Update(func(tx *Tx) error {
			tx.CreateBucket("widgets")
			tx.CreateBucket("woojits")
			return nil
		})
		db.Update(func(tx *Tx) error {
			buckets := tx.Buckets()
			assert.Equal(t, len(buckets), 2)
			assert.Equal(t, buckets[0].Name(), "widgets")
			assert.Equal(t, buckets[1].Name(), "woojits")
			buckets[0].Put([]byte("foo"), []byte("0000"))
			buckets[1].Put([]byte("bar"), []byte("0001"))
			return nil
		})
		db.View(func(tx *Tx) error {
			assert.Equal(t, []byte("0000"), tx.Bucket("widgets").Get([]byte("foo")))
			assert.Equal(t, []byte("0001"), tx.Bucket("woojits").Get([]byte("bar")))
			return nil
		})
	})
}

// Ensure that a bucket can be created and retrieved.
func TestTxCreateBucket(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		// Create a bucket.
		db.Update(func(tx *Tx) error {
			assert.NoError(t, tx.CreateBucket("widgets"))
			return nil
		})

		// Read the bucket through a separate transaction.
		db.View(func(tx *Tx) error {
			b := tx.Bucket("widgets")
			assert.NotNil(t, b)
			return nil
		})
	})
}

// Ensure that a bucket can be created if it doesn't already exist.
func TestTxCreateBucketIfNotExists(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.Update(func(tx *Tx) error {
			assert.NoError(t, tx.CreateBucketIfNotExists("widgets"))
			assert.NoError(t, tx.CreateBucketIfNotExists("widgets"))
			assert.Equal(t, tx.CreateBucketIfNotExists(""), ErrBucketNameRequired)
			return nil
		})

		// Read the bucket through a separate transaction.
		db.View(func(tx *Tx) error {
			b := tx.Bucket("widgets")
			assert.NotNil(t, b)
			return nil
		})
	})
}

// Ensure that a bucket cannot be created twice.
func TestTxRecreateBucket(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		// Create a bucket.
		db.Update(func(tx *Tx) error {
			assert.NoError(t, tx.CreateBucket("widgets"))
			return nil
		})

		// Create the same bucket again.
		db.Update(func(tx *Tx) error {
			assert.Equal(t, ErrBucketExists, tx.CreateBucket("widgets"))
			return nil
		})
	})
}

// Ensure that a bucket is created with a non-blank name.
func TestTxCreateBucketWithoutName(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.Update(func(tx *Tx) error {
			assert.Equal(t, ErrBucketNameRequired, tx.CreateBucket(""))
			return nil
		})
	})
}

// Ensure that a bucket name is not too long.
func TestTxCreateBucketWithLongName(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.Update(func(tx *Tx) error {
			assert.NoError(t, tx.CreateBucket(strings.Repeat("X", 255)))
			assert.Equal(t, ErrBucketNameTooLarge, tx.CreateBucket(strings.Repeat("X", 256)))
			return nil
		})
	})
}

// Ensure that a bucket can be deleted.
func TestTxDeleteBucket(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		// Create a bucket and add a value.
		db.Update(func(tx *Tx) error {
			tx.CreateBucket("widgets")
			tx.Bucket("widgets").Put([]byte("foo"), []byte("bar"))
			return nil
		})

		// Save root page id.
		var root pgid
		db.View(func(tx *Tx) error {
			root = tx.Bucket("widgets").root
			return nil
		})

		// Delete the bucket and make sure we can't get the value.
		db.Update(func(tx *Tx) error {
			assert.NoError(t, tx.DeleteBucket("widgets"))
			assert.Nil(t, tx.Bucket("widgets"))
			return nil
		})

		db.Update(func(tx *Tx) error {
			// Verify that the bucket's page is free.
			assert.Equal(t, []pgid{6, root, 3}, db.freelist.all())

			// Create the bucket again and make sure there's not a phantom value.
			assert.NoError(t, tx.CreateBucket("widgets"))
			assert.Nil(t, tx.Bucket("widgets").Get([]byte("foo")))
			return nil
		})
	})
}

// Ensure that deleting a bucket on a closed transaction returns an error.
func TestTxDeleteBucketClosed(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		tx, _ := db.Begin(true)
		tx.Commit()
		assert.Equal(t, tx.DeleteBucket("foo"), ErrTxClosed)
	})
}

// Ensure that deleting a bucket with a read-only transaction returns an error.
func TestTxDeleteBucketReadOnly(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.View(func(tx *Tx) error {
			assert.Equal(t, tx.DeleteBucket("foo"), ErrTxNotWritable)
			return nil
		})
	})
}

// Ensure that an error is returned when deleting from a bucket that doesn't exist.
func TestTxDeleteBucketNotFound(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.Update(func(tx *Tx) error {
			assert.Equal(t, ErrBucketNotFound, tx.DeleteBucket("widgets"))
			return nil
		})
	})
}

// Ensure that a Tx cursor can iterate over an empty bucket without error.
func TestTxCursorEmptyBucket(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.Update(func(tx *Tx) error {
			return tx.CreateBucket("widgets")
		})
		db.View(func(tx *Tx) error {
			c := tx.Bucket("widgets").Cursor()
			k, v := c.First()
			assert.Nil(t, k)
			assert.Nil(t, v)
			return nil
		})
	})
}

// Ensure that a Tx cursor can reverse iterate over an empty bucket without error.
func TestCursorEmptyBucketReverse(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.Update(func(tx *Tx) error {
			return tx.CreateBucket("widgets")
		})
		db.View(func(tx *Tx) error {
			c := tx.Bucket("widgets").Cursor()
			k, v := c.Last()
			assert.Nil(t, k)
			assert.Nil(t, v)
			return nil
		})
	})
}

// Ensure that a Tx cursor can iterate over a single root with a couple elements.
func TestTxCursorLeafRoot(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.Update(func(tx *Tx) error {
			tx.CreateBucket("widgets")
			tx.Bucket("widgets").Put([]byte("baz"), []byte{})
			tx.Bucket("widgets").Put([]byte("foo"), []byte{0})
			tx.Bucket("widgets").Put([]byte("bar"), []byte{1})
			return nil
		})
		tx, _ := db.Begin(false)
		c := tx.Bucket("widgets").Cursor()

		k, v := c.First()
		assert.Equal(t, string(k), "bar")
		assert.Equal(t, v, []byte{1})

		k, v = c.Next()
		assert.Equal(t, string(k), "baz")
		assert.Equal(t, v, []byte{})

		k, v = c.Next()
		assert.Equal(t, string(k), "foo")
		assert.Equal(t, v, []byte{0})

		k, v = c.Next()
		assert.Nil(t, k)
		assert.Nil(t, v)

		k, v = c.Next()
		assert.Nil(t, k)
		assert.Nil(t, v)

		tx.Rollback()
	})
}

// Ensure that a Tx cursor can iterate in reverse over a single root with a couple elements.
func TestTxCursorLeafRootReverse(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.Update(func(tx *Tx) error {
			tx.CreateBucket("widgets")
			tx.Bucket("widgets").Put([]byte("baz"), []byte{})
			tx.Bucket("widgets").Put([]byte("foo"), []byte{0})
			tx.Bucket("widgets").Put([]byte("bar"), []byte{1})
			return nil
		})
		tx, _ := db.Begin(false)
		c := tx.Bucket("widgets").Cursor()

		k, v := c.Last()
		assert.Equal(t, string(k), "foo")
		assert.Equal(t, v, []byte{0})

		k, v = c.Prev()
		assert.Equal(t, string(k), "baz")
		assert.Equal(t, v, []byte{})

		k, v = c.Prev()
		assert.Equal(t, string(k), "bar")
		assert.Equal(t, v, []byte{1})

		k, v = c.Prev()
		assert.Nil(t, k)
		assert.Nil(t, v)

		k, v = c.Prev()
		assert.Nil(t, k)
		assert.Nil(t, v)

		tx.Rollback()
	})
}

// Ensure that a Tx cursor can restart from the beginning.
func TestTxCursorRestart(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.Update(func(tx *Tx) error {
			tx.CreateBucket("widgets")
			tx.Bucket("widgets").Put([]byte("bar"), []byte{})
			tx.Bucket("widgets").Put([]byte("foo"), []byte{})
			return nil
		})

		tx, _ := db.Begin(false)
		c := tx.Bucket("widgets").Cursor()

		k, _ := c.First()
		assert.Equal(t, string(k), "bar")

		k, _ = c.Next()
		assert.Equal(t, string(k), "foo")

		k, _ = c.First()
		assert.Equal(t, string(k), "bar")

		k, _ = c.Next()
		assert.Equal(t, string(k), "foo")

		tx.Rollback()
	})
}

// Ensure that a Tx can iterate over all elements in a bucket.
func TestTxCursorIterate(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	f := func(items testdata) bool {
		withOpenDB(func(db *DB, path string) {
			// Bulk insert all values.
			tx, _ := db.Begin(true)
			tx.CreateBucket("widgets")
			b := tx.Bucket("widgets")
			for _, item := range items {
				assert.NoError(t, b.Put(item.Key, item.Value))
			}
			assert.NoError(t, tx.Commit())

			// Sort test data.
			sort.Sort(items)

			// Iterate over all items and check consistency.
			var index = 0
			tx, _ = db.Begin(false)
			c := tx.Bucket("widgets").Cursor()
			for k, v := c.First(); k != nil && index < len(items); k, v = c.Next() {
				assert.Equal(t, k, items[index].Key)
				assert.Equal(t, v, items[index].Value)
				index++
			}
			assert.Equal(t, len(items), index)
			tx.Rollback()
		})
		fmt.Fprint(os.Stderr, ".")
		return true
	}
	if err := quick.Check(f, qconfig()); err != nil {
		t.Error(err)
	}
	fmt.Fprint(os.Stderr, "\n")
}

// Ensure that a transaction can iterate over all elements in a bucket in reverse.
func TestTxCursorIterateReverse(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	f := func(items testdata) bool {
		withOpenDB(func(db *DB, path string) {
			// Bulk insert all values.
			tx, _ := db.Begin(true)
			tx.CreateBucket("widgets")
			b := tx.Bucket("widgets")
			for _, item := range items {
				assert.NoError(t, b.Put(item.Key, item.Value))
			}
			assert.NoError(t, tx.Commit())

			// Sort test data.
			sort.Sort(revtestdata(items))

			// Iterate over all items and check consistency.
			var index = 0
			tx, _ = db.Begin(false)
			c := tx.Bucket("widgets").Cursor()
			for k, v := c.Last(); k != nil && index < len(items); k, v = c.Prev() {
				assert.Equal(t, k, items[index].Key)
				assert.Equal(t, v, items[index].Value)
				index++
			}
			assert.Equal(t, len(items), index)
			tx.Rollback()
		})
		fmt.Fprint(os.Stderr, ".")
		return true
	}
	if err := quick.Check(f, qconfig()); err != nil {
		t.Error(err)
	}
	fmt.Fprint(os.Stderr, "\n")
}

// Benchmark the performance iterating over a cursor.
func BenchmarkTxCursor(b *testing.B) {
	indexes := rand.Perm(b.N)
	value := []byte(strings.Repeat("0", 64))

	withOpenDB(func(db *DB, path string) {
		// Write data to bucket.
		db.Update(func(tx *Tx) error {
			tx.CreateBucket("widgets")
			bucket := tx.Bucket("widgets")
			for i := 0; i < b.N; i++ {
				bucket.Put([]byte(strconv.Itoa(indexes[i])), value)
			}
			return nil
		})
		b.ResetTimer()

		// Iterate over bucket using cursor.
		db.View(func(tx *Tx) error {
			count := 0
			c := tx.Bucket("widgets").Cursor()
			for k, _ := c.First(); k != nil; k, _ = c.Next() {
				count++
			}
			if count != b.N {
				b.Fatalf("wrong count: %d; expected: %d", count, b.N)
			}
			return nil
		})
	})
}

// Benchmark the performance of bulk put transactions in random order.
func BenchmarkTxPutRandom(b *testing.B) {
	indexes := rand.Perm(b.N)
	value := []byte(strings.Repeat("0", 64))
	withOpenDB(func(db *DB, path string) {
		db.Update(func(tx *Tx) error {
			return tx.CreateBucket("widgets")
		})
		var tx *Tx
		var bucket *Bucket
		for i := 0; i < b.N; i++ {
			if i%1000 == 0 {
				if tx != nil {
					tx.Commit()
				}
				tx, _ = db.Begin(true)
				bucket = tx.Bucket("widgets")
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
			return tx.CreateBucket("widgets")
		})
		db.Update(func(tx *Tx) error {
			bucket := tx.Bucket("widgets")
			for i := 0; i < b.N; i++ {
				bucket.Put([]byte(strconv.Itoa(i)), value)
			}
			return nil
		})
	})
}
