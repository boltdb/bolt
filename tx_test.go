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

// Ensure that the database can retrieve a list of buckets.
func TestTxBuckets(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.CreateBucket("foo")
		db.CreateBucket("bar")
		db.CreateBucket("baz")
		buckets, err := db.Buckets()
		if assert.NoError(t, err) && assert.Equal(t, len(buckets), 3) {
			assert.Equal(t, buckets[0].Name(), "bar")
			assert.Equal(t, buckets[1].Name(), "baz")
			assert.Equal(t, buckets[2].Name(), "foo")
		}
	})
}

// Ensure that creating a bucket with a read-only transaction returns an error.
func TestTxCreateBucketReadOnly(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.With(func(tx *Tx) error {
			assert.Equal(t, tx.CreateBucket("foo"), ErrTxNotWritable)
			return nil
		})
	})
}

// Ensure that a Tx can retrieve a bucket.
func TestTxBucketMissing(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.CreateBucket("widgets")
		b, err := db.Bucket("widgets")
		assert.NoError(t, err)
		if assert.NotNil(t, b) {
			assert.Equal(t, "widgets", b.Name())
		}
	})
}

// Ensure that a Tx retrieving a non-existent key returns nil.
func TestTxGetMissing(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.CreateBucket("widgets")
		db.Put("widgets", []byte("foo"), []byte("bar"))
		value, err := db.Get("widgets", []byte("no_such_key"))
		assert.NoError(t, err)
		assert.Nil(t, value)
	})
}

// Ensure that retrieving all buckets returns writable buckets.
func TestTxWritableBuckets(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.CreateBucket("widgets")
		db.CreateBucket("woojits")
		db.Do(func(tx *Tx) error {
			buckets := tx.Buckets()
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
func TestTxCreateBucket(t *testing.T) {
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
func TestTxCreateBucketIfNotExists(t *testing.T) {
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
func TestTxRecreateBucket(t *testing.T) {
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
func TestTxCreateBucketWithoutName(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		err := db.CreateBucket("")
		assert.Equal(t, err, ErrBucketNameRequired)
	})
}

// Ensure that a bucket name is not too long.
func TestTxCreateBucketWithLongName(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		err := db.CreateBucket(strings.Repeat("X", 255))
		assert.NoError(t, err)

		err = db.CreateBucket(strings.Repeat("X", 256))
		assert.Equal(t, err, ErrBucketNameTooLarge)
	})
}

// Ensure that a bucket can be deleted.
func TestTxDeleteBucket(t *testing.T) {
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

// Ensure that deleting a bucket with a read-only transaction returns an error.
func TestTxDeleteBucketReadOnly(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.With(func(tx *Tx) error {
			assert.Equal(t, tx.DeleteBucket("foo"), ErrTxNotWritable)
			return nil
		})
	})
}

// Ensure that an error is returned when deleting from a bucket that doesn't exist.
func TestTxDeleteBucketNotFound(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		err := db.DeleteBucket("widgets")
		assert.Equal(t, err, ErrBucketNotFound)
	})
}

// Ensure that a Tx cursor can iterate over an empty bucket without error.
func TestTxCursorEmptyBucket(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.CreateBucket("widgets")
		db.With(func(tx *Tx) error {
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
		db.CreateBucket("widgets")
		db.With(func(tx *Tx) error {
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
		db.CreateBucket("widgets")
		db.Put("widgets", []byte("baz"), []byte{})
		db.Put("widgets", []byte("foo"), []byte{0})
		db.Put("widgets", []byte("bar"), []byte{1})
		tx, _ := db.Tx()
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
		db.CreateBucket("widgets")
		db.Put("widgets", []byte("baz"), []byte{})
		db.Put("widgets", []byte("foo"), []byte{0})
		db.Put("widgets", []byte("bar"), []byte{1})
		tx, _ := db.Tx()
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
		db.CreateBucket("widgets")
		db.Put("widgets", []byte("bar"), []byte{})
		db.Put("widgets", []byte("foo"), []byte{})

		tx, _ := db.Tx()
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
	f := func(items testdata) bool {
		withOpenDB(func(db *DB, path string) {
			// Bulk insert all values.
			db.CreateBucket("widgets")
			tx, _ := db.RWTx()
			b := tx.Bucket("widgets")
			for _, item := range items {
				assert.NoError(t, b.Put(item.Key, item.Value))
			}
			assert.NoError(t, tx.Commit())

			// Sort test data.
			sort.Sort(items)

			// Iterate over all items and check consistency.
			var index = 0
			tx, _ = db.Tx()
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
	f := func(items testdata) bool {
		withOpenDB(func(db *DB, path string) {
			// Bulk insert all values.
			db.CreateBucket("widgets")
			tx, _ := db.RWTx()
			b := tx.Bucket("widgets")
			for _, item := range items {
				assert.NoError(t, b.Put(item.Key, item.Value))
			}
			assert.NoError(t, tx.Commit())

			// Sort test data.
			sort.Sort(revtestdata(items))

			// Iterate over all items and check consistency.
			var index = 0
			tx, _ = db.Tx()
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
		db.CreateBucket("widgets")
		db.Do(func(tx *Tx) error {
			bucket := tx.Bucket("widgets")
			for i := 0; i < b.N; i++ {
				bucket.Put([]byte(strconv.Itoa(indexes[i])), value)
			}
			return nil
		})
		b.ResetTimer()

		// Iterate over bucket using cursor.
		db.With(func(tx *Tx) error {
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
		db.CreateBucket("widgets")
		var tx *Tx
		var bucket *Bucket
		for i := 0; i < b.N; i++ {
			if i%1000 == 0 {
				if tx != nil {
					tx.Commit()
				}
				tx, _ = db.RWTx()
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
		db.CreateBucket("widgets")
		db.Do(func(tx *Tx) error {
			bucket := tx.Bucket("widgets")
			for i := 0; i < b.N; i++ {
				bucket.Put([]byte(strconv.Itoa(i)), value)
			}
			return nil
		})
	})
}
