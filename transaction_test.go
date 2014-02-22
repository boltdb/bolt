package bolt

import (
	"bytes"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"testing/quick"

	"github.com/stretchr/testify/assert"
)

// Ensure that the database can retrieve a list of buckets.
func TestTransactionBuckets(t *testing.T) {
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

// Ensure that a Transaction cursor can iterate over an empty bucket without error.
func TestTransactionCursorEmptyBucket(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.CreateBucket("widgets")
		txn, _ := db.Transaction()
		c := txn.Bucket("widgets").Cursor()
		k, v := c.First()
		assert.Nil(t, k)
		assert.Nil(t, v)
		txn.Rollback()
	})
}

// Ensure that a Transaction cursor can iterate over a single root with a couple elements.
func TestTransactionCursorLeafRoot(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.CreateBucket("widgets")
		db.Put("widgets", []byte("baz"), []byte{})
		db.Put("widgets", []byte("foo"), []byte{0})
		db.Put("widgets", []byte("bar"), []byte{1})
		txn, _ := db.Transaction()
		c := txn.Bucket("widgets").Cursor()

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

		txn.Rollback()
	})
}

// Ensure that a Transaction cursor can iterate in reverse over a single root with a couple elements.
func TestTransactionCursorLeafRootReverse(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.CreateBucket("widgets")
		db.Put("widgets", []byte("baz"), []byte{})
		db.Put("widgets", []byte("foo"), []byte{0})
		db.Put("widgets", []byte("bar"), []byte{1})
		txn, _ := db.Transaction()
		c := txn.Bucket("widgets").Cursor()

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

		txn.Rollback()
	})
}

// Ensure that a Transaction cursor can restart from the beginning.
func TestTransactionCursorRestart(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.CreateBucket("widgets")
		db.Put("widgets", []byte("bar"), []byte{})
		db.Put("widgets", []byte("foo"), []byte{})

		txn, _ := db.Transaction()
		c := txn.Bucket("widgets").Cursor()

		k, _ := c.First()
		assert.Equal(t, string(k), "bar")

		k, _ = c.Next()
		assert.Equal(t, string(k), "foo")

		k, _ = c.First()
		assert.Equal(t, string(k), "bar")

		k, _ = c.Next()
		assert.Equal(t, string(k), "foo")

		txn.Rollback()
	})
}

// Ensure that a transaction can iterate over all elements in a bucket.
func TestTransactionCursorIterate(t *testing.T) {
	f := func(items testdata) bool {
		withOpenDB(func(db *DB, path string) {
			// Bulk insert all values.
			db.CreateBucket("widgets")
			txn, _ := db.RWTransaction()
			b := txn.Bucket("widgets")
			for _, item := range items {
				assert.NoError(t, b.Put(item.Key, item.Value))
			}
			assert.NoError(t, txn.Commit())

			// Sort test data.
			sort.Sort(items)

			// Iterate over all items and check consistency.
			var index = 0
			txn, _ = db.Transaction()
			c := txn.Bucket("widgets").Cursor()
			for k, v := c.First(); k != nil && index < len(items); k, v = c.Next() {
				assert.Equal(t, k, items[index].Key)
				assert.Equal(t, v, items[index].Value)
				index++
			}
			assert.Equal(t, len(items), index)
			txn.Rollback()
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
func TestTransactionCursorIterateReverse(t *testing.T) {
	f := func(items testdata) bool {
		withOpenDB(func(db *DB, path string) {
			// Bulk insert all values.
			db.CreateBucket("widgets")
			txn, _ := db.RWTransaction()
			b := txn.Bucket("widgets")
			for _, item := range items {
				assert.NoError(t, b.Put(item.Key, item.Value))
			}
			assert.NoError(t, txn.Commit())

			// Sort test data.
			sort.Sort(revtestdata(items))

			// Iterate over all items and check consistency.
			var index = 0
			txn, _ = db.Transaction()
			c := txn.Bucket("widgets").Cursor()
			for k, v := c.Last(); k != nil && index < len(items); k, v = c.Prev() {
				assert.Equal(t, k, items[index].Key)
				assert.Equal(t, v, items[index].Value)
				index++
			}
			assert.Equal(t, len(items), index)
			txn.Rollback()
		})
		fmt.Fprint(os.Stderr, ".")
		return true
	}
	if err := quick.Check(f, qconfig()); err != nil {
		t.Error(err)
	}
	fmt.Fprint(os.Stderr, "\n")
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

// Ensure that a bucket can be created if it doesn't already exist.
func TestTransactionCreateBucketIfNotExists(t *testing.T) {
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
func TestTransactionRecreateBucket(t *testing.T) {
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
func TestTransactionCreateBucketWithoutName(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		err := db.CreateBucket("")
		assert.Equal(t, err, ErrBucketNameRequired)
	})
}

// Ensure that a bucket name is not too long.
func TestTransactionCreateBucketWithLongName(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		err := db.CreateBucket(strings.Repeat("X", 255))
		assert.NoError(t, err)

		err = db.CreateBucket(strings.Repeat("X", 256))
		assert.Equal(t, err, ErrBucketNameTooLarge)
	})
}

// Ensure that a bucket can be deleted.
func TestTransactionDeleteBucket(t *testing.T) {
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

// Ensure that a bucket can return an autoincrementing sequence.
func TestTransactionNextSequence(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.CreateBucket("widgets")
		db.CreateBucket("woojits")

		// Make sure sequence increments.
		seq, err := db.NextSequence("widgets")
		assert.NoError(t, err)
		assert.Equal(t, seq, 1)
		seq, err = db.NextSequence("widgets")
		assert.NoError(t, err)
		assert.Equal(t, seq, 2)

		// Buckets should be separate.
		seq, err = db.NextSequence("woojits")
		assert.NoError(t, err)
		assert.Equal(t, seq, 1)

		// Missing buckets return an error.
		seq, err = db.NextSequence("no_such_bucket")
		assert.Equal(t, err, ErrBucketNotFound)
		assert.Equal(t, seq, 0)
	})
}

// Ensure that incrementing past the maximum sequence number will return an error.
func TestTransactionNextSequenceOverflow(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.CreateBucket("widgets")
		db.Do(func(txn *Transaction) error {
			b := txn.Bucket("widgets")
			b.bucket.sequence = uint64(maxInt)
			seq, err := b.NextSequence()
			assert.Equal(t, err, ErrSequenceOverflow)
			assert.Equal(t, seq, 0)
			return nil
		})
	})
}

// Ensure that an error is returned when inserting into a bucket that doesn't exist.
func TestTransactionPutBucketNotFound(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		err := db.Put("widgets", []byte("foo"), []byte("bar"))
		assert.Equal(t, err, ErrBucketNotFound)
	})
}

// Ensure that an error is returned when inserting with an empty key.
func TestTransactionPutEmptyKey(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.CreateBucket("widgets")
		err := db.Put("widgets", []byte(""), []byte("bar"))
		assert.Equal(t, err, ErrKeyRequired)
		err = db.Put("widgets", nil, []byte("bar"))
		assert.Equal(t, err, ErrKeyRequired)
	})
}

// Ensure that an error is returned when inserting with a key that's too large.
func TestTransactionPutKeyTooLarge(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.CreateBucket("widgets")
		err := db.Put("widgets", make([]byte, 32769), []byte("bar"))
		assert.Equal(t, err, ErrKeyTooLarge)
	})
}

// Ensure that an error is returned when deleting from a bucket that doesn't exist.
func TestTransactionDeleteBucketNotFound(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		err := db.DeleteBucket("widgets")
		assert.Equal(t, err, ErrBucketNotFound)
	})
}

// Ensure that a bucket can write random keys and values across multiple txns.
func TestTransactionPutSingle(t *testing.T) {
	index := 0
	f := func(items testdata) bool {
		withOpenDB(func(db *DB, path string) {
			m := make(map[string][]byte)

			db.CreateBucket("widgets")
			for _, item := range items {
				if err := db.Put("widgets", item.Key, item.Value); err != nil {
					panic("put error: " + err.Error())
				}
				m[string(item.Key)] = item.Value

				// Verify all key/values so far.
				i := 0
				for k, v := range m {
					value, err := db.Get("widgets", []byte(k))
					if err != nil {
						panic("get error: " + err.Error())
					}
					if !bytes.Equal(value, v) {
						db.CopyFile("/tmp/bolt.put.single.db", 0666)
						t.Fatalf("value mismatch [run %d] (%d of %d):\nkey: %x\ngot: %x\nexp: %x", index, i, len(m), []byte(k), value, v)
					}
					i++
				}
			}

			fmt.Fprint(os.Stderr, ".")
		})
		index++
		return true
	}
	if err := quick.Check(f, qconfig()); err != nil {
		t.Error(err)
	}
	fmt.Fprint(os.Stderr, "\n")
}

// Ensure that a transaction can insert multiple key/value pairs at once.
func TestTransactionPutMultiple(t *testing.T) {
	f := func(items testdata) bool {
		withOpenDB(func(db *DB, path string) {
			// Bulk insert all values.
			db.CreateBucket("widgets")
			txn, _ := db.RWTransaction()
			b := txn.Bucket("widgets")
			for _, item := range items {
				assert.NoError(t, b.Put(item.Key, item.Value))
			}
			assert.NoError(t, txn.Commit())

			// Verify all items exist.
			txn, _ = db.Transaction()
			for _, item := range items {
				value := txn.Bucket("widgets").Get(item.Key)
				if !assert.Equal(t, item.Value, value) {
					db.CopyFile("/tmp/bolt.put.multiple.db", 0666)
					t.FailNow()
				}
			}
			txn.Rollback()
		})
		fmt.Fprint(os.Stderr, ".")
		return true
	}
	if err := quick.Check(f, qconfig()); err != nil {
		t.Error(err)
	}
	fmt.Fprint(os.Stderr, "\n")
}

// Ensure that a transaction can delete all key/value pairs and return to a single leaf page.
func TestTransactionDelete(t *testing.T) {
	f := func(items testdata) bool {
		withOpenDB(func(db *DB, path string) {
			// Bulk insert all values.
			db.CreateBucket("widgets")
			txn, _ := db.RWTransaction()
			b := txn.Bucket("widgets")
			for _, item := range items {
				assert.NoError(t, b.Put(item.Key, item.Value))
			}
			assert.NoError(t, txn.Commit())

			// Remove items one at a time and check consistency.
			for i, item := range items {
				assert.NoError(t, db.Delete("widgets", item.Key))

				// Anything before our deletion index should be nil.
				txn, _ := db.Transaction()
				for j, exp := range items {
					if j > i {
						value := txn.Bucket("widgets").Get(exp.Key)
						if !assert.Equal(t, exp.Value, value) {
							t.FailNow()
						}
					} else {
						value := txn.Bucket("widgets").Get(exp.Key)
						if !assert.Nil(t, value) {
							t.FailNow()
						}
					}
				}
				txn.Rollback()
			}
		})
		fmt.Fprint(os.Stderr, ".")
		return true
	}
	if err := quick.Check(f, qconfig()); err != nil {
		t.Error(err)
	}
	fmt.Fprint(os.Stderr, "\n")
}
