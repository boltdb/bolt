package bolt

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"testing"
	"testing/quick"

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

// Ensure that a bucket can return an autoincrementing sequence.
func TestRWTransactionNextSequence(t *testing.T) {
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
func TestRWTransactionNextSequenceOverflow(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.CreateBucket("widgets")
		db.Do(func(txn *RWTransaction) error {
			b := txn.Bucket("widgets")
			b.bucket.sequence = uint64(maxInt)
			seq, err := txn.NextSequence("widgets")
			assert.Equal(t, err, ErrSequenceOverflow)
			assert.Equal(t, seq, 0)
			return nil
		})
	})
}

// Ensure that an error is returned when inserting into a bucket that doesn't exist.
func TestRWTransactionPutBucketNotFound(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		err := db.Put("widgets", []byte("foo"), []byte("bar"))
		assert.Equal(t, err, ErrBucketNotFound)
	})
}

// Ensure that an error is returned when inserting with an empty key.
func TestRWTransactionPutEmptyKey(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.CreateBucket("widgets")
		err := db.Put("widgets", []byte(""), []byte("bar"))
		assert.Equal(t, err, ErrKeyRequired)
		err = db.Put("widgets", nil, []byte("bar"))
		assert.Equal(t, err, ErrKeyRequired)
	})
}

// Ensure that an error is returned when inserting with a key that's too large.
func TestRWTransactionPutKeyTooLarge(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.CreateBucket("widgets")
		err := db.Put("widgets", make([]byte, 32769), []byte("bar"))
		assert.Equal(t, err, ErrKeyTooLarge)
	})
}

// Ensure that an error is returned when deleting from a bucket that doesn't exist.
func TestRWTransactionDeleteBucketNotFound(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		err := db.DeleteBucket("widgets")
		assert.Equal(t, err, ErrBucketNotFound)
	})
}

// Ensure that a bucket can write random keys and values across multiple txns.
func TestRWTransactionPutSingle(t *testing.T) {
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
func TestRWTransactionPutMultiple(t *testing.T) {
	f := func(items testdata) bool {
		withOpenDB(func(db *DB, path string) {
			// Bulk insert all values.
			db.CreateBucket("widgets")
			rwtxn, _ := db.RWTransaction()
			for _, item := range items {
				assert.NoError(t, rwtxn.Put("widgets", item.Key, item.Value))
			}
			assert.NoError(t, rwtxn.Commit())

			// Verify all items exist.
			txn, _ := db.Transaction()
			for _, item := range items {
				value, err := txn.Get("widgets", item.Key)
				assert.NoError(t, err)
				if !assert.Equal(t, item.Value, value) {
					db.CopyFile("/tmp/bolt.put.multiple.db", 0666)
					t.FailNow()
				}
			}
			txn.Close()
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
func TestRWTransactionDelete(t *testing.T) {
	f := func(items testdata) bool {
		withOpenDB(func(db *DB, path string) {
			// Bulk insert all values.
			db.CreateBucket("widgets")
			rwtxn, _ := db.RWTransaction()
			for _, item := range items {
				assert.NoError(t, rwtxn.Put("widgets", item.Key, item.Value))
			}
			assert.NoError(t, rwtxn.Commit())

			// Remove items one at a time and check consistency.
			for i, item := range items {
				assert.NoError(t, db.Delete("widgets", item.Key))

				// Anything before our deletion index should be nil.
				txn, _ := db.Transaction()
				for j, exp := range items {
					if j > i {
						value, err := txn.Get("widgets", exp.Key)
						assert.NoError(t, err)
						if !assert.Equal(t, exp.Value, value) {
							t.FailNow()
						}
					} else {
						value, err := txn.Get("widgets", exp.Key)
						assert.NoError(t, err)
						if !assert.Nil(t, value) {
							t.FailNow()
						}
					}
				}
				txn.Close()
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
