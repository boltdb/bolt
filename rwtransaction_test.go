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

// Ensure that a bucket cannot be created twice.
func TestRWTransactionRecreateBucket(t *testing.T) {
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
func TestRWTransactionCreateBucketWithoutName(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		err := db.CreateBucket("")
		assert.Equal(t, err, &Error{"bucket name cannot be blank", nil})
	})
}

// Ensure that a bucket name is not too long.
func TestRWTransactionCreateBucketWithLongName(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		err := db.CreateBucket(strings.Repeat("X", 255))
		assert.NoError(t, err)

		err = db.CreateBucket(strings.Repeat("X", 256))
		assert.Equal(t, err, &Error{"bucket name too long", nil})
	})
}

// Ensure that a bucket can be deleted.
func TestRWTransactionDeleteBucket(t *testing.T) {
	t.Skip("pending") // TODO(benbjohnson)
}

// Ensure that an error is returned when inserting into a bucket that doesn't exist.
func TestRWTransactionPutBucketNotFound(t *testing.T) {
	t.Skip("pending") // TODO(benbjohnson)
}

// Ensure that an error is returned when inserting with an empty key.
func TestRWTransactionPutEmptyKey(t *testing.T) {
	t.Skip("pending") // TODO(benbjohnson)
}

// Ensure that an error is returned when inserting with a key that's too large.
func TestRWTransactionPutKeyTooLarge(t *testing.T) {
	t.Skip("pending") // TODO(benbjohnson)
}

// Ensure that an error is returned when inserting with data that's too large.
func TestRWTransactionPutDataTooLarge(t *testing.T) {
	t.Skip("pending") // TODO(benbjohnson)
}

// Ensure that an error is returned when deleting from a bucket that doesn't exist.
func TestRWTransactionDeleteBucketNotFound(t *testing.T) {
	t.Skip("pending") // TODO(benbjohnson)
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
						db.CopyFile("/tmp/bolt.put.single.db")
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
				if !assert.Equal(t, item.Value, txn.Get("widgets", item.Key)) {
					db.CopyFile("/tmp/bolt.put.multiple.db")
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
						if !assert.Equal(t, exp.Value, txn.Get("widgets", exp.Key)) {
							t.FailNow()
						}
					} else {
						if !assert.Nil(t, txn.Get("widgets", exp.Key)) {
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
