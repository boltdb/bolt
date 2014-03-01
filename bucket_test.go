package bolt

import (
	"bytes"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"testing/quick"

	"github.com/stretchr/testify/assert"
)

// Ensure that a bucket that gets a non-existent key returns nil.
func TestBucketGetNonExistent(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.CreateBucket("widgets")
		value, err := db.Get("widgets", []byte("foo"))
		if assert.NoError(t, err) {
			assert.Nil(t, value)
		}
	})
}

// Ensure that a bucket can read a value that is not flushed yet.
func TestBucketGetFromNode(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.CreateBucket("widgets")
		db.Do(func(txn *RWTransaction) error {
			b := txn.Bucket("widgets")
			b.Put([]byte("foo"), []byte("bar"))
			value := b.Get([]byte("foo"))
			assert.Equal(t, value, []byte("bar"))
			return nil
		})
	})
}

// Ensure that a bucket can write a key/value.
func TestBucketPut(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.CreateBucket("widgets")
		err := db.Put("widgets", []byte("foo"), []byte("bar"))
		assert.NoError(t, err)
		value, err := db.Get("widgets", []byte("foo"))
		if assert.NoError(t, err) {
			assert.Equal(t, value, []byte("bar"))
		}
	})
}

// Ensure that setting a value on a read-only bucket returns an error.
func TestBucketPutReadOnly(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.CreateBucket("widgets")
		db.With(func(txn *Transaction) error {
			b := txn.Bucket("widgets")
			err := b.Put([]byte("foo"), []byte("bar"))
			assert.Equal(t, err, ErrBucketNotWritable)
			return nil
		})
	})
}

// Ensure that a bucket can delete an existing key.
func TestBucketDelete(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.CreateBucket("widgets")
		db.Put("widgets", []byte("foo"), []byte("bar"))
		err := db.Delete("widgets", []byte("foo"))
		assert.NoError(t, err)
		value, err := db.Get("widgets", []byte("foo"))
		if assert.NoError(t, err) {
			assert.Nil(t, value)
		}
	})
}

// Ensure that deleting a key on a read-only bucket returns an error.
func TestBucketDeleteReadOnly(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.CreateBucket("widgets")
		db.With(func(txn *Transaction) error {
			b := txn.Bucket("widgets")
			err := b.Delete([]byte("foo"))
			assert.Equal(t, err, ErrBucketNotWritable)
			return nil
		})
	})
}

// Ensure that a bucket can return an autoincrementing sequence.
func TestBucketNextSequence(t *testing.T) {
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

// Ensure that retrieving the next sequence on a read-only bucket returns an error.
func TestBucketNextSequenceReadOnly(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.CreateBucket("widgets")
		db.With(func(txn *Transaction) error {
			b := txn.Bucket("widgets")
			i, err := b.NextSequence()
			assert.Equal(t, i, 0)
			assert.Equal(t, err, ErrBucketNotWritable)
			return nil
		})
	})
}

// Ensure that incrementing past the maximum sequence number will return an error.
func TestBucketNextSequenceOverflow(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.CreateBucket("widgets")
		db.Do(func(txn *RWTransaction) error {
			b := txn.Bucket("widgets")
			b.bucket.sequence = uint64(maxInt)
			seq, err := b.NextSequence()
			assert.Equal(t, err, ErrSequenceOverflow)
			assert.Equal(t, seq, 0)
			return nil
		})
	})
}

// Ensure a database can loop over all key/value pairs in a bucket.
func TestBucketForEach(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.CreateBucket("widgets")
		db.Put("widgets", []byte("foo"), []byte("0000"))
		db.Put("widgets", []byte("baz"), []byte("0001"))
		db.Put("widgets", []byte("bar"), []byte("0002"))

		var index int
		err := db.ForEach("widgets", func(k, v []byte) error {
			switch index {
			case 0:
				assert.Equal(t, k, []byte("bar"))
				assert.Equal(t, v, []byte("0002"))
			case 1:
				assert.Equal(t, k, []byte("baz"))
				assert.Equal(t, v, []byte("0001"))
			case 2:
				assert.Equal(t, k, []byte("foo"))
				assert.Equal(t, v, []byte("0000"))
			}
			index++
			return nil
		})
		assert.NoError(t, err)
		assert.Equal(t, index, 3)
	})
}

// Ensure a database can stop iteration early.
func TestBucketForEachShortCircuit(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.CreateBucket("widgets")
		db.Put("widgets", []byte("bar"), []byte("0000"))
		db.Put("widgets", []byte("baz"), []byte("0000"))
		db.Put("widgets", []byte("foo"), []byte("0000"))

		var index int
		err := db.ForEach("widgets", func(k, v []byte) error {
			index++
			if bytes.Equal(k, []byte("baz")) {
				return &Error{"marker", nil}
			}
			return nil
		})
		assert.Equal(t, err, &Error{"marker", nil})
		assert.Equal(t, index, 2)
	})
}

// Ensure that an error is returned when inserting with an empty key.
func TestBucketPutEmptyKey(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.CreateBucket("widgets")
		err := db.Put("widgets", []byte(""), []byte("bar"))
		assert.Equal(t, err, ErrKeyRequired)
		err = db.Put("widgets", nil, []byte("bar"))
		assert.Equal(t, err, ErrKeyRequired)
	})
}

// Ensure that an error is returned when inserting with a key that's too large.
func TestBucketPutKeyTooLarge(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.CreateBucket("widgets")
		err := db.Put("widgets", make([]byte, 32769), []byte("bar"))
		assert.Equal(t, err, ErrKeyTooLarge)
	})
}

// Ensure a bucket can calculate stats.
func TestBucketStat(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.Do(func(txn *RWTransaction) error {
			// Add bucket with lots of keys.
			txn.CreateBucket("widgets")
			b := txn.Bucket("widgets")
			for i := 0; i < 100000; i++ {
				b.Put([]byte(strconv.Itoa(i)), []byte(strconv.Itoa(i)))
			}

			// Add bucket with fewer keys but one big value.
			txn.CreateBucket("woojits")
			b = txn.Bucket("woojits")
			for i := 0; i < 500; i++ {
				b.Put([]byte(strconv.Itoa(i)), []byte(strconv.Itoa(i)))
			}
			b.Put([]byte("really-big-value"), []byte(strings.Repeat("*", 10000)))

			// Add a bucket that fits on a single root leaf.
			txn.CreateBucket("whozawhats")
			b = txn.Bucket("whozawhats")
			b.Put([]byte("foo"), []byte("bar"))

			return nil
		})
		db.With(func(txn *Transaction) error {
			b := txn.Bucket("widgets")
			stat := b.Stat()
			assert.Equal(t, stat.BranchPageCount, 15)
			assert.Equal(t, stat.LeafPageCount, 1281)
			assert.Equal(t, stat.OverflowPageCount, 0)
			assert.Equal(t, stat.KeyCount, 100000)
			assert.Equal(t, stat.MaxDepth, 3)

			b = txn.Bucket("woojits")
			stat = b.Stat()
			assert.Equal(t, stat.BranchPageCount, 1)
			assert.Equal(t, stat.LeafPageCount, 6)
			assert.Equal(t, stat.OverflowPageCount, 2)
			assert.Equal(t, stat.KeyCount, 501)
			assert.Equal(t, stat.MaxDepth, 2)

			b = txn.Bucket("whozawhats")
			stat = b.Stat()
			assert.Equal(t, stat.BranchPageCount, 0)
			assert.Equal(t, stat.LeafPageCount, 1)
			assert.Equal(t, stat.OverflowPageCount, 0)
			assert.Equal(t, stat.KeyCount, 1)
			assert.Equal(t, stat.MaxDepth, 1)

			return nil
		})
	})
}

// Ensure that a bucket can write random keys and values across multiple txns.
func TestBucketPutSingle(t *testing.T) {
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
func TestBucketPutMultiple(t *testing.T) {
	f := func(items testdata) bool {
		withOpenDB(func(db *DB, path string) {
			// Bulk insert all values.
			db.CreateBucket("widgets")
			rwtxn, _ := db.RWTransaction()
			b := rwtxn.Bucket("widgets")
			for _, item := range items {
				assert.NoError(t, b.Put(item.Key, item.Value))
			}
			assert.NoError(t, rwtxn.Commit())

			// Verify all items exist.
			txn, _ := db.Transaction()
			b = txn.Bucket("widgets")
			for _, item := range items {
				value := b.Get(item.Key)
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
func TestBucketDeleteQuick(t *testing.T) {
	f := func(items testdata) bool {
		withOpenDB(func(db *DB, path string) {
			// Bulk insert all values.
			db.CreateBucket("widgets")
			rwtxn, _ := db.RWTransaction()
			b := rwtxn.Bucket("widgets")
			for _, item := range items {
				assert.NoError(t, b.Put(item.Key, item.Value))
			}
			assert.NoError(t, rwtxn.Commit())

			// Remove items one at a time and check consistency.
			for i, item := range items {
				assert.NoError(t, db.Delete("widgets", item.Key))

				// Anything before our deletion index should be nil.
				txn, _ := db.Transaction()
				b := txn.Bucket("widgets")
				for j, exp := range items {
					if j > i {
						value := b.Get(exp.Key)
						if !assert.Equal(t, exp.Value, value) {
							t.FailNow()
						}
					} else {
						value := b.Get(exp.Key)
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
