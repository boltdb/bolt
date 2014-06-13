package bolt

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"
	"testing/quick"

	"github.com/stretchr/testify/assert"
)

// Ensure that a bucket that gets a non-existent key returns nil.
func TestBucket_Get_NonExistent(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.Update(func(tx *Tx) error {
			tx.CreateBucket([]byte("widgets"))
			value := tx.Bucket([]byte("widgets")).Get([]byte("foo"))
			assert.Nil(t, value)
			return nil
		})
	})
}

// Ensure that a bucket can read a value that is not flushed yet.
func TestBucket_Get_FromNode(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.Update(func(tx *Tx) error {
			tx.CreateBucket([]byte("widgets"))
			b := tx.Bucket([]byte("widgets"))
			b.Put([]byte("foo"), []byte("bar"))
			value := b.Get([]byte("foo"))
			assert.Equal(t, value, []byte("bar"))
			return nil
		})
	})
}

// Ensure that a bucket retrieved via Get() returns a nil.
func TestBucket_Get_IncompatibleValue(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.Update(func(tx *Tx) error {
			tx.CreateBucket([]byte("widgets"))
			_, err := tx.Bucket([]byte("widgets")).CreateBucket([]byte("foo"))
			assert.NoError(t, err)
			assert.Nil(t, tx.Bucket([]byte("widgets")).Get([]byte("foo")))
			return nil
		})
	})
}

// Ensure that a bucket can write a key/value.
func TestBucket_Put(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.Update(func(tx *Tx) error {
			tx.CreateBucket([]byte("widgets"))
			err := tx.Bucket([]byte("widgets")).Put([]byte("foo"), []byte("bar"))
			assert.NoError(t, err)
			value := tx.Bucket([]byte("widgets")).Get([]byte("foo"))
			assert.Equal(t, value, []byte("bar"))
			return nil
		})
	})
}

// Ensure that a bucket can rewrite a key in the same transaction.
func TestBucket_Put_Repeat(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.Update(func(tx *Tx) error {
			tx.CreateBucket([]byte("widgets"))
			b := tx.Bucket([]byte("widgets"))
			assert.NoError(t, b.Put([]byte("foo"), []byte("bar")))
			assert.NoError(t, b.Put([]byte("foo"), []byte("baz")))
			value := tx.Bucket([]byte("widgets")).Get([]byte("foo"))
			assert.Equal(t, value, []byte("baz"))
			return nil
		})
	})
}

// Ensure that a bucket can write a bunch of large values.
func TestBucket_Put_Large(t *testing.T) {
	var count = 100
	var factor = 200
	withOpenDB(func(db *DB, path string) {
		db.Update(func(tx *Tx) error {
			tx.CreateBucket([]byte("widgets"))
			b := tx.Bucket([]byte("widgets"))
			for i := 1; i < count; i++ {
				assert.NoError(t, b.Put([]byte(strings.Repeat("0", i*factor)), []byte(strings.Repeat("X", (count-i)*factor))))
			}
			return nil
		})
		db.View(func(tx *Tx) error {
			b := tx.Bucket([]byte("widgets"))
			for i := 1; i < count; i++ {
				value := b.Get([]byte(strings.Repeat("0", i*factor)))
				assert.Equal(t, []byte(strings.Repeat("X", (count-i)*factor)), value)
			}
			return nil
		})
	})
}

// Ensure that a setting a value on a key with a bucket value returns an error.
func TestBucket_Put_IncompatibleValue(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.Update(func(tx *Tx) error {
			tx.CreateBucket([]byte("widgets"))
			_, err := tx.Bucket([]byte("widgets")).CreateBucket([]byte("foo"))
			assert.NoError(t, err)
			assert.Equal(t, ErrIncompatibleValue, tx.Bucket([]byte("widgets")).Put([]byte("foo"), []byte("bar")))
			return nil
		})
	})
}

// Ensure that a setting a value while the transaction is closed returns an error.
func TestBucket_Put_Closed(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		tx, _ := db.Begin(true)
		tx.CreateBucket([]byte("widgets"))
		b := tx.Bucket([]byte("widgets"))
		tx.Rollback()
		assert.Equal(t, ErrTxClosed, b.Put([]byte("foo"), []byte("bar")))
	})
}

// Ensure that setting a value on a read-only bucket returns an error.
func TestBucket_Put_ReadOnly(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.Update(func(tx *Tx) error {
			_, err := tx.CreateBucket([]byte("widgets"))
			assert.NoError(t, err)
			return nil
		})
		db.View(func(tx *Tx) error {
			b := tx.Bucket([]byte("widgets"))
			err := b.Put([]byte("foo"), []byte("bar"))
			assert.Equal(t, err, ErrTxNotWritable)
			return nil
		})
	})
}

// Ensure that a bucket can delete an existing key.
func TestBucket_Delete(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.Update(func(tx *Tx) error {
			tx.CreateBucket([]byte("widgets"))
			tx.Bucket([]byte("widgets")).Put([]byte("foo"), []byte("bar"))
			err := tx.Bucket([]byte("widgets")).Delete([]byte("foo"))
			assert.NoError(t, err)
			value := tx.Bucket([]byte("widgets")).Get([]byte("foo"))
			assert.Nil(t, value)
			return nil
		})
	})
}

// Ensure that deleting a large set of keys will work correctly.
func TestBucket_Delete_Large(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.Update(func(tx *Tx) error {
			var b, _ = tx.CreateBucket([]byte("widgets"))
			for i := 0; i < 100; i++ {
				assert.NoError(t, b.Put([]byte(strconv.Itoa(i)), []byte(strings.Repeat("*", 1024))))
			}
			return nil
		})
		db.Update(func(tx *Tx) error {
			var b = tx.Bucket([]byte("widgets"))
			for i := 0; i < 100; i++ {
				assert.NoError(t, b.Delete([]byte(strconv.Itoa(i))))
			}
			return nil
		})
		db.View(func(tx *Tx) error {
			var b = tx.Bucket([]byte("widgets"))
			for i := 0; i < 100; i++ {
				assert.Nil(t, b.Get([]byte(strconv.Itoa(i))))
			}
			return nil
		})
	})
}

// Deleting a very large list of keys will overflow the freelist.
// https://github.com/boltdb/bolt/issues/192
func TestBucket_Delete_ErrFreelistOverflow(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	withOpenDB(func(db *DB, path string) {
		k := make([]byte, 16)
		for i := uint64(0); i < 10000; i++ {
			err := db.Update(func(tx *Tx) error {
				b, err := tx.CreateBucketIfNotExists([]byte("0"))
				if err != nil {
					t.Fatalf("bucket error: %s", err)
				}

				for j := uint64(0); j < 1000; j++ {
					binary.BigEndian.PutUint64(k[:8], i)
					binary.BigEndian.PutUint64(k[8:], j)
					if err := b.Put(k, nil); err != nil {
						t.Fatalf("put error: %s", err)
					}
				}

				return nil
			})

			if err != nil {
				t.Fatalf("update error: %s", err)
			}
		}

		// Delete all of them in one large transaction
		err := db.Update(func(tx *Tx) error {
			b := tx.Bucket([]byte("0"))
			c := b.Cursor()
			for k, _ := c.First(); k != nil; k, _ = c.Next() {
				b.Delete(k)
			}
			return nil
		})

		// Check that a freelist overflow occurred.
		assert.Equal(t, ErrFreelistOverflow, err)
	})
}

// Ensure that accessing and updating nested buckets is ok across transactions.
func TestBucket_Nested(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.Update(func(tx *Tx) error {
			// Create a widgets bucket.
			b, err := tx.CreateBucket([]byte("widgets"))
			assert.NoError(t, err)

			// Create a widgets/foo bucket.
			_, err = b.CreateBucket([]byte("foo"))
			assert.NoError(t, err)

			// Create a widgets/bar key.
			assert.NoError(t, b.Put([]byte("bar"), []byte("0000")))

			return nil
		})
		mustCheck(db)

		// Update widgets/bar.
		db.Update(func(tx *Tx) error {
			var b = tx.Bucket([]byte("widgets"))
			assert.NoError(t, b.Put([]byte("bar"), []byte("xxxx")))
			return nil
		})
		mustCheck(db)

		// Cause a split.
		db.Update(func(tx *Tx) error {
			var b = tx.Bucket([]byte("widgets"))
			for i := 0; i < 10000; i++ {
				assert.NoError(t, b.Put([]byte(strconv.Itoa(i)), []byte(strconv.Itoa(i))))
			}
			return nil
		})
		mustCheck(db)

		// Insert into widgets/foo/baz.
		db.Update(func(tx *Tx) error {
			var b = tx.Bucket([]byte("widgets"))
			assert.NoError(t, b.Bucket([]byte("foo")).Put([]byte("baz"), []byte("yyyy")))
			return nil
		})
		mustCheck(db)

		// Verify.
		db.View(func(tx *Tx) error {
			var b = tx.Bucket([]byte("widgets"))
			assert.Equal(t, []byte("yyyy"), b.Bucket([]byte("foo")).Get([]byte("baz")))
			assert.Equal(t, []byte("xxxx"), b.Get([]byte("bar")))
			for i := 0; i < 10000; i++ {
				assert.Equal(t, []byte(strconv.Itoa(i)), b.Get([]byte(strconv.Itoa(i))))
			}
			return nil
		})
	})
}

// Ensure that deleting a bucket using Delete() returns an error.
func TestBucket_Delete_Bucket(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.Update(func(tx *Tx) error {
			tx.CreateBucket([]byte("widgets"))
			b := tx.Bucket([]byte("widgets"))
			_, err := b.CreateBucket([]byte("foo"))
			assert.NoError(t, err)
			assert.Equal(t, ErrIncompatibleValue, b.Delete([]byte("foo")))
			return nil
		})
	})
}

// Ensure that deleting a key on a read-only bucket returns an error.
func TestBucket_Delete_ReadOnly(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.Update(func(tx *Tx) error {
			tx.CreateBucket([]byte("widgets"))
			return nil
		})
		db.View(func(tx *Tx) error {
			b := tx.Bucket([]byte("widgets"))
			err := b.Delete([]byte("foo"))
			assert.Equal(t, err, ErrTxNotWritable)
			return nil
		})
	})
}

// Ensure that a deleting value while the transaction is closed returns an error.
func TestBucket_Delete_Closed(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		tx, _ := db.Begin(true)
		tx.CreateBucket([]byte("widgets"))
		b := tx.Bucket([]byte("widgets"))
		tx.Rollback()
		assert.Equal(t, ErrTxClosed, b.Delete([]byte("foo")))
	})
}

// Ensure that deleting a bucket causes nested buckets to be deleted.
func TestBucket_DeleteBucket_Nested(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.Update(func(tx *Tx) error {
			tx.CreateBucket([]byte("widgets"))
			_, err := tx.Bucket([]byte("widgets")).CreateBucket([]byte("foo"))
			assert.NoError(t, err)
			_, err = tx.Bucket([]byte("widgets")).Bucket([]byte("foo")).CreateBucket([]byte("bar"))
			assert.NoError(t, err)
			assert.NoError(t, tx.Bucket([]byte("widgets")).Bucket([]byte("foo")).Bucket([]byte("bar")).Put([]byte("baz"), []byte("bat")))
			assert.NoError(t, tx.Bucket([]byte("widgets")).DeleteBucket([]byte("foo")))
			return nil
		})
	})
}

// Ensure that deleting a bucket causes nested buckets to be deleted after they have been committed.
func TestBucket_DeleteBucket_Nested2(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.Update(func(tx *Tx) error {
			tx.CreateBucket([]byte("widgets"))
			_, err := tx.Bucket([]byte("widgets")).CreateBucket([]byte("foo"))
			assert.NoError(t, err)
			_, err = tx.Bucket([]byte("widgets")).Bucket([]byte("foo")).CreateBucket([]byte("bar"))
			assert.NoError(t, err)
			assert.NoError(t, tx.Bucket([]byte("widgets")).Bucket([]byte("foo")).Bucket([]byte("bar")).Put([]byte("baz"), []byte("bat")))
			return nil
		})
		db.Update(func(tx *Tx) error {
			assert.NotNil(t, tx.Bucket([]byte("widgets")))
			assert.NotNil(t, tx.Bucket([]byte("widgets")).Bucket([]byte("foo")))
			assert.NotNil(t, tx.Bucket([]byte("widgets")).Bucket([]byte("foo")).Bucket([]byte("bar")))
			assert.Equal(t, []byte("bat"), tx.Bucket([]byte("widgets")).Bucket([]byte("foo")).Bucket([]byte("bar")).Get([]byte("baz")))
			assert.NoError(t, tx.DeleteBucket([]byte("widgets")))
			return nil
		})
		db.View(func(tx *Tx) error {
			assert.Nil(t, tx.Bucket([]byte("widgets")))
			return nil
		})
	})
}

// Ensure that deleting a child bucket with multiple pages causes all pages to get collected.
func TestBucket_DeleteBucket_Large(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.Update(func(tx *Tx) error {
			_, err := tx.CreateBucket([]byte("widgets"))
			assert.NoError(t, err)
			_, err = tx.Bucket([]byte("widgets")).CreateBucket([]byte("foo"))
			assert.NoError(t, err)
			b := tx.Bucket([]byte("widgets")).Bucket([]byte("foo"))
			for i := 0; i < 1000; i++ {
				assert.NoError(t, b.Put([]byte(fmt.Sprintf("%d", i)), []byte(fmt.Sprintf("%0100d", i))))
			}
			return nil
		})
		db.Update(func(tx *Tx) error {
			assert.NoError(t, tx.DeleteBucket([]byte("widgets")))
			return nil
		})

		// NOTE: Consistency check in withOpenDB() will error if pages not freed properly.
	})
}

// Ensure that a simple value retrieved via Bucket() returns a nil.
func TestBucket_Bucket_IncompatibleValue(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.Update(func(tx *Tx) error {
			tx.CreateBucket([]byte("widgets"))
			assert.NoError(t, tx.Bucket([]byte("widgets")).Put([]byte("foo"), []byte("bar")))
			assert.Nil(t, tx.Bucket([]byte("widgets")).Bucket([]byte("foo")))
			return nil
		})
	})
}

// Ensure that creating a bucket on an existing non-bucket key returns an error.
func TestBucket_CreateBucket_IncompatibleValue(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.Update(func(tx *Tx) error {
			_, err := tx.CreateBucket([]byte("widgets"))
			assert.NoError(t, err)
			assert.NoError(t, tx.Bucket([]byte("widgets")).Put([]byte("foo"), []byte("bar")))
			_, err = tx.Bucket([]byte("widgets")).CreateBucket([]byte("foo"))
			assert.Equal(t, ErrIncompatibleValue, err)
			return nil
		})
	})
}

// Ensure that deleting a bucket on an existing non-bucket key returns an error.
func TestBucket_DeleteBucket_IncompatibleValue(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.Update(func(tx *Tx) error {
			_, err := tx.CreateBucket([]byte("widgets"))
			assert.NoError(t, err)
			assert.NoError(t, tx.Bucket([]byte("widgets")).Put([]byte("foo"), []byte("bar")))
			assert.Equal(t, ErrIncompatibleValue, tx.Bucket([]byte("widgets")).DeleteBucket([]byte("foo")))
			return nil
		})
	})
}

// Ensure that a bucket can return an autoincrementing sequence.
func TestBucket_NextSequence(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.Update(func(tx *Tx) error {
			tx.CreateBucket([]byte("widgets"))
			tx.CreateBucket([]byte("woojits"))

			// Make sure sequence increments.
			seq, err := tx.Bucket([]byte("widgets")).NextSequence()
			assert.NoError(t, err)
			assert.Equal(t, seq, 1)
			seq, err = tx.Bucket([]byte("widgets")).NextSequence()
			assert.NoError(t, err)
			assert.Equal(t, seq, 2)

			// Buckets should be separate.
			seq, err = tx.Bucket([]byte("woojits")).NextSequence()
			assert.NoError(t, err)
			assert.Equal(t, seq, 1)
			return nil
		})
	})
}

// Ensure that retrieving the next sequence on a read-only bucket returns an error.
func TestBucket_NextSequence_ReadOnly(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.Update(func(tx *Tx) error {
			tx.CreateBucket([]byte("widgets"))
			return nil
		})
		db.View(func(tx *Tx) error {
			b := tx.Bucket([]byte("widgets"))
			i, err := b.NextSequence()
			assert.Equal(t, i, 0)
			assert.Equal(t, err, ErrTxNotWritable)
			return nil
		})
	})
}

// Ensure that incrementing past the maximum sequence number will return an error.
func TestBucket_NextSequence_Overflow(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.Update(func(tx *Tx) error {
			tx.CreateBucket([]byte("widgets"))
			return nil
		})
		db.Update(func(tx *Tx) error {
			b := tx.Bucket([]byte("widgets"))
			b.bucket.sequence = uint64(maxInt)
			seq, err := b.NextSequence()
			assert.Equal(t, err, ErrSequenceOverflow)
			assert.Equal(t, seq, 0)
			return nil
		})
	})
}

// Ensure that retrieving the next sequence for a bucket on a closed database return an error.
func TestBucket_NextSequence_Closed(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		tx, _ := db.Begin(true)
		tx.CreateBucket([]byte("widgets"))
		b := tx.Bucket([]byte("widgets"))
		tx.Rollback()
		_, err := b.NextSequence()
		assert.Equal(t, ErrTxClosed, err)
	})
}

// Ensure a user can loop over all key/value pairs in a bucket.
func TestBucket_ForEach(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.Update(func(tx *Tx) error {
			tx.CreateBucket([]byte("widgets"))
			tx.Bucket([]byte("widgets")).Put([]byte("foo"), []byte("0000"))
			tx.Bucket([]byte("widgets")).Put([]byte("baz"), []byte("0001"))
			tx.Bucket([]byte("widgets")).Put([]byte("bar"), []byte("0002"))

			var index int
			err := tx.Bucket([]byte("widgets")).ForEach(func(k, v []byte) error {
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
			return nil
		})
	})
}

// Ensure a database can stop iteration early.
func TestBucket_ForEach_ShortCircuit(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.Update(func(tx *Tx) error {
			tx.CreateBucket([]byte("widgets"))
			tx.Bucket([]byte("widgets")).Put([]byte("bar"), []byte("0000"))
			tx.Bucket([]byte("widgets")).Put([]byte("baz"), []byte("0000"))
			tx.Bucket([]byte("widgets")).Put([]byte("foo"), []byte("0000"))

			var index int
			err := tx.Bucket([]byte("widgets")).ForEach(func(k, v []byte) error {
				index++
				if bytes.Equal(k, []byte("baz")) {
					return errors.New("marker")
				}
				return nil
			})
			assert.Equal(t, errors.New("marker"), err)
			assert.Equal(t, 2, index)
			return nil
		})
	})
}

// Ensure that looping over a bucket on a closed database returns an error.
func TestBucket_ForEach_Closed(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		tx, _ := db.Begin(true)
		tx.CreateBucket([]byte("widgets"))
		b := tx.Bucket([]byte("widgets"))
		tx.Rollback()
		err := b.ForEach(func(k, v []byte) error { return nil })
		assert.Equal(t, ErrTxClosed, err)
	})
}

// Ensure that an error is returned when inserting with an empty key.
func TestBucket_Put_EmptyKey(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.Update(func(tx *Tx) error {
			tx.CreateBucket([]byte("widgets"))
			err := tx.Bucket([]byte("widgets")).Put([]byte(""), []byte("bar"))
			assert.Equal(t, err, ErrKeyRequired)
			err = tx.Bucket([]byte("widgets")).Put(nil, []byte("bar"))
			assert.Equal(t, err, ErrKeyRequired)
			return nil
		})
	})
}

// Ensure that an error is returned when inserting with a key that's too large.
func TestBucket_Put_KeyTooLarge(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.Update(func(tx *Tx) error {
			tx.CreateBucket([]byte("widgets"))
			err := tx.Bucket([]byte("widgets")).Put(make([]byte, 32769), []byte("bar"))
			assert.Equal(t, err, ErrKeyTooLarge)
			return nil
		})
	})
}

// Ensure a bucket can calculate stats.
func TestBucket_Stats(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		// Add bucket with fewer keys but one big value.
		big_key := []byte("really-big-value")
		for i := 0; i < 500; i++ {
			db.Update(func(tx *Tx) error {
				b, _ := tx.CreateBucketIfNotExists([]byte("woojits"))
				return b.Put([]byte(fmt.Sprintf("%03d", i)), []byte(strconv.Itoa(i)))
			})
		}
		db.Update(func(tx *Tx) error {
			b, _ := tx.CreateBucketIfNotExists([]byte("woojits"))
			return b.Put(big_key, []byte(strings.Repeat("*", 10000)))
		})

		mustCheck(db)
		db.View(func(tx *Tx) error {
			b := tx.Bucket([]byte("woojits"))
			stats := b.Stats()
			assert.Equal(t, 1, stats.BranchPageN, "BranchPageN")
			assert.Equal(t, 0, stats.BranchOverflowN, "BranchOverflowN")
			assert.Equal(t, 7, stats.LeafPageN, "LeafPageN")
			assert.Equal(t, 2, stats.LeafOverflowN, "LeafOverflowN")
			assert.Equal(t, 501, stats.KeyN, "KeyN")
			assert.Equal(t, 2, stats.Depth, "Depth")

			branchInuse := pageHeaderSize            // branch page header
			branchInuse += 7 * branchPageElementSize // branch elements
			branchInuse += 7 * 3                     // branch keys (6 3-byte keys)
			assert.Equal(t, branchInuse, stats.BranchInuse, "BranchInuse")

			leafInuse := 7 * pageHeaderSize          // leaf page header
			leafInuse += 501 * leafPageElementSize   // leaf elements
			leafInuse += 500*3 + len(big_key)        // leaf keys
			leafInuse += 1*10 + 2*90 + 3*400 + 10000 // leaf values
			assert.Equal(t, leafInuse, stats.LeafInuse, "LeafInuse")

			if os.Getpagesize() == 4096 {
				// Incompatible page size
				assert.Equal(t, 4096, stats.BranchAlloc, "BranchAlloc")
				assert.Equal(t, 36864, stats.LeafAlloc, "LeafAlloc")
			}

			assert.Equal(t, 1, stats.BucketN, "BucketN")
			assert.Equal(t, 0, stats.InlineBucketN, "InlineBucketN")
			assert.Equal(t, 0, stats.InlineBucketInuse, "InlineBucketInuse")
			return nil
		})
	})
}

// Ensure a bucket with random insertion utilizes fill percentage correctly.
func TestBucket_Stats_RandomFill(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	if os.Getpagesize() != 4096 {
		t.Skip("invalid page size for test")
	}

	withOpenDB(func(db *DB, path string) {
		db.FillPercent = 0.9

		// Add a set of values in random order. It will be the same random
		// order so we can maintain consistency between test runs.
		var count int
		r := rand.New(rand.NewSource(42))
		for _, i := range r.Perm(1000) {
			db.Update(func(tx *Tx) error {
				b, _ := tx.CreateBucketIfNotExists([]byte("woojits"))
				for _, j := range r.Perm(100) {
					index := (j * 10000) + i
					b.Put([]byte(fmt.Sprintf("%d000000000000000", index)), []byte("0000000000"))
					count++
				}
				return nil
			})
		}
		mustCheck(db)

		db.View(func(tx *Tx) error {
			s := tx.Bucket([]byte("woojits")).Stats()
			assert.Equal(t, 100000, s.KeyN, "KeyN")

			assert.Equal(t, 22, s.BranchPageN, "BranchPageN")
			assert.Equal(t, 0, s.BranchOverflowN, "BranchOverflowN")
			assert.Equal(t, 61708, s.BranchInuse, "BranchInuse")
			assert.Equal(t, 90112, s.BranchAlloc, "BranchAlloc")

			assert.Equal(t, 1643, s.LeafPageN, "LeafPageN")
			assert.Equal(t, 0, s.LeafOverflowN, "LeafOverflowN")
			assert.Equal(t, 4714178, s.LeafInuse, "LeafInuse")
			assert.Equal(t, 6729728, s.LeafAlloc, "LeafAlloc")
			return nil
		})
	})
}

// Ensure a bucket can calculate stats.
func TestBucket_Stats_Small(t *testing.T) {

	withOpenDB(func(db *DB, path string) {
		db.Update(func(tx *Tx) error {
			// Add a bucket that fits on a single root leaf.
			b, err := tx.CreateBucket([]byte("whozawhats"))
			assert.NoError(t, err)
			b.Put([]byte("foo"), []byte("bar"))

			return nil
		})
		mustCheck(db)
		db.View(func(tx *Tx) error {
			b := tx.Bucket([]byte("whozawhats"))
			stats := b.Stats()
			assert.Equal(t, 0, stats.BranchPageN, "BranchPageN")
			assert.Equal(t, 0, stats.BranchOverflowN, "BranchOverflowN")
			assert.Equal(t, 0, stats.LeafPageN, "LeafPageN")
			assert.Equal(t, 0, stats.LeafOverflowN, "LeafOverflowN")
			assert.Equal(t, 1, stats.KeyN, "KeyN")
			assert.Equal(t, 1, stats.Depth, "Depth")
			assert.Equal(t, 0, stats.BranchInuse, "BranchInuse")
			assert.Equal(t, 0, stats.LeafInuse, "LeafInuse")
			if os.Getpagesize() == 4096 {
				// Incompatible page size
				assert.Equal(t, 0, stats.BranchAlloc, "BranchAlloc")
				assert.Equal(t, 0, stats.LeafAlloc, "LeafAlloc")
			}
			assert.Equal(t, 1, stats.BucketN, "BucketN")
			assert.Equal(t, 1, stats.InlineBucketN, "InlineBucketN")
			assert.Equal(t, pageHeaderSize+leafPageElementSize+6, stats.InlineBucketInuse, "InlineBucketInuse")
			return nil
		})
	})
}

func TestBucket_Stats_EmptyBucket(t *testing.T) {

	withOpenDB(func(db *DB, path string) {
		db.Update(func(tx *Tx) error {
			// Add a bucket that fits on a single root leaf.
			_, err := tx.CreateBucket([]byte("whozawhats"))
			assert.NoError(t, err)
			return nil
		})
		mustCheck(db)
		db.View(func(tx *Tx) error {
			b := tx.Bucket([]byte("whozawhats"))
			stats := b.Stats()
			assert.Equal(t, 0, stats.BranchPageN, "BranchPageN")
			assert.Equal(t, 0, stats.BranchOverflowN, "BranchOverflowN")
			assert.Equal(t, 0, stats.LeafPageN, "LeafPageN")
			assert.Equal(t, 0, stats.LeafOverflowN, "LeafOverflowN")
			assert.Equal(t, 0, stats.KeyN, "KeyN")
			assert.Equal(t, 1, stats.Depth, "Depth")
			assert.Equal(t, 0, stats.BranchInuse, "BranchInuse")
			assert.Equal(t, 0, stats.LeafInuse, "LeafInuse")
			if os.Getpagesize() == 4096 {
				// Incompatible page size
				assert.Equal(t, 0, stats.BranchAlloc, "BranchAlloc")
				assert.Equal(t, 0, stats.LeafAlloc, "LeafAlloc")
			}
			assert.Equal(t, 1, stats.BucketN, "BucketN")
			assert.Equal(t, 1, stats.InlineBucketN, "InlineBucketN")
			assert.Equal(t, pageHeaderSize, stats.InlineBucketInuse, "InlineBucketInuse")
			return nil
		})
	})
}

// Ensure a bucket can calculate stats.
func TestBucket_Stats_Nested(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.Update(func(tx *Tx) error {
			b, err := tx.CreateBucket([]byte("foo"))
			assert.NoError(t, err)
			for i := 0; i < 100; i++ {
				b.Put([]byte(fmt.Sprintf("%02d", i)), []byte(fmt.Sprintf("%02d", i)))
			}
			bar, err := b.CreateBucket([]byte("bar"))
			assert.NoError(t, err)
			for i := 0; i < 10; i++ {
				bar.Put([]byte(strconv.Itoa(i)), []byte(strconv.Itoa(i)))
			}
			baz, err := bar.CreateBucket([]byte("baz"))
			assert.NoError(t, err)
			for i := 0; i < 10; i++ {
				baz.Put([]byte(strconv.Itoa(i)), []byte(strconv.Itoa(i)))
			}
			return nil
		})

		mustCheck(db)

		db.View(func(tx *Tx) error {
			b := tx.Bucket([]byte("foo"))
			stats := b.Stats()
			assert.Equal(t, 0, stats.BranchPageN, "BranchPageN")
			assert.Equal(t, 0, stats.BranchOverflowN, "BranchOverflowN")
			assert.Equal(t, 2, stats.LeafPageN, "LeafPageN")
			assert.Equal(t, 0, stats.LeafOverflowN, "LeafOverflowN")
			assert.Equal(t, 122, stats.KeyN, "KeyN")
			assert.Equal(t, 3, stats.Depth, "Depth")
			assert.Equal(t, 0, stats.BranchInuse, "BranchInuse")

			foo := pageHeaderSize            // foo
			foo += 101 * leafPageElementSize // foo leaf elements
			foo += 100*2 + 100*2             // foo leaf key/values
			foo += 3 + bucketHeaderSize      // foo -> bar key/value

			bar := pageHeaderSize           // bar
			bar += 11 * leafPageElementSize // bar leaf elements
			bar += 10 + 10                  // bar leaf key/values
			bar += 3 + bucketHeaderSize     // bar -> baz key/value

			baz := pageHeaderSize           // baz (inline)
			baz += 10 * leafPageElementSize // baz leaf elements
			baz += 10 + 10                  // baz leaf key/values

			assert.Equal(t, foo+bar+baz, stats.LeafInuse, "LeafInuse")
			if os.Getpagesize() == 4096 {
				// Incompatible page size
				assert.Equal(t, 0, stats.BranchAlloc, "BranchAlloc")
				assert.Equal(t, 8192, stats.LeafAlloc, "LeafAlloc")
			}
			assert.Equal(t, 3, stats.BucketN, "BucketN")
			assert.Equal(t, 1, stats.InlineBucketN, "InlineBucketN")
			assert.Equal(t, baz, stats.InlineBucketInuse, "InlineBucketInuse")
			return nil
		})
	})
}

// Ensure a large bucket can calculate stats.
func TestBucket_Stats_Large(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	withOpenDB(func(db *DB, path string) {
		var index int
		for i := 0; i < 10000; i++ {
			db.Update(func(tx *Tx) error {
				// Add bucket with lots of keys.
				b, _ := tx.CreateBucketIfNotExists([]byte("widgets"))
				for i := 0; i < 10; i++ {
					b.Put([]byte(strconv.Itoa(index)), []byte(strconv.Itoa(index)))
					index++
				}
				return nil
			})
		}
		mustCheck(db)

		db.View(func(tx *Tx) error {
			b := tx.Bucket([]byte("widgets"))
			stats := b.Stats()
			assert.Equal(t, 13, stats.BranchPageN, "BranchPageN")
			assert.Equal(t, 0, stats.BranchOverflowN, "BranchOverflowN")
			assert.Equal(t, 1195, stats.LeafPageN, "LeafPageN")
			assert.Equal(t, 0, stats.LeafOverflowN, "LeafOverflowN")
			assert.Equal(t, 100000, stats.KeyN, "KeyN")
			assert.Equal(t, 3, stats.Depth, "Depth")
			assert.Equal(t, 25208, stats.BranchInuse, "BranchInuse")
			assert.Equal(t, 2596900, stats.LeafInuse, "LeafInuse")
			if os.Getpagesize() == 4096 {
				// Incompatible page size
				assert.Equal(t, 53248, stats.BranchAlloc, "BranchAlloc")
				assert.Equal(t, 4894720, stats.LeafAlloc, "LeafAlloc")
			}
			assert.Equal(t, 1, stats.BucketN, "BucketN")
			assert.Equal(t, 0, stats.InlineBucketN, "InlineBucketN")
			assert.Equal(t, 0, stats.InlineBucketInuse, "InlineBucketInuse")
			return nil
		})
	})
}

// Ensure that a bucket can write random keys and values across multiple transactions.
func TestBucket_Put_Single(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	index := 0
	f := func(items testdata) bool {
		withOpenDB(func(db *DB, path string) {
			m := make(map[string][]byte)

			db.Update(func(tx *Tx) error {
				_, err := tx.CreateBucket([]byte("widgets"))
				return err
			})
			for _, item := range items {
				db.Update(func(tx *Tx) error {
					if err := tx.Bucket([]byte("widgets")).Put(item.Key, item.Value); err != nil {
						panic("put error: " + err.Error())
					}
					m[string(item.Key)] = item.Value
					return nil
				})

				// Verify all key/values so far.
				db.View(func(tx *Tx) error {
					i := 0
					for k, v := range m {
						value := tx.Bucket([]byte("widgets")).Get([]byte(k))
						if !bytes.Equal(value, v) {
							t.Logf("value mismatch [run %d] (%d of %d):\nkey: %x\ngot: %x\nexp: %x", index, i, len(m), []byte(k), value, v)
							copyAndFailNow(t, db)
						}
						i++
					}
					return nil
				})
			}
		})

		index++
		return true
	}
	if err := quick.Check(f, qconfig()); err != nil {
		t.Error(err)
	}
}

// Ensure that a transaction can insert multiple key/value pairs at once.
func TestBucket_Put_Multiple(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	f := func(items testdata) bool {
		withOpenDB(func(db *DB, path string) {
			// Bulk insert all values.
			db.Update(func(tx *Tx) error {
				_, err := tx.CreateBucket([]byte("widgets"))
				return err
			})
			err := db.Update(func(tx *Tx) error {
				b := tx.Bucket([]byte("widgets"))
				for _, item := range items {
					assert.NoError(t, b.Put(item.Key, item.Value))
				}
				return nil
			})
			assert.NoError(t, err)

			// Verify all items exist.
			db.View(func(tx *Tx) error {
				b := tx.Bucket([]byte("widgets"))
				for _, item := range items {
					value := b.Get(item.Key)
					if !assert.Equal(t, item.Value, value) {
						copyAndFailNow(t, db)
					}
				}
				return nil
			})
		})
		return true
	}
	if err := quick.Check(f, qconfig()); err != nil {
		t.Error(err)
	}
}

// Ensure that a transaction can delete all key/value pairs and return to a single leaf page.
func TestBucket_Delete_Quick(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	f := func(items testdata) bool {
		withOpenDB(func(db *DB, path string) {
			// Bulk insert all values.
			db.Update(func(tx *Tx) error {
				_, err := tx.CreateBucket([]byte("widgets"))
				return err
			})
			err := db.Update(func(tx *Tx) error {
				b := tx.Bucket([]byte("widgets"))
				for _, item := range items {
					assert.NoError(t, b.Put(item.Key, item.Value))
				}
				return nil
			})
			assert.NoError(t, err)

			// Remove items one at a time and check consistency.
			for _, item := range items {
				err := db.Update(func(tx *Tx) error {
					return tx.Bucket([]byte("widgets")).Delete(item.Key)
				})
				assert.NoError(t, err)
			}

			// Anything before our deletion index should be nil.
			db.View(func(tx *Tx) error {
				tx.Bucket([]byte("widgets")).ForEach(func(k, v []byte) error {
					t.Fatalf("bucket should be empty; found: %06x", trunc(k, 3))
					return nil
				})
				return nil
			})
		})
		return true
	}
	if err := quick.Check(f, qconfig()); err != nil {
		t.Error(err)
	}
}

func ExampleBucket_Put() {
	// Open the database.
	db, _ := Open(tempfile(), 0666)
	defer os.Remove(db.Path())
	defer db.Close()

	// Start a write transaction.
	db.Update(func(tx *Tx) error {
		// Create a bucket.
		tx.CreateBucket([]byte("widgets"))

		// Set the value "bar" for the key "foo".
		tx.Bucket([]byte("widgets")).Put([]byte("foo"), []byte("bar"))
		return nil
	})

	// Read value back in a different read-only transaction.
	db.View(func(tx *Tx) error {
		value := tx.Bucket([]byte("widgets")).Get([]byte("foo"))
		fmt.Printf("The value of 'foo' is: %s\n", value)
		return nil
	})

	// Output:
	// The value of 'foo' is: bar
}

func ExampleBucket_Delete() {
	// Open the database.
	db, _ := Open(tempfile(), 0666)
	defer os.Remove(db.Path())
	defer db.Close()

	// Start a write transaction.
	db.Update(func(tx *Tx) error {
		// Create a bucket.
		tx.CreateBucket([]byte("widgets"))
		b := tx.Bucket([]byte("widgets"))

		// Set the value "bar" for the key "foo".
		b.Put([]byte("foo"), []byte("bar"))

		// Retrieve the key back from the database and verify it.
		value := b.Get([]byte("foo"))
		fmt.Printf("The value of 'foo' was: %s\n", value)
		return nil
	})

	// Delete the key in a different write transaction.
	db.Update(func(tx *Tx) error {
		return tx.Bucket([]byte("widgets")).Delete([]byte("foo"))
	})

	// Retrieve the key again.
	db.View(func(tx *Tx) error {
		value := tx.Bucket([]byte("widgets")).Get([]byte("foo"))
		if value == nil {
			fmt.Printf("The value of 'foo' is now: nil\n")
		}
		return nil
	})

	// Output:
	// The value of 'foo' was: bar
	// The value of 'foo' is now: nil
}

func ExampleBucket_ForEach() {
	// Open the database.
	db, _ := Open(tempfile(), 0666)
	defer os.Remove(db.Path())
	defer db.Close()

	// Insert data into a bucket.
	db.Update(func(tx *Tx) error {
		tx.CreateBucket([]byte("animals"))
		b := tx.Bucket([]byte("animals"))
		b.Put([]byte("dog"), []byte("fun"))
		b.Put([]byte("cat"), []byte("lame"))
		b.Put([]byte("liger"), []byte("awesome"))

		// Iterate over items in sorted key order.
		b.ForEach(func(k, v []byte) error {
			fmt.Printf("A %s is %s.\n", k, v)
			return nil
		})
		return nil
	})

	// Output:
	// A cat is lame.
	// A dog is fun.
	// A liger is awesome.
}
