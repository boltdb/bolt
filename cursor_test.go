package bolt

import (
	"sort"
	"testing"
	"testing/quick"

	"github.com/stretchr/testify/assert"
)

// Ensure that a cursor can return a reference to the bucket that created it.
func TestCursor_Bucket(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.Update(func(tx *Tx) error {
			b, _ := tx.CreateBucket([]byte("widgets"))
			c := b.Cursor()
			assert.Equal(t, b, c.Bucket())
			return nil
		})
	})
}

// Ensure that a Tx cursor can seek to the appropriate keys.
func TestCursor_Seek(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.Update(func(tx *Tx) error {
			b, err := tx.CreateBucket([]byte("widgets"))
			assert.NoError(t, err)
			assert.NoError(t, b.Put([]byte("foo"), []byte("0001")))
			assert.NoError(t, b.Put([]byte("bar"), []byte("0002")))
			assert.NoError(t, b.Put([]byte("baz"), []byte("0003")))
			_, err = b.CreateBucket([]byte("bkt"))
			assert.NoError(t, err)
			return nil
		})
		db.View(func(tx *Tx) error {
			c := tx.Bucket([]byte("widgets")).Cursor()

			// Exact match should go to the key.
			k, v := c.Seek([]byte("bar"))
			assert.Equal(t, []byte("bar"), k)
			assert.Equal(t, []byte("0002"), v)

			// Inexact match should go to the next key.
			k, v = c.Seek([]byte("bas"))
			assert.Equal(t, []byte("baz"), k)
			assert.Equal(t, []byte("0003"), v)

			// Low key should go to the first key.
			k, v = c.Seek([]byte(""))
			assert.Equal(t, []byte("bar"), k)
			assert.Equal(t, []byte("0002"), v)

			// High key should return no key.
			k, v = c.Seek([]byte("zzz"))
			assert.Nil(t, k)
			assert.Nil(t, v)

			// Buckets should return their key but no value.
			k, v = c.Seek([]byte("bkt"))
			assert.Equal(t, []byte("bkt"), k)
			assert.Nil(t, v)

			return nil
		})
	})
}

// Ensure that a cursor can iterate over an empty bucket without error.
func TestCursor_EmptyBucket(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.Update(func(tx *Tx) error {
			_, err := tx.CreateBucket([]byte("widgets"))
			return err
		})
		db.View(func(tx *Tx) error {
			c := tx.Bucket([]byte("widgets")).Cursor()
			k, v := c.First()
			assert.Nil(t, k)
			assert.Nil(t, v)
			return nil
		})
	})
}

// Ensure that a Tx cursor can reverse iterate over an empty bucket without error.
func TestCursor_EmptyBucketReverse(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.Update(func(tx *Tx) error {
			_, err := tx.CreateBucket([]byte("widgets"))
			return err
		})
		db.View(func(tx *Tx) error {
			c := tx.Bucket([]byte("widgets")).Cursor()
			k, v := c.Last()
			assert.Nil(t, k)
			assert.Nil(t, v)
			return nil
		})
	})
}

// Ensure that a Tx cursor can iterate over a single root with a couple elements.
func TestCursor_LeafRoot(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.Update(func(tx *Tx) error {
			tx.CreateBucket([]byte("widgets"))
			tx.Bucket([]byte("widgets")).Put([]byte("baz"), []byte{})
			tx.Bucket([]byte("widgets")).Put([]byte("foo"), []byte{0})
			tx.Bucket([]byte("widgets")).Put([]byte("bar"), []byte{1})
			return nil
		})
		tx, _ := db.Begin(false)
		c := tx.Bucket([]byte("widgets")).Cursor()

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
func TestCursor_LeafRootReverse(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.Update(func(tx *Tx) error {
			tx.CreateBucket([]byte("widgets"))
			tx.Bucket([]byte("widgets")).Put([]byte("baz"), []byte{})
			tx.Bucket([]byte("widgets")).Put([]byte("foo"), []byte{0})
			tx.Bucket([]byte("widgets")).Put([]byte("bar"), []byte{1})
			return nil
		})
		tx, _ := db.Begin(false)
		c := tx.Bucket([]byte("widgets")).Cursor()

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
func TestCursor_Restart(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.Update(func(tx *Tx) error {
			tx.CreateBucket([]byte("widgets"))
			tx.Bucket([]byte("widgets")).Put([]byte("bar"), []byte{})
			tx.Bucket([]byte("widgets")).Put([]byte("foo"), []byte{})
			return nil
		})

		tx, _ := db.Begin(false)
		c := tx.Bucket([]byte("widgets")).Cursor()

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
func TestCursor_Iterate(t *testing.T) {
	f := func(items testdata) bool {
		withOpenDB(func(db *DB, path string) {
			// Bulk insert all values.
			tx, _ := db.Begin(true)
			tx.CreateBucket([]byte("widgets"))
			b := tx.Bucket([]byte("widgets"))
			for _, item := range items {
				assert.NoError(t, b.Put(item.Key, item.Value))
			}
			assert.NoError(t, tx.Commit())

			// Sort test data.
			sort.Sort(items)

			// Iterate over all items and check consistency.
			var index = 0
			tx, _ = db.Begin(false)
			c := tx.Bucket([]byte("widgets")).Cursor()
			for k, v := c.First(); k != nil && index < len(items); k, v = c.Next() {
				assert.Equal(t, k, items[index].Key)
				assert.Equal(t, v, items[index].Value)
				index++
			}
			assert.Equal(t, len(items), index)
			tx.Rollback()
		})
		return true
	}
	if err := quick.Check(f, qconfig()); err != nil {
		t.Error(err)
	}
}

// Ensure that a transaction can iterate over all elements in a bucket in reverse.
func TestCursor_Iterate_Reverse(t *testing.T) {
	f := func(items testdata) bool {
		withOpenDB(func(db *DB, path string) {
			// Bulk insert all values.
			tx, _ := db.Begin(true)
			tx.CreateBucket([]byte("widgets"))
			b := tx.Bucket([]byte("widgets"))
			for _, item := range items {
				assert.NoError(t, b.Put(item.Key, item.Value))
			}
			assert.NoError(t, tx.Commit())

			// Sort test data.
			sort.Sort(revtestdata(items))

			// Iterate over all items and check consistency.
			var index = 0
			tx, _ = db.Begin(false)
			c := tx.Bucket([]byte("widgets")).Cursor()
			for k, v := c.Last(); k != nil && index < len(items); k, v = c.Prev() {
				assert.Equal(t, k, items[index].Key)
				assert.Equal(t, v, items[index].Value)
				index++
			}
			assert.Equal(t, len(items), index)
			tx.Rollback()
		})
		return true
	}
	if err := quick.Check(f, qconfig()); err != nil {
		t.Error(err)
	}
}

// Ensure that a Tx cursor can iterate over subbuckets.
func TestCursor_Iterate_BucketsOnly(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.Update(func(tx *Tx) error {
			b, err := tx.CreateBucket([]byte("widgets"))
			assert.NoError(t, err)
			_, err = b.CreateBucket([]byte("foo"))
			assert.NoError(t, err)
			_, err = b.CreateBucket([]byte("bar"))
			assert.NoError(t, err)
			_, err = b.CreateBucket([]byte("baz"))
			assert.NoError(t, err)
			return nil
		})
		db.View(func(tx *Tx) error {
			var names []string
			c := tx.Bucket([]byte("widgets")).Cursor()
			for k, v := c.First(); k != nil; k, v = c.Next() {
				names = append(names, string(k))
				assert.Nil(t, v)
			}
			assert.Equal(t, names, []string{"bar", "baz", "foo"})
			return nil
		})
	})
}

// Ensure that a Tx cursor can reverse iterate over subbuckets.
func TestCursor_Iterate_BucketsOnly_Reverse(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.Update(func(tx *Tx) error {
			b, err := tx.CreateBucket([]byte("widgets"))
			assert.NoError(t, err)
			_, err = b.CreateBucket([]byte("foo"))
			assert.NoError(t, err)
			_, err = b.CreateBucket([]byte("bar"))
			assert.NoError(t, err)
			_, err = b.CreateBucket([]byte("baz"))
			assert.NoError(t, err)
			return nil
		})
		db.View(func(tx *Tx) error {
			var names []string
			c := tx.Bucket([]byte("widgets")).Cursor()
			for k, v := c.Last(); k != nil; k, v = c.Prev() {
				names = append(names, string(k))
				assert.Nil(t, v)
			}
			assert.Equal(t, names, []string{"foo", "baz", "bar"})
			return nil
		})
	})
}
