package c

import (
	"github.com/boltdb/bolt"
	"github.com/stretchr/testify/assert"
	"testing"
	"testing/quick"
)

// Ensure that a cursor can iterate over all elements in a bucket.
func TestIterate(t *testing.T) {
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
			c := NewCursor(tx.Bucket("widgets"))
			for key, value := c.first(); key != nil && index < len(items); key, value = c.next() {
				assert.Equal(t, key, items[index].Key)
				assert.Equal(t, value, items[index].Value)
				index++
			}
			assert.Equal(t, len(items), index)
			assert.Equal(t, len(items), index)
			tx.Rollback()
		})
		return true
	}
	if err := quick.Check(f, qconfig()); err != nil {
		t.Error(err)
	}
	fmt.Fprint(os.Stderr, "\n")
}
