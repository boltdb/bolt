package c

import "C"
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
			var k, v C.bolt_val
			c := NewCursor(tx.Bucket("widgets"))
			C.bolt_cursor_first(c, &k, &v)
			key := C.GoBytes(k.data, k.size)
			for key != nil && index < len(items) {
				assert.Equal(t, key, items[index].Key)
				assert.Equal(t, C.GoBytes(v.data, v.size), items[index].Value)
				index++
				C.bolt_cursor_next(c, &k, &v)
				key := C.GoBytes(k.data, k.size)
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
