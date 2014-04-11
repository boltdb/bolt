package main_test

import (
	"testing"

	"github.com/boltdb/bolt"
	. "github.com/boltdb/bolt/cmd/bolt"
	"github.com/stretchr/testify/assert"
)

// Ensure that a database can be exported.
func TestExport(t *testing.T) {
	SetTestMode(true)
	open(func(db *bolt.DB, path string) {
		db.Update(func(tx *bolt.Tx) error {
			tx.CreateBucket("widgets")
			b := tx.Bucket("widgets")
			b.Put([]byte("foo"), []byte("0000"))
			b.Put([]byte("bar"), []byte(""))

			tx.CreateBucket("woojits")
			b = tx.Bucket("woojits")
			b.Put([]byte("baz"), []byte("XXXX"))
			return nil
		})
		db.Close()
		output := run("export", path)
		assert.Equal(t, `[{"type":"bucket","key":"d2lkZ2V0cw==","value":[{"key":"YmFy","value":""},{"key":"Zm9v","value":"MDAwMA=="}]},{"type":"bucket","key":"d29vaml0cw==","value":[{"key":"YmF6","value":"WFhYWA=="}]}]`, output)
	})
}

// Ensure that an error is reported if the database is not found.
func TestExport_NotFound(t *testing.T) {
	SetTestMode(true)
	output := run("export", "no/such/db")
	assert.Equal(t, "stat no/such/db: no such file or directory", output)
}
