package main_test

import (
	"testing"

	"github.com/boltdb/bolt"
	. "github.com/boltdb/bolt/cmd/bolt"
)

// Ensure that a database can be exported.
func TestExport(t *testing.T) {
	SetTestMode(true)
	open(func(db *bolt.DB, path string) {
		db.Update(func(tx *bolt.Tx) error {
			tx.CreateBucket([]byte("widgets"))
			b := tx.Bucket([]byte("widgets"))
			b.Put([]byte("foo"), []byte("0000"))
			b.Put([]byte("bar"), []byte(""))

			tx.CreateBucket([]byte("woojits"))
			b = tx.Bucket([]byte("woojits"))
			b.Put([]byte("baz"), []byte("XXXX"))

			b.CreateBucket([]byte("woojits/subbucket"))
			b = b.Bucket([]byte("woojits/subbucket"))
			b.Put([]byte("bat"), []byte("A"))

			tx.CreateBucket([]byte("empty"))

			return nil
		})
		db.Close()
		output := run("export", path)
		equals(t, `[{"type":"bucket","key":"ZW1wdHk=","value":[]},{"type":"bucket","key":"d2lkZ2V0cw==","value":[{"key":"YmFy","value":""},{"key":"Zm9v","value":"MDAwMA=="}]},{"type":"bucket","key":"d29vaml0cw==","value":[{"key":"YmF6","value":"WFhYWA=="},{"type":"bucket","key":"d29vaml0cy9zdWJidWNrZXQ=","value":[{"key":"YmF0","value":"QQ=="}]}]}]`, output)
	})
}

// Ensure that an error is reported if the database is not found.
func TestExport_NotFound(t *testing.T) {
	SetTestMode(true)
	output := run("export", "no/such/db")
	equals(t, "stat no/such/db: no such file or directory", output)
}
