package main_test

import (
	"io/ioutil"
	"testing"

	"github.com/boltdb/bolt"
	. "github.com/boltdb/bolt/cmd/bolt"
)

// Ensure that a database can be imported.
func TestImport(t *testing.T) {
	SetTestMode(true)

	// Write input file.
	input := tempfile()
	ok(t, ioutil.WriteFile(input, []byte(`[{"type":"bucket","key":"ZW1wdHk=","value":[]},{"type":"bucket","key":"d2lkZ2V0cw==","value":[{"key":"YmFy","value":""},{"key":"Zm9v","value":"MDAwMA=="}]},{"type":"bucket","key":"d29vaml0cw==","value":[{"key":"YmF6","value":"WFhYWA=="},{"type":"bucket","key":"d29vaml0cy9zdWJidWNrZXQ=","value":[{"key":"YmF0","value":"QQ=="}]}]}]`), 0600))

	// Import database.
	path := tempfile()
	output := run("import", path, "--input", input)
	equals(t, ``, output)

	// Open database and verify contents.
	db, err := bolt.Open(path, 0600, nil)
	ok(t, err)
	db.View(func(tx *bolt.Tx) error {
		assert(t, tx.Bucket([]byte("empty")) != nil, "")

		b := tx.Bucket([]byte("widgets"))
		assert(t, b != nil, "")
		equals(t, []byte("0000"), b.Get([]byte("foo")))
		equals(t, []byte(""), b.Get([]byte("bar")))

		b = tx.Bucket([]byte("woojits"))
		assert(t, b != nil, "")
		equals(t, []byte("XXXX"), b.Get([]byte("baz")))

		b = b.Bucket([]byte("woojits/subbucket"))
		equals(t, []byte("A"), b.Get([]byte("bat")))

		return nil
	})
	db.Close()
}

// Ensure that an error is reported if the database is not found.
func TestImport_NotFound(t *testing.T) {
	SetTestMode(true)
	output := run("import", "path/to/db", "--input", "no/such/file")
	equals(t, "open no/such/file: no such file or directory", output)
}
