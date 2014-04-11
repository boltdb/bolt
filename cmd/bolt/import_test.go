package main_test

import (
	"io/ioutil"
	"testing"

	"github.com/boltdb/bolt"
	. "github.com/boltdb/bolt/cmd/bolt"
	"github.com/stretchr/testify/assert"
)

// Ensure that a database can be imported.
func TestImport(t *testing.T) {
	SetTestMode(true)

	// Write input file.
	input := tempfile()
	assert.NoError(t, ioutil.WriteFile(input, []byte(`[{"type":"bucket","key":"ZW1wdHk=","value":[]},{"type":"bucket","key":"d2lkZ2V0cw==","value":[{"key":"YmFy","value":""},{"key":"Zm9v","value":"MDAwMA=="}]},{"type":"bucket","key":"d29vaml0cw==","value":[{"key":"YmF6","value":"WFhYWA=="},{"type":"bucket","key":"d29vaml0cy9zdWJidWNrZXQ=","value":[{"key":"YmF0","value":"QQ=="}]}]}]`), 0600))

	// Import database.
	path := tempfile()
	output := run("import", path, "--input", input)
	assert.Equal(t, ``, output)

	// Open database and verify contents.
	db, err := bolt.Open(path, 0600)
	assert.NoError(t, err)
	db.View(func(tx *bolt.Tx) error {
		assert.NotNil(t, tx.Bucket([]byte("empty")))

		b := tx.Bucket([]byte("widgets"))
		if assert.NotNil(t, b) {
			assert.Equal(t, []byte("0000"), b.Get([]byte("foo")))
			assert.Equal(t, []byte(""), b.Get([]byte("bar")))
		}

		b = tx.Bucket([]byte("woojits"))
		if assert.NotNil(t, b) {
			assert.Equal(t, []byte("XXXX"), b.Get([]byte("baz")))

			b = b.Bucket([]byte("woojits/subbucket"))
			assert.Equal(t, []byte("A"), b.Get([]byte("bat")))
		}

		return nil
	})
	db.Close()
}

// Ensure that an error is reported if the database is not found.
func TestImport_NotFound(t *testing.T) {
	SetTestMode(true)
	output := run("import", "path/to/db", "--input", "no/such/file")
	assert.Equal(t, "open no/such/file: no such file or directory", output)
}
