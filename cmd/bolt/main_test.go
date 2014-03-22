package main_test

import (
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/boltdb/bolt"
	. "github.com/boltdb/bolt/cmd/bolt"
	"github.com/stretchr/testify/assert"
)

// Ensure that a value can be retrieved from the CLI.
func TestGet(t *testing.T) {
	SetTestMode(true)
	open(func(db *bolt.DB) {
		db.Do(func(tx *bolt.Tx) error {
			tx.CreateBucket("widgets")
			tx.Bucket("widgets").Put([]byte("foo"), []byte("bar"))
			return nil
		})
		output := run("get", db.Path(), "widgets", "foo")
		assert.Equal(t, "bar", output)
	})
}

// Ensure that an error is reported if the database is not found.
func TestGetDBNotFound(t *testing.T) {
	SetTestMode(true)
	output := run("get", "no/such/db", "widgets", "foo")
	assert.Equal(t, "stat no/such/db: no such file or directory", output)
}

// Ensure that an error is reported if the bucket is not found.
func TestGetBucketNotFound(t *testing.T) {
	SetTestMode(true)
	open(func(db *bolt.DB) {
		output := run("get", db.Path(), "widgets", "foo")
		assert.Equal(t, "bucket not found: widgets", output)
	})
}

// Ensure that an error is reported if the key is not found.
func TestGetKeyNotFound(t *testing.T) {
	SetTestMode(true)
	open(func(db *bolt.DB) {
		db.Do(func(tx *bolt.Tx) error {
			return tx.CreateBucket("widgets")
		})
		output := run("get", db.Path(), "widgets", "foo")
		assert.Equal(t, "key not found: foo", output)
	})
}

// Ensure that a list of keys can be retrieved for a given bucket.
func TestKeys(t *testing.T) {
	SetTestMode(true)
	open(func(db *bolt.DB) {
		db.Do(func(tx *bolt.Tx) error {
			tx.CreateBucket("widgets")
			tx.Bucket("widgets").Put([]byte("0002"), []byte(""))
			tx.Bucket("widgets").Put([]byte("0001"), []byte(""))
			tx.Bucket("widgets").Put([]byte("0003"), []byte(""))
			return nil
		})
		output := run("keys", db.Path(), "widgets")
		assert.Equal(t, "0001\n0002\n0003", output)
	})
}

// Ensure that an error is reported if the database is not found.
func TestKeysDBNotFound(t *testing.T) {
	SetTestMode(true)
	output := run("keys", "no/such/db", "widgets")
	assert.Equal(t, "stat no/such/db: no such file or directory", output)
}

// Ensure that an error is reported if the bucket is not found.
func TestKeysBucketNotFound(t *testing.T) {
	SetTestMode(true)
	open(func(db *bolt.DB) {
		output := run("keys", db.Path(), "widgets")
		assert.Equal(t, "bucket not found: widgets", output)
	})
}

// open creates and opens a Bolt database in the temp directory.
func open(fn func(*bolt.DB)) {
	f, _ := ioutil.TempFile("", "bolt-")
	f.Close()
	os.Remove(f.Name())
	defer os.RemoveAll(f.Name())

	db, err := bolt.Open(f.Name(), 0600)
	if err != nil {
		panic("db open error: " + err.Error())
	}
	fn(db)
}

// run executes a command against the CLI and returns the output.
func run(args ...string) string {
	args = append([]string{"bolt"}, args...)
	NewApp().Run(args)
	return strings.TrimSpace(LogBuffer())
}
