package bolt

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Ensure that a database can be opened without error.
func TestDBOpen(t *testing.T) {
	withDB(func(db *DB, path string) {
		err := db.Open(path, 0666)
		assert.NoError(t, err)
		assert.Equal(t, db.Path(), path)
	})
}

// Ensure that the database returns an error if already open.
func TestDBReopen(t *testing.T) {
	withDB(func(db *DB, path string) {
		db.Open(path, 0666)
		err := db.Open(path, 0666)
		assert.Equal(t, err, DatabaseAlreadyOpenedError)
	})
}

// Ensure that the database returns an error if the file handle cannot be open.
func TestDBOpenFileError(t *testing.T) {
	withMockDB(func(db *DB, mockos *mockos, path string) {
		exp := &os.PathError{}
		mockos.On("OpenFile", path, os.O_RDWR|os.O_CREATE, os.FileMode(0666)).Return((*os.File)(nil), exp)
		err := db.Open(path, 0666)
		assert.Equal(t, err, exp)
	})
}

// Ensure that the database returns an error if the meta file handle cannot be open.
func TestDBOpenMetaFileError(t *testing.T) {
	withMockDB(func(db *DB, mockos *mockos, path string) {
		exp := &os.PathError{}
		mockos.On("OpenFile", path, os.O_RDWR|os.O_CREATE, os.FileMode(0666)).Return(&os.File{}, nil)
		mockos.On("OpenFile", path, os.O_RDWR|os.O_SYNC, os.FileMode(0666)).Return((*os.File)(nil), exp)
		err := db.Open(path, 0666)
		assert.Equal(t, err, exp)
	})
}

// withDB executes a function with a database reference.
func withDB(fn func(*DB, string)) {
	f, _ := ioutil.TempFile("", "bolt-")
	path := f.Name()
	f.Close()
	os.Remove(path)
	defer os.RemoveAll(path)

	db := NewDB()
	fn(db, path)
}

// withMockDB executes a function with a database reference and a mock filesystem.
func withMockDB(fn func(*DB, *mockos, string)) {
	os := &mockos{}
	db := NewDB()
	db.os = os
	fn(db, os, "/mock/db")
}
