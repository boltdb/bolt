package main_test

import (
	"io/ioutil"
	"os"
	"strings"

	"github.com/boltdb/bolt"
	. "github.com/boltdb/bolt/cmd/bolt"
)

// open creates and opens a Bolt database in the temp directory.
func open(fn func(*bolt.DB, string)) {
	path := tempfile()
	defer os.RemoveAll(path)

	db, err := bolt.Open(path, 0600)
	if err != nil {
		panic("db open error: " + err.Error())
	}
	fn(db, path)
}

// run executes a command against the CLI and returns the output.
func run(args ...string) string {
	args = append([]string{"bolt"}, args...)
	NewApp().Run(args)
	return strings.TrimSpace(LogBuffer())
}

// tempfile returns a temporary file path.
func tempfile() string {
	f, _ := ioutil.TempFile("", "bolt-")
	f.Close()
	os.Remove(f.Name())
	return f.Name()
}
