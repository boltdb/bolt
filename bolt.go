package bolt

import (
	"fmt"
	"os"
)

// Open creates and opens a database at the given path.
// If the file does not exist then it will be created automatically.
func Open(path string, mode os.FileMode) (*DB, error) {
	db := &DB{}
	if err := db.Open(path, mode); err != nil {
		return nil, err
	}
	return db, nil
}

// ErrorList represents a slice of errors.
type ErrorList []error

// Error returns a readable count of the errors in the list.
func (l ErrorList) Error() string {
	return fmt.Sprintf("%d errors occurred", len(l))
}

// _assert will panic with a given formatted message if the given condition is false.
func _assert(condition bool, msg string, v ...interface{}) {
	if !condition {
		panic(fmt.Sprintf("assertion failed: "+msg, v...))
	}
}

func warn(v ...interface{}) {
	fmt.Fprintln(os.Stderr, v...)
}

func warnf(msg string, v ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", v...)
}
