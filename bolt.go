package bolt

import (
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
