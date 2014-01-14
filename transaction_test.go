package bolt

import (
	"testing"

//	"github.com/stretchr/testify/assert"
)

//--------------------------------------
// Cursor()
//--------------------------------------

// Ensure that a read transaction can get a cursor.
func TestTransactionCursor(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		/*
		txn, _ := db.Transaction(false)
		c := txn.Cursor()
		assert.NotNil(t, c)
		*/
	})
}
