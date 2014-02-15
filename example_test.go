package bolt

import (
	"fmt"
	"os"
)

func init() {
	os.RemoveAll("/tmp/bolt")
	os.MkdirAll("/tmp/bolt", 0777)
}

func ExampleDB_Put() {
	// Open the database.
	var db DB
	db.Open("/tmp/bolt/db_put.db", 0666)
	defer db.Close()

	// Create a bucket.
	db.CreateBucket("widgets")

	// Set the value "bar" for the key "foo".
	db.Put("widgets", []byte("foo"), []byte("bar"))

	// Retrieve the key back from the database and verify it.
	value, _ := db.Get("widgets", []byte("foo"))
	fmt.Printf("The value of 'foo' is: %s\n", string(value))

	// Output:
	// The value of 'foo' is: bar
}

func ExampleDB_Delete() {
	// Open the database.
	var db DB
	db.Open("/tmp/bolt/db_delete.db", 0666)
	defer db.Close()

	// Create a bucket.
	db.CreateBucket("widgets")

	// Set the value "bar" for the key "foo".
	db.Put("widgets", []byte("foo"), []byte("bar"))

	// Retrieve the key back from the database and verify it.
	value, _ := db.Get("widgets", []byte("foo"))
	fmt.Printf("The value of 'foo' was: %s\n", string(value))

	// Delete the "foo" key.
	db.Delete("widgets", []byte("foo"))

	// Retrieve the key again.
	value, _ = db.Get("widgets", []byte("foo"))
	if value == nil {
		fmt.Printf("The value of 'foo' is now: nil\n")
	}

	// Output:
	// The value of 'foo' was: bar
	// The value of 'foo' is now: nil
}

func ExampleDB_Do() {
	// Open the database.
	var db DB
	db.Open("/tmp/bolt/db_do.db", 0666)
	defer db.Close()

	// Execute several commands within a write transaction.
	err := db.Do(func(t *RWTransaction) error {
		if err := t.CreateBucket("widgets"); err != nil {
			return err
		}
		if err := t.Put("widgets", []byte("foo"), []byte("bar")); err != nil {
			return err
		}
		return nil
	})

	// If our transactional block didn't return an error then our data is saved.
	if err == nil {
		value, _ := db.Get("widgets", []byte("foo"))
		fmt.Printf("The value of 'foo' is: %s\n", string(value))
	}

	// Output:
	// The value of 'foo' is: bar
}

func ExampleRWTransaction() {
	// Open the database.
	var db DB
	db.Open("/tmp/bolt/rwtransaction.db", 0666)
	defer db.Close()

	// Create a bucket.
	db.CreateBucket("widgets")

	// Create several keys in a transaction.
	rwtxn, _ := db.RWTransaction()
	rwtxn.Put("widgets", []byte("john"), []byte("blue"))
	rwtxn.Put("widgets", []byte("abby"), []byte("red"))
	rwtxn.Put("widgets", []byte("zephyr"), []byte("purple"))
	rwtxn.Commit()

	// Iterate over the values in sorted key order.
	txn, _ := db.Transaction()
	c, _ := txn.Cursor("widgets")
	for k, v := c.First(); k != nil; k, v = c.Next() {
		fmt.Printf("%s likes %s\n", string(k), string(v))
	}
	txn.Close()

	// Output:
	// abby likes red
	// john likes blue
	// zephyr likes purple
}

func ExampleRWTransaction_rollback() {
	// Open the database.
	var db DB
	db.Open("/tmp/bolt/rwtransaction_rollback.db", 0666)
	defer db.Close()

	// Create a bucket.
	db.CreateBucket("widgets")

	// Set a value for a key.
	db.Put("widgets", []byte("foo"), []byte("bar"))

	// Update the key but rollback the transaction so it never saves.
	rwtxn, _ := db.RWTransaction()
	rwtxn.Put("widgets", []byte("foo"), []byte("baz"))
	rwtxn.Rollback()

	// Ensure that our original value is still set.
	value, _ := db.Get("widgets", []byte("foo"))
	fmt.Printf("The value for 'foo' is still: %s\n", string(value))

	// Output:
	// The value for 'foo' is still: bar
}

func ExampleDB_CopyFile() {
	// Open the database.
	var db DB
	db.Open("/tmp/bolt/db_copy.db", 0666)
	defer db.Close()

	// Create a bucket and a key.
	db.CreateBucket("widgets")
	db.Put("widgets", []byte("foo"), []byte("bar"))

	// Copy the database to another file.
	db.CopyFile("/tmp/bolt/db_copy_2.db", 0666)

	// Open the cloned database.
	var db2 DB
	db2.Open("/tmp/bolt/db_copy_2.db", 0666)
	defer db2.Close()

	// Ensure that the key exists in the copy.
	value, _ := db2.Get("widgets", []byte("foo"))
	fmt.Printf("The value for 'foo' in the clone is: %s\n", string(value))

	// Output:
	// The value for 'foo' in the clone is: bar
}
