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
	err := db.Do(func(t *RWTx) error {
		if err := t.CreateBucket("widgets"); err != nil {
			return err
		}
		b := t.Bucket("widgets")
		if err := b.Put([]byte("foo"), []byte("bar")); err != nil {
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

func ExampleDB_With() {
	// Open the database.
	var db DB
	db.Open("/tmp/bolt/db_foreach.db", 0666)
	defer db.Close()

	// Insert data into a bucket.
	db.CreateBucket("people")
	db.Put("people", []byte("john"), []byte("doe"))
	db.Put("people", []byte("susy"), []byte("que"))

	// Access data from within a read-only transactional block.
	db.With(func(t *Tx) error {
		v := t.Bucket("people").Get([]byte("john"))
		fmt.Printf("John's last name is %s.\n", string(v))
		return nil
	})

	// Output:
	// John's last name is doe.
}

func ExampleDB_ForEach() {
	// Open the database.
	var db DB
	db.Open("/tmp/bolt/db_foreach.db", 0666)
	defer db.Close()

	// Insert data into a bucket.
	db.CreateBucket("animals")
	db.Put("animals", []byte("dog"), []byte("fun"))
	db.Put("animals", []byte("cat"), []byte("lame"))
	db.Put("animals", []byte("liger"), []byte("awesome"))

	// Iterate over items in sorted key order.
	db.ForEach("animals", func(k, v []byte) error {
		fmt.Printf("A %s is %s.\n", string(k), string(v))
		return nil
	})

	// Output:
	// A cat is lame.
	// A dog is fun.
	// A liger is awesome.
}

func ExampleRWTx() {
	// Open the database.
	var db DB
	db.Open("/tmp/bolt/rwtx.db", 0666)
	defer db.Close()

	// Create a bucket.
	db.CreateBucket("widgets")

	// Create several keys in a transaction.
	rwtxn, _ := db.RWTx()
	b := rwtxn.Bucket("widgets")
	b.Put([]byte("john"), []byte("blue"))
	b.Put([]byte("abby"), []byte("red"))
	b.Put([]byte("zephyr"), []byte("purple"))
	rwtxn.Commit()

	// Iterate over the values in sorted key order.
	txn, _ := db.Tx()
	c := txn.Bucket("widgets").Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		fmt.Printf("%s likes %s\n", string(k), string(v))
	}
	txn.Close()

	// Output:
	// abby likes red
	// john likes blue
	// zephyr likes purple
}

func ExampleRWTx_rollback() {
	// Open the database.
	var db DB
	db.Open("/tmp/bolt/rwtx_rollback.db", 0666)
	defer db.Close()

	// Create a bucket.
	db.CreateBucket("widgets")

	// Set a value for a key.
	db.Put("widgets", []byte("foo"), []byte("bar"))

	// Update the key but rollback the transaction so it never saves.
	rwtxn, _ := db.RWTx()
	b := rwtxn.Bucket("widgets")
	b.Put([]byte("foo"), []byte("baz"))
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
