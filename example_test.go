package bolt

import (
	"fmt"
	"os"
)

func init() {
	os.RemoveAll("/tmp/bolt")
	os.MkdirAll("/tmp/bolt", 0777)
}

func ExampleDB_Update() {
	// Open the database.
	var db DB
	db.Open("/tmp/bolt/db_do.db", 0666)
	defer db.Close()

	// Execute several commands within a write transaction.
	err := db.Update(func(tx *Tx) error {
		if err := tx.CreateBucket("widgets"); err != nil {
			return err
		}
		b := tx.Bucket("widgets")
		if err := b.Put([]byte("foo"), []byte("bar")); err != nil {
			return err
		}
		return nil
	})

	// If our transactional block didn't return an error then our data is saved.
	if err == nil {
		db.View(func(tx *Tx) error {
			value := tx.Bucket("widgets").Get([]byte("foo"))
			fmt.Printf("The value of 'foo' is: %s\n", string(value))
			return nil
		})
	}

	// Output:
	// The value of 'foo' is: bar
}

func ExampleDB_View() {
	// Open the database.
	var db DB
	db.Open("/tmp/bolt/db_with.db", 0666)
	defer db.Close()

	// Insert data into a bucket.
	db.Update(func(tx *Tx) error {
		tx.CreateBucket("people")
		tx.Bucket("people").Put([]byte("john"), []byte("doe"))
		tx.Bucket("people").Put([]byte("susy"), []byte("que"))
		return nil
	})

	// Access data from within a read-only transactional block.
	db.View(func(t *Tx) error {
		v := t.Bucket("people").Get([]byte("john"))
		fmt.Printf("John's last name is %s.\n", string(v))
		return nil
	})

	// Output:
	// John's last name is doe.
}

func ExampleTx_Put() {
	// Open the database.
	var db DB
	db.Open("/tmp/bolt/db_put.db", 0666)
	defer db.Close()

	// Start a write transaction.
	db.Update(func(tx *Tx) error {
		// Create a bucket.
		tx.CreateBucket("widgets")

		// Set the value "bar" for the key "foo".
		tx.Bucket("widgets").Put([]byte("foo"), []byte("bar"))
		return nil
	})

	// Read value back in a different read-only transaction.
	db.Update(func(tx *Tx) error {
		value := tx.Bucket("widgets").Get([]byte("foo"))
		fmt.Printf("The value of 'foo' is: %s\n", string(value))
		return nil
	})

	// Output:
	// The value of 'foo' is: bar
}

func ExampleTx_Delete() {
	// Open the database.
	var db DB
	db.Open("/tmp/bolt/db_delete.db", 0666)
	defer db.Close()

	// Start a write transaction.
	db.Update(func(tx *Tx) error {
		// Create a bucket.
		tx.CreateBucket("widgets")
		b := tx.Bucket("widgets")

		// Set the value "bar" for the key "foo".
		b.Put([]byte("foo"), []byte("bar"))

		// Retrieve the key back from the database and verify it.
		value := b.Get([]byte("foo"))
		fmt.Printf("The value of 'foo' was: %s\n", string(value))
		return nil
	})

	// Delete the key in a different write transaction.
	db.Update(func(tx *Tx) error {
		return tx.Bucket("widgets").Delete([]byte("foo"))
	})

	// Retrieve the key again.
	db.View(func(tx *Tx) error {
		value := tx.Bucket("widgets").Get([]byte("foo"))
		if value == nil {
			fmt.Printf("The value of 'foo' is now: nil\n")
		}
		return nil
	})

	// Output:
	// The value of 'foo' was: bar
	// The value of 'foo' is now: nil
}

func ExampleTx_ForEach() {
	// Open the database.
	var db DB
	db.Open("/tmp/bolt/tx_foreach.db", 0666)
	defer db.Close()

	// Insert data into a bucket.
	db.Update(func(tx *Tx) error {
		tx.CreateBucket("animals")
		b := tx.Bucket("animals")
		b.Put([]byte("dog"), []byte("fun"))
		b.Put([]byte("cat"), []byte("lame"))
		b.Put([]byte("liger"), []byte("awesome"))

		// Iterate over items in sorted key order.
		b.ForEach(func(k, v []byte) error {
			fmt.Printf("A %s is %s.\n", string(k), string(v))
			return nil
		})
		return nil
	})

	// Output:
	// A cat is lame.
	// A dog is fun.
	// A liger is awesome.
}

func ExampleBegin_ReadOnly() {
	// Open the database.
	var db DB
	db.Open("/tmp/bolt/tx.db", 0666)
	defer db.Close()

	// Create a bucket.
	db.Update(func(tx *Tx) error {
		return tx.CreateBucket("widgets")
	})

	// Create several keys in a transaction.
	tx, _ := db.Begin(true)
	b := tx.Bucket("widgets")
	b.Put([]byte("john"), []byte("blue"))
	b.Put([]byte("abby"), []byte("red"))
	b.Put([]byte("zephyr"), []byte("purple"))
	tx.Commit()

	// Iterate over the values in sorted key order.
	tx, _ = db.Begin(false)
	c := tx.Bucket("widgets").Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		fmt.Printf("%s likes %s\n", string(k), string(v))
	}
	tx.Rollback()

	// Output:
	// abby likes red
	// john likes blue
	// zephyr likes purple
}

func ExampleTx_rollback() {
	// Open the database.
	var db DB
	db.Open("/tmp/bolt/tx_rollback.db", 0666)
	defer db.Close()

	// Create a bucket.
	db.Update(func(tx *Tx) error {
		return tx.CreateBucket("widgets")
	})

	// Set a value for a key.
	db.Update(func(tx *Tx) error {
		return tx.Bucket("widgets").Put([]byte("foo"), []byte("bar"))
	})

	// Update the key but rollback the transaction so it never saves.
	tx, _ := db.Begin(true)
	b := tx.Bucket("widgets")
	b.Put([]byte("foo"), []byte("baz"))
	tx.Rollback()

	// Ensure that our original value is still set.
	db.View(func(tx *Tx) error {
		value := tx.Bucket("widgets").Get([]byte("foo"))
		fmt.Printf("The value for 'foo' is still: %s\n", string(value))
		return nil
	})

	// Output:
	// The value for 'foo' is still: bar
}

func ExampleDB_CopyFile() {
	// Open the database.
	var db DB
	db.Open("/tmp/bolt/db_copy.db", 0666)
	defer db.Close()

	// Create a bucket and a key.
	db.Update(func(tx *Tx) error {
		tx.CreateBucket("widgets")
		tx.Bucket("widgets").Put([]byte("foo"), []byte("bar"))
		return nil
	})

	// Copy the database to another file.
	db.CopyFile("/tmp/bolt/db_copy_2.db", 0666)

	// Open the cloned database.
	var db2 DB
	db2.Open("/tmp/bolt/db_copy_2.db", 0666)
	defer db2.Close()

	// Ensure that the key exists in the copy.
	db2.View(func(tx *Tx) error {
		value := tx.Bucket("widgets").Get([]byte("foo"))
		fmt.Printf("The value for 'foo' in the clone is: %s\n", string(value))
		return nil
	})

	// Output:
	// The value for 'foo' in the clone is: bar
}
