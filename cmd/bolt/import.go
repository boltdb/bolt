package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/boltdb/bolt"
)

// Import converts an exported database dump into a new database.
func Import(path string, input string) {
	f, err := os.Open(input)
	if err != nil {
		fatal(err)
		return
	}
	defer f.Close()

	// Read in entire dump.
	var root []*rawMessage
	if err := json.NewDecoder(f).Decode(&root); err != nil {
		fatal(err)
	}

	// Open the database.
	db, err := bolt.Open(path, 0600)
	if err != nil {
		fatal(err)
		return
	}
	defer db.Close()

	// Insert entire dump into database.
	err = db.Update(func(tx *bolt.Tx) error {
		// Loop over every message and create a bucket.
		for _, message := range root {
			// Validate that root messages are buckets.
			if message.Type != "bucket" {
				return fmt.Errorf("invalid root type: %q", message.Type)
			}

			// Create the bucket if it doesn't exist.
			if err := tx.CreateBucketIfNotExists(string(message.Key)); err != nil {
				return fmt.Errorf("create bucket: %s", err)
			}

			// Decode child messages.
			var children []*rawMessage
			if err := json.Unmarshal(message.Value, &children); err != nil {
				return fmt.Errorf("decode children: %s", err)
			}

			// Import all the values into the bucket.
			b := tx.Bucket(string(message.Key))
			if err := importBucket(b, children); err != nil {
				return fmt.Errorf("import bucket: %s", err)
			}
		}
		return nil
	})
	if err != nil {
		fatal("update: ", err)
	}
}

func importBucket(b *bolt.Bucket, children []*rawMessage) error {
	// Decode each message into a key/value pair.
	for _, child := range children {
		// Decode the base64 value.
		var value []byte
		if err := json.Unmarshal(child.Value, &value); err != nil {
			return fmt.Errorf("decode value: %s", err)
		}

		// Insert key/value into bucket.
		if err := b.Put(child.Key, value); err != nil {
			return fmt.Errorf("put: %s", err)
		}
	}
	return nil
}
