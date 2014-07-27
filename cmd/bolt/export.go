package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/boltdb/bolt"
)

// Export exports the entire database as a JSON document.
func Export(path string) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		fatal(err)
		return
	}

	// Open the database.
	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		fatal(err)
		return
	}
	defer db.Close()

	err = db.View(func(tx *bolt.Tx) error {
		// Loop over every bucket and export it as a raw message.
		var root []*rawMessage
		err := tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			message, err := exportBucket(b)
			if err != nil {
				fatal(err)
			}
			message.Key = name
			root = append(root, message)
			return nil
		})
		if err != nil {
			return err
		}

		// Encode all buckets into JSON.
		output, err := json.Marshal(root)
		if err != nil {
			return fmt.Errorf("encode: %s", err)
		}
		print(string(output))
		return nil
	})
	if err != nil {
		fatal(err)
	}
}

func exportBucket(b *bolt.Bucket) (*rawMessage, error) {
	// Encode individual key/value pairs into raw messages.
	var children = make([]*rawMessage, 0)
	err := b.ForEach(func(k, v []byte) error {
		var err error

		// If there is no value then it is a bucket.
		if v == nil {
			child, err := exportBucket(b.Bucket(k))
			if err != nil {
				return fmt.Errorf("bucket: %s", err)
			}
			child.Key = k
			children = append(children, child)
			return nil
		}

		// Otherwise it's a regular key.
		var child = &rawMessage{Key: k}
		if child.Value, err = json.Marshal(v); err != nil {
			return fmt.Errorf("value: %s", err)
		}
		children = append(children, child)
		return nil
	})
	if err != nil {
		return nil, err
	}

	// Encode bucket into a raw message.
	var root = rawMessage{Type: "bucket"}
	if root.Value, err = json.Marshal(children); err != nil {
		return nil, fmt.Errorf("children: %s", err)
	}

	return &root, nil
}
