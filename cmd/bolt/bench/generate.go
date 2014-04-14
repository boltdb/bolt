package bench

import (
	"fmt"
	"strings"

	"github.com/boltdb/bolt"
)

// Generate and write data to specified number of buckets/items.
func GenerateDB(db *bolt.DB, numBuckets, numItemsPerBucket int) error {
	return db.Update(func(tx *bolt.Tx) error {
		for bucketIndex := 0; bucketIndex < numBuckets; bucketIndex++ {
			bucketName := fmt.Sprintf("bucket%08d")
			tx.CreateBucket([]byte(bucketName))
			bucket := tx.Bucket([]byte(bucketName))
			for i := 0; i < numItemsPerBucket; i++ {
				value := []byte(strings.Repeat("0", 100))
				bucket.Put([]byte(fmt.Sprintf("key%08d", i)), value)
			}
		}
		return nil
	})
}
