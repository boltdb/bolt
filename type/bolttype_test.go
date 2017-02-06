package bolttype

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBasic(t *testing.T) {
	runWithDB(t, testBasic)
}

func testBasic(t *testing.T, db *DB) {
	bucketName := []byte("test")
	err := db.Update(
		func(tx *Tx) error {
			bucket, err := tx.CreateBucket(bucketName)
			require.NoError(t, err)
			require.NoError(t, bucket.PutUint32Uint32ToString(1, 2, "a"))
			require.NoError(t, bucket.PutUint32Uint32ToString(1, 4, "b"))
			require.NoError(t, bucket.PutUint32Uint32ToString(2, 3, "c"))
			require.NoError(t, bucket.PutUint32Uint32ToString(3, 3, "d"))
			return nil
		},
	)
	require.NoError(t, err)
	err = db.View(
		func(tx *Tx) error {
			bucket := tx.Bucket(bucketName)
			expected := []struct {
				Key1  uint32
				Key2  uint32
				Value string
			}{
				{
					Key1:  1,
					Key2:  2,
					Value: "a",
				},
				{
					Key1:  1,
					Key2:  4,
					Value: "b",
				},
				{
					Key1:  2,
					Key2:  3,
					Value: "c",
				},
				{
					Key1:  3,
					Key2:  3,
					Value: "d",
				},
			}
			i := 0
			err := bucket.ForEachUint32Uint32ToString(func(key1 uint32, key2 uint32, value string) error {
				t.Logf("i:%d key1:%d key2:%d value:%s\n", i, key1, key2, value)
				require.True(t, i < len(expected))
				elem := expected[i]
				require.Equal(t, elem.Key1, key1)
				require.Equal(t, elem.Key2, key2)
				require.Equal(t, elem.Value, value)
				i++
				return nil
			})
			require.NoError(t, err)
			require.Equal(t, 4, i)
			return nil
		},
	)
	require.NoError(t, err)
}

// TODO(pedge): require functions will mean cleanup is not done and close is not called
func runWithDB(t *testing.T, f func(*testing.T, *DB)) {
	file, err := ioutil.TempFile("", "bolttype")
	require.NoError(t, err)
	name := file.Name()
	defer func() {
		_ = file.Close()
		_ = os.Remove(name)
	}()
	db, err := Open(name, 0644, nil)
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()
	f(t, db)
}
