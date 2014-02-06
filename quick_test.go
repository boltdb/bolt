package bolt

import (
	"bytes"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"testing"
	"testing/quick"
	"time"
)

// testing/quick defaults to 100 iterations and a random seed.
// You can override these settings from the command line:
//
//   -quickchecks     The number of iterations to perform.
//   -quick.seed      The seed to use for randomizing.
//   -quick.maxitems  The maximum number of items to insert into a DB.
//   -quick.maxksize  The maximum size of a key.
//   -quick.maxvsize  The maximum size of a value.
//

var qseed, qmaxitems, qmaxksize, qmaxvsize int

func init() {
	flag.IntVar(&qseed, "quick.seed", int(time.Now().UnixNano())%100000, "")
	flag.IntVar(&qmaxitems, "quick.maxitems", 1000, "")
	flag.IntVar(&qmaxksize, "quick.maxksize", 1024, "")
	flag.IntVar(&qmaxvsize, "quick.maxvsize", 1024, "")
	flag.Parse()
	warn("seed:", qseed)
}

// Ensure that a bucket can write random keys and values across multiple txns.
func TestQuickPut(t *testing.T) {
	index := 0
	f := func(items testdata) bool {
		withOpenDB(func(db *DB, path string) {
			m := make(map[string][]byte)

			db.CreateBucket("widgets")

			for _, item := range items {
				if err := db.Put("widgets", item.Key, item.Value); err != nil {
					panic("put error: " + err.Error())
				}
				m[string(item.Key)] = item.Value

				// Verify all key/values so far.
				i := 0
				for k, v := range m {
					value, err := db.Get("widgets", []byte(k))
					if err != nil {
						panic("get error: " + err.Error())
					}
					if !bytes.Equal(value, v) {
						db.CopyFile("/tmp/bolt.random.db")
						t.Fatalf("value mismatch [run %d] (%d of %d):\nkey: %x\ngot: %x\nexp: %x", index, i, len(m), []byte(k), v, value)
					}
					i++
				}
			}

			fmt.Fprint(os.Stderr, ".")
		})
		index++
		return true
	}
	if err := quick.Check(f, &quick.Config{Rand: rand.New(rand.NewSource(int64(qseed)))}); err != nil {
		t.Error(err)
	}
	fmt.Fprint(os.Stderr, "\n")
}

type testdata []testdataitem

func (t testdata) Generate(rand *rand.Rand, size int) reflect.Value {
	n := rand.Intn(qmaxitems-1) + 1
	items := make(testdata, n)
	for i := 0; i < n; i++ {
		item := &items[i]
		item.Key = randByteSlice(rand, 1, qmaxksize)
		item.Value = randByteSlice(rand, 0, qmaxvsize)
	}
	return reflect.ValueOf(items)
}

type testdataitem struct {
	Key   []byte
	Value []byte
}

func randByteSlice(rand *rand.Rand, minSize, maxSize int) []byte {
	n := rand.Intn(maxSize-minSize) + minSize
	b := make([]byte, n)
	for i := 0; i < n; i++ {
		b[i] = byte(rand.Intn(255))
	}
	return b
}
