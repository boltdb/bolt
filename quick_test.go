package bolt

import (
	"flag"
	"math/rand"
	"reflect"
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

var seed, testMaxItemCount, testMaxKeySize, testMaxValueSize int

func init() {
	flag.IntVar(&seed, "quick.seed", int(time.Now().UnixNano())%100000, "")
	flag.IntVar(&testMaxItemCount, "quick.maxitems", 1024, "")
	flag.IntVar(&testMaxKeySize, "quick.maxksize", 1024, "")
	flag.IntVar(&testMaxValueSize, "quick.maxvsize", 1024, "")
	warn("seed:", seed)
}

// qc creates a testing/quick configuration.
func qc() *quick.Config {
	return &quick.Config{Rand: rand.New(rand.NewSource(int64(seed)))}
}

type testKeyValuePairs []testKeyValuePair

func (t testKeyValuePairs) Generate(rand *rand.Rand, size int) reflect.Value {
	n := rand.Intn(testMaxItemCount-1) + 1
	items := make(testKeyValuePairs, n)
	for i := 0; i < n; i++ {
		items[i].Generate(rand, size)
	}
	return reflect.ValueOf(items)
}

type testKeyValuePair struct {
	Key   []byte
	Value []byte
}

func (t testKeyValuePair) Generate(rand *rand.Rand, size int) reflect.Value {
	t.Key = randByteSlice(rand, 1, testMaxKeySize)
	t.Value = randByteSlice(rand, 0, testMaxValueSize)
	return reflect.ValueOf(t)
}

func randByteSlice(rand *rand.Rand, minSize, maxSize int) []byte {
	n := rand.Intn(maxSize - minSize) + minSize
	b := make([]byte, n)
	for i := 0; i < n; i++ {
		b[i] = byte(rand.Intn(255))
	}
	return b
}
