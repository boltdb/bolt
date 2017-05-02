package bolt_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/boltdb/bolt"
)

func TestCompact(t *testing.T) {

	// Open a data file.
	db := MustOpenDB()
	defer db.MustClose()

	// Insert until we get above the minimum 4MB size.
	if err := db.Update(func(tx *bolt.Tx) error {
		b, _ := tx.CreateBucketIfNotExists([]byte("data"))
		for i := 0; i < 10000; i++ {
			if err := b.Put([]byte(fmt.Sprintf("%04d", i)), make([]byte, 1000)); err != nil {
				t.Fatal(err)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	fi, err := os.Stat(db.Path())
	if err != nil {
		t.Fatal(err)
	}

	// Compact then check that it is all still there.
	err = db.Compact(0, testing.Verbose())
	if err != nil {
		t.Fatal(err)
	}

	fi2, err := os.Stat(db.Path())
	if err != nil {
		t.Fatal(err)
	}

	if err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("data"))
		for i := 0; i < 10000; i++ {
			if v := b.Get([]byte(fmt.Sprintf("%04d", i))); v == nil || len(v) != 1000 {
				t.Fatalf("expected non-nil value of len 1000")
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// check that we shrunk in size also
	startSz := fi.Size()
	endSz := fi2.Size()
	if endSz < startSz {
		// good, typically seeing  37490688 -> 25444352 bytes (gain=1.47x)
	} else {
		// bad
		t.Fatalf("after Compress, endSz = %v was not less than startSz %v", endSz, startSz)
	}

	if err := db.DB.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestAutoCompact(t *testing.T) {

	// Open a data file.
	db := MustOpenDB()
	defer db.MustClose()

	// setup automatic compaction after one commit.
	db.CompactAfterCommitCount = 1

	// Insert until we get above the minimum 4MB size.
	if err := db.Update(func(tx *bolt.Tx) error {
		b, _ := tx.CreateBucketIfNotExists([]byte("data"))
		for i := 0; i < 10000; i++ {
			if err := b.Put([]byte(fmt.Sprintf("%04d", i)), make([]byte, 1000)); err != nil {
				t.Fatal(err)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// auto-compaction should have happened
	count := db.SuccessfulCompactionCount()
	if count != 1 {
		t.Fatalf("expected 1 auto-compaction, saw %v", count)
	}

	fi, err := os.Stat(db.Path())
	if err != nil {
		t.Fatal(err)
	}
	if testing.Verbose() {
		fmt.Printf("Size after auto-compaction: %v bytes.\n", fi.Size())
	}

	// do 20 more after setting
	db.CompactAfterCommitCount = 100
	// so we should not see any auto-compaction.

	for j := 0; j < 20; j++ {
		if err := db.Update(func(tx *bolt.Tx) error {
			b, _ := tx.CreateBucketIfNotExists([]byte("data"))
			for i := 0; i < 1; i++ {
				if err := b.Put([]byte(fmt.Sprintf("%04d", i)), make([]byte, 1000)); err != nil {
					t.Fatal(err)
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}

	// should have seen no additional compaction.
	count = db.SuccessfulCompactionCount()
	if count != 1 {
		t.Fatalf("expected 1 auto-compaction, saw %v", count)
	}

	// after having done 20 commits above, if we set
	db.CompactAfterCommitCount = 20
	// then one more commit should result in a compaction.

	if err := db.Update(func(tx *bolt.Tx) error {
		b, _ := tx.CreateBucketIfNotExists([]byte("data"))
		for i := 0; i < 1; i++ {
			if err := b.Put([]byte(fmt.Sprintf("%04d", i)), make([]byte, 1000)); err != nil {
				t.Fatal(err)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	count = db.SuccessfulCompactionCount()
	if count != 2 {
		t.Fatalf("expected 2 auto-compaction, saw %v", count)
	}

	if err := db.DB.Close(); err != nil {
		t.Fatal(err)
	}
}
