package bolt

import (
	"fmt"
	"log"
	"os"
	"sync/atomic"
)

// QuietlyCompact calls compact with the defaut batchSizeBytes
// and does no logging of the impact of the compression.
func (db *DB) CompactQuietly() error {
	return db.Compact(0, false)
}

// Compact does these steps to reduce the
// space fragmentation that happens in a bolt database.
//
// 0) Open a fresh .compact_in_progress bolt db file.
// 1) Read each object from the bolt db and write it
//    into the fresh .compact_in_progress database.
// 2) Close the both files.
// 3) Rename the .compact_in_progress file to be the original db file name.
//    This leverages the fact that os.Rename is atomic.
// 4) Re-open the newly compact-ed db file.
//
// INVAR: db must be already open.
//
// Side effect:
//  The file db.path+".compact_in_progress" will be
//  deleted at the top of Compact(), then used to
//  create the newly compacted database.
//
// batchSizeInBytes should supply the threshold size in bytes
// after which we commit a transaction. The default of 4MB
// will be used if batchSizeInBytes is <= 0.
//
// If logImpact is true, we will call log.Printf with a
// message reporting how much space was saved by the
// compaction.
//
// If db.readOnly, then Compact is a no-op, and we return nil.
//
func (db *DB) Compact(batchSizeBytes int64, logImpact bool) error {

	if db.readOnly {
		return nil
	}

	// Obtainer writer lock to guarantee nobody else is
	// using the db at this time.
	db.rwlock.Lock()
	skipDbUnlock := false
	defer func() {
		if !skipDbUnlock {
			db.rwlock.Unlock()
		}
	}()

	// the db.Close() below will wipe db.path, so preserve it.
	srcPath := db.path

	if batchSizeBytes <= 0 {
		batchSizeBytes = db.CompactBatchSizeBytes
	}

	// Ensure source file exists.
	fi, err := os.Stat(srcPath)
	if os.IsNotExist(err) {
		return fmt.Errorf("file not found '%s'", srcPath)
	} else if err != nil {
		return err
	}

	dstPath := srcPath + ".compact_in_progress"
	os.Remove(dstPath)

	// Open destination database.
	dst, err := Open(dstPath, fi.Mode(), nil)
	if err != nil {
		return err
	}
	skipDstClose := false
	defer func() {
		if !skipDstClose {
			dst.Close()
		}
	}()

	// Run compaction.
	if err := compactInBatches(dst, db, batchSizeBytes); err != nil {
		return err
	}

	// Report stats on new size.
	fi2, err := os.Stat(dstPath)
	if err != nil {
		return err
	} else if fi2.Size() == 0 {
		return fmt.Errorf("zero db size")
	}
	if logImpact {
		log.Printf("Compact() did: %d -> %d bytes (gain=%.2fx)\n", fi.Size(), fi2.Size(), float64(fi.Size())/float64(fi2.Size()))
	}
	dst.Close()
	skipDstClose = true

	// we already hold the db.rwlock, so we can't call db.Close() directly.
	db.metalock.Lock()
	db.mmaplock.RLock()
	db.close()
	db.mmaplock.RUnlock()
	db.metalock.Unlock()

	// now move into place the compacted file.
	err = os.Rename(dstPath, srcPath)

	db2, err2 := reOpen(db, srcPath)
	if db2 != nil {
		cacc := db.CompactAfterCommitCount
		scc := db.successfulCompactionCount
		*db = *db2
		db.CompactAfterCommitCount = cacc
		db.successfulCompactionCount = scc
		skipDbUnlock = true
	}

	if err != nil {
		err = fmt.Errorf("error in Compact() on os.Rename from '%s' to '%s' got err: '%v'", dstPath, srcPath, err)
		if err2 != nil {
			err = fmt.Errorf("%s. And: %s", err.Error(), err2.Error())
		}
		return err
	}
	if err2 != nil {
		return err2
	}
	atomic.AddInt64(&db.successfulCompactionCount, 1)
	return nil
}

func reOpen(orig *DB, srcPath string) (*DB, error) {
	db, err := Open(srcPath, orig.origOpenMode, orig.origOptions)
	if err != nil {
		wd, _ := os.Getwd()
		return nil, fmt.Errorf("Compact() error opening boltdb,"+
			" in use by other process? error detail: '%v' "+
			"upon trying to open path '%s' in cwd '%s'", err, orig.path, wd)
	}
	return db, nil
}

func compactInBatches(dst, src *DB, compactTxMaxSizeBytes int64) error {
	// commit regularly, or we'll run out of
	// memory for large datasets if using one transaction.
	var size int64
	tx, err := dst.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := walk(src, func(keys [][]byte, k, v []byte, seq uint64) error {
		// On each key/value, check if we have exceeded tx size.
		sz := int64(len(k) + len(v))
		if size+sz > compactTxMaxSizeBytes && compactTxMaxSizeBytes != 0 {
			// Commit previous transaction.
			if err := tx.Commit(); err != nil {
				return err
			}

			// Start new transaction.
			tx, err = dst.Begin(true)
			if err != nil {
				return err
			}
			size = 0
		}
		size += sz

		// Create bucket on the root transaction if this is the first level.
		nk := len(keys)
		if nk == 0 {
			bkt, err := tx.CreateBucket(k)
			if err != nil {
				return err
			}
			if err := bkt.SetSequence(seq); err != nil {
				return err
			}
			return nil
		}

		// Create buckets on subsequent levels, if necessary.
		b := tx.Bucket(keys[0])
		if nk > 1 {
			for _, k := range keys[1:] {
				b = b.Bucket(k)
			}
		}

		// If there is no value then this is a bucket call.
		if v == nil {
			bkt, err := b.CreateBucket(k)
			if err != nil {
				return err
			}
			if err := bkt.SetSequence(seq); err != nil {
				return err
			}
			return nil
		}

		// Otherwise treat it as a key/value pair.
		return b.Put(k, v)
	}); err != nil {
		return err
	}

	return tx.Commit()
}

// walkFunc is the type of the function called for keys (buckets and "normal"
// values) discovered by Walk. keys is the list of keys to descend to the bucket
// owning the discovered key/value pair k/v.
type walkFunc func(keys [][]byte, k, v []byte, seq uint64) error

// walk walks recursively the bolt database db, calling walkFn for each key it finds.
func walk(db *DB, walkFn walkFunc) error {
	return db.View(func(tx *Tx) error {
		return tx.ForEach(func(name []byte, buck *Bucket) error {
			return walkBucket(buck, nil, name, nil, buck.Sequence(), walkFn)
		})
	})
}

func walkBucket(buck *Bucket, keypath [][]byte, k, v []byte, seq uint64, fn walkFunc) error {
	// Execute callback.
	if err := fn(keypath, k, v, seq); err != nil {
		return err
	}

	// If this is not a bucket then stop.
	if v != nil {
		return nil
	}

	// Iterate over each child key/value.
	keypath = append(keypath, k)
	return buck.ForEach(func(k, v []byte) error {
		if v == nil {
			bkt := buck.Bucket(k)
			return walkBucket(bkt, keypath, k, nil, bkt.Sequence(), fn)
		}
		return walkBucket(buck, keypath, k, v, buck.Sequence(), fn)
	})
}
