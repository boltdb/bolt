package bolt

import (
	"io"
	"os"
	"sync"
	"syscall"
	"unsafe"
)

// The smallest size that the mmap can be.
const minMmapSize = 1 << 22 // 4MB

// The largest step that can be taken when remapping the mmap.
const maxMmapStep = 1 << 30 // 1GB

// DB represents a collection of buckets persisted to a file on disk.
// All data access is performed through transactions which can be obtained through the DB.
// All the functions on DB will return a DatabaseNotOpenError if accessed before Open() is called.
type DB struct {
	os            _os
	syscall       _syscall
	path          string
	file          file
	metafile      file
	data          []byte
	meta0         *meta
	meta1         *meta
	pageSize      int
	opened        bool
	rwtransaction *RWTransaction
	transactions  []*Transaction
	freelist      *freelist

	rwlock   sync.Mutex   // Allows only one writer at a time.
	metalock sync.Mutex   // Protects meta page access.
	mmaplock sync.RWMutex // Protects mmap access during remapping.
}

// Path returns the path to currently open database file.
func (db *DB) Path() string {
	return db.path
}

// Open opens a data file at the given path and initializes the database.
// If the file does not exist then it will be created automatically.
func (db *DB) Open(path string, mode os.FileMode) error {
	var err error
	db.metalock.Lock()
	defer db.metalock.Unlock()

	// Initialize OS/Syscall references.
	// These are overridden by mocks during some tests.
	if db.os == nil {
		db.os = &sysos{}
	}
	if db.syscall == nil {
		db.syscall = &syssyscall{}
	}

	// Exit if the database is currently open.
	if db.opened {
		return DatabaseOpenError
	}

	// Open data file and separate sync handler for metadata writes.
	db.path = path
	if db.file, err = db.os.OpenFile(db.path, os.O_RDWR|os.O_CREATE, mode); err != nil {
		db.close()
		return err
	}
	if db.metafile, err = db.os.OpenFile(db.path, os.O_RDWR|os.O_SYNC, mode); err != nil {
		db.close()
		return err
	}

	// Initialize the database if it doesn't exist.
	if info, err := db.file.Stat(); err != nil {
		return &Error{"stat error", err}
	} else if info.Size() == 0 {
		// Initialize new files with meta pages.
		if err := db.init(); err != nil {
			return err
		}
	} else {
		// Read the first meta page to determine the page size.
		var buf [0x1000]byte
		if _, err := db.file.ReadAt(buf[:], 0); err == nil {
			m := db.pageInBuffer(buf[:], 0).meta()
			if err := m.validate(); err != nil {
				return &Error{"meta error", err}
			}
			db.pageSize = int(m.pageSize)
		}
	}

	// Memory map the data file.
	if err := db.mmap(0); err != nil {
		db.close()
		return err
	}

	// Read in the freelist.
	db.freelist = &freelist{pending: make(map[txnid][]pgid)}
	db.freelist.read(db.page(db.meta().freelist))

	// Mark the database as opened and return.
	db.opened = true
	return nil
}

// mmap opens the underlying memory-mapped file and initializes the meta references.
// minsz is the minimum size that the new mmap can be.
func (db *DB) mmap(minsz int) error {
	db.mmaplock.Lock()
	defer db.mmaplock.Unlock()

	// Dereference all mmap references before unmapping.
	if db.rwtransaction != nil {
		db.rwtransaction.dereference()
	}

	// Unmap existing data before continuing.
	db.munmap()

	info, err := db.file.Stat()
	if err != nil {
		return &Error{"mmap stat error", err}
	} else if int(info.Size()) < db.pageSize*2 {
		return &Error{"file size too small", err}
	}

	// Ensure the size is at least the minimum size.
	var size = int(info.Size())
	if size < minsz {
		size = minsz
	}
	size = db.mmapSize(minsz)

	// Memory-map the data file as a byte slice.
	if db.data, err = db.syscall.Mmap(int(db.file.Fd()), 0, size, syscall.PROT_READ, syscall.MAP_SHARED); err != nil {
		return err
	}

	// Save references to the meta pages.
	db.meta0 = db.page(0).meta()
	db.meta1 = db.page(1).meta()

	// Validate the meta pages.
	if err := db.meta0.validate(); err != nil {
		return &Error{"meta0 error", err}
	}
	if err := db.meta1.validate(); err != nil {
		return &Error{"meta1 error", err}
	}

	return nil
}

// munmap unmaps the data file from memory.
func (db *DB) munmap() {
	if db.data != nil {
		if err := db.syscall.Munmap(db.data); err != nil {
			panic("unmap error: " + err.Error())
		}
		db.data = nil
	}
}

// mmapSize determines the appropriate size for the mmap given the current size
// of the database. The minimum size is 4MB and doubles until it reaches 1GB.
func (db *DB) mmapSize(size int) int {
	if size < minMmapSize {
		return minMmapSize
	} else if size < maxMmapStep {
		size *= 2
	} else {
		size += maxMmapStep
	}

	// Ensure that the mmap size is a multiple of the page size.
	if (size % db.pageSize) != 0 {
		size = ((size / db.pageSize) + 1) * db.pageSize
	}

	return size
}

// init creates a new database file and initializes its meta pages.
func (db *DB) init() error {
	// Set the page size to the OS page size.
	db.pageSize = db.os.Getpagesize()

	// Create two meta pages on a buffer.
	buf := make([]byte, db.pageSize*4)
	for i := 0; i < 2; i++ {
		p := db.pageInBuffer(buf[:], pgid(i))
		p.id = pgid(i)
		p.flags = metaPageFlag

		// Initialize the meta page.
		m := p.meta()
		m.magic = magic
		m.version = version
		m.pageSize = uint32(db.pageSize)
		m.version = version
		m.freelist = 2
		m.buckets = 3
		m.pgid = 4
		m.txnid = txnid(i)
	}

	// Write an empty freelist at page 3.
	p := db.pageInBuffer(buf[:], pgid(2))
	p.id = pgid(2)
	p.flags = freelistPageFlag
	p.count = 0

	// Write an empty leaf page at page 4.
	p = db.pageInBuffer(buf[:], pgid(3))
	p.id = pgid(3)
	p.flags = bucketsPageFlag
	p.count = 0

	// Write the buffer to our data file.
	if _, err := db.metafile.WriteAt(buf, 0); err != nil {
		return err
	}

	return nil
}

// Close releases all database resources.
// All transactions must be closed before closing the database.
func (db *DB) Close() {
	db.metalock.Lock()
	defer db.metalock.Unlock()
	db.close()
}

func (db *DB) close() {
	db.opened = false
	
	// TODO(benbjohnson): Undo everything in Open().
	db.freelist = nil
	db.path = ""

	db.munmap()
}

// Transaction creates a read-only transaction.
// Multiple read-only transactions can be used concurrently.
//
// IMPORTANT: You must close the transaction after you are finished or else the database will not reclaim old pages.
func (db *DB) Transaction() (*Transaction, error) {
	db.metalock.Lock()
	defer db.metalock.Unlock()

	// Obtain a read-only lock on the mmap. When the mmap is remapped it will
	// obtain a write lock so all transactions must finish before it can be
	// remapped.
	db.mmaplock.RLock()

	// Exit if the database is not open yet.
	if !db.opened {
		return nil, DatabaseNotOpenError
	}

	// Create a transaction associated with the database.
	t := &Transaction{}
	t.init(db)

	// Keep track of transaction until it closes.
	db.transactions = append(db.transactions, t)

	return t, nil
}

// RWTransaction creates a read/write transaction.
// Only one read/write transaction is allowed at a time.
// You must call Commit() or Rollback() on the transaction to close it.
func (db *DB) RWTransaction() (*RWTransaction, error) {
	db.metalock.Lock()
	defer db.metalock.Unlock()

	// Obtain writer lock. This is released by the RWTransaction when it closes.
	db.rwlock.Lock()

	// Exit if the database is not open yet.
	if !db.opened {
		db.rwlock.Unlock()
		return nil, DatabaseNotOpenError
	}

	// Create a transaction associated with the database.
	t := &RWTransaction{nodes: make(map[pgid]*node)}
	t.init(db)
	db.rwtransaction = t

	// Free any pages associated with closed read-only transactions.
	var minid txnid = 0xFFFFFFFFFFFFFFFF
	for _, t := range db.transactions {
		if t.id() < minid {
			minid = t.id()
		}
	}
	if minid > 0 {
		db.freelist.release(minid - 1)
	}

	return t, nil
}

// removeTransaction removes a transaction from the database.
func (db *DB) removeTransaction(t *Transaction) {
	db.metalock.Lock()
	defer db.metalock.Unlock()

	// Release the read lock on the mmap.
	db.mmaplock.RUnlock()

	// Remove the transaction.
	for i, txn := range db.transactions {
		if txn == t {
			db.transactions = append(db.transactions[:i], db.transactions[i+1:]...)
			break
		}
	}
}

// Do executes a function within the context of a RWTransaction.
// If no error is returned from the function then the transaction is committed.
// If an error is returned then the entire transaction is rolled back.
// Any error that is returned from the function or returned from the commit is
// returned from the Do() method.
func (db *DB) Do(fn func(*RWTransaction) error) error {
	t, err := db.RWTransaction()
	if err != nil {
		return err
	}

	// If an error is returned from the function then rollback and return error.
	if err := fn(t); err != nil {
		t.Rollback()
		return err
	}

	return t.Commit()
}

// Bucket retrieves a reference to a bucket.
// This is typically useful for checking the existence of a bucket.
func (db *DB) Bucket(name string) (*Bucket, error) {
	t, err := db.Transaction()
	if err != nil {
		return nil, err
	}
	defer t.Close()
	return t.Bucket(name), nil
}

// Buckets retrieves a list of all buckets in the database.
func (db *DB) Buckets() ([]*Bucket, error) {
	t, err := db.Transaction()
	if err != nil {
		return nil, err
	}
	defer t.Close()
	return t.Buckets(), nil
}

// CreateBucket creates a new bucket with the given name.
// This function can return an error if the bucket already exists, if the name
// is blank, or the bucket name is too long.
func (db *DB) CreateBucket(name string) error {
	return db.Do(func(t *RWTransaction) error {
		return t.CreateBucket(name)
	})
}

// DeleteBucket removes a bucket from the database.
// Returns an error if the bucket does not exist.
func (db *DB) DeleteBucket(name string) error {
	return db.Do(func(t *RWTransaction) error {
		return t.DeleteBucket(name)
	})
}

// NextSequence returns an autoincrementing integer for the bucket.
// This function can return an error if the bucket does not exist.
func (db *DB) NextSequence(name string) (int, error) {
	var seq int
	err := db.Do(func(t *RWTransaction) error {
		var err error
		seq, err = t.NextSequence(name)
		return err
	})
	if err != nil {
		return 0, err
	}
	return seq, nil
}

// Get retrieves the value for a key in a bucket.
// Returns an error if the key does not exist.
func (db *DB) Get(name string, key []byte) ([]byte, error) {
	t, err := db.Transaction()
	if err != nil {
		return nil, err
	}
	defer t.Close()
	return t.Get(name, key)
}

// Put sets the value for a key in a bucket.
// Returns an error if the bucket is not found, if key is blank, if the key is too large, or if the value is too large.
func (db *DB) Put(name string, key []byte, value []byte) error {
	return db.Do(func(t *RWTransaction) error {
		return t.Put(name, key, value)
	})
}

// Delete removes a key from a bucket.
// Returns an error if the bucket cannot be found.
func (db *DB) Delete(name string, key []byte) error {
	return db.Do(func(t *RWTransaction) error {
		return t.Delete(name, key)
	})
}

// Copy writes the entire database to a writer.
// A reader transaction is maintained during the copy so it is safe to continue
// using the database while a copy is in progress.
func (db *DB) Copy(w io.Writer) error {
	if !db.opened {
		return DatabaseNotOpenError
	}

	// Maintain a reader transaction so pages don't get reclaimed.
	t, err := db.Transaction()
	if err != nil {
		return err
	}
	defer t.Close()

	// Open reader on the database.
	f, err := os.Open(db.path)
	if err != nil {
		return err
	}
	defer f.Close()

	// Copy everything.
	if _, err := io.Copy(w, f); err != nil {
		return err
	}
	return nil
}

// CopyFile copies the entire database to file at the given path.
// A reader transaction is maintained during the copy so it is safe to continue
// using the database while a copy is in progress.
func (db *DB) CopyFile(path string, mode os.FileMode) error {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, mode)
	if err != nil {
		return err
	}
	defer f.Close()

	return db.Copy(f)
}

// page retrieves a page reference from the mmap based on the current page size.
func (db *DB) page(id pgid) *page {
	return (*page)(unsafe.Pointer(&db.data[id*pgid(db.pageSize)]))
}

// pageInBuffer retrieves a page reference from a given byte array based on the current page size.
func (db *DB) pageInBuffer(b []byte, id pgid) *page {
	return (*page)(unsafe.Pointer(&b[id*pgid(db.pageSize)]))
}

// meta retrieves the current meta page reference.
func (db *DB) meta() *meta {
	if db.meta0.txnid > db.meta1.txnid {
		return db.meta0
	}
	return db.meta1
}

// allocate returns a contiguous block of memory starting at a given page.
func (db *DB) allocate(count int) (*page, error) {
	// Allocate a temporary buffer for the page.
	buf := make([]byte, count*db.pageSize)
	p := (*page)(unsafe.Pointer(&buf[0]))
	p.overflow = uint32(count - 1)

	// Use pages from the freelist if they are available.
	if p.id = db.freelist.allocate(count); p.id != 0 {
		return p, nil
	}

	// Resize mmap() if we're at the end.
	p.id = db.rwtransaction.meta.pgid
	var minsz = int((p.id+pgid(count))+1) * db.pageSize
	if minsz >= len(db.data) {
		if err := db.mmap(minsz); err != nil {
			return nil, &Error{"mmap allocate error", err}
		}
	}

	// Move the page id high water mark.
	db.rwtransaction.meta.pgid += pgid(count)

	return p, nil
}

// sync flushes the file descriptor to disk.
func (db *DB) sync(force bool) error {
	if db.opened {
		return DatabaseNotOpenError
	}
	if err := syscall.Fsync(int(db.file.Fd())); err != nil {
		return err
	}
	return nil
}
