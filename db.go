package bolt

import (
	"fmt"
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
// All the functions on DB will return a ErrDatabaseNotOpen if accessed before Open() is called.
type DB struct {
	path     string
	file     *os.File
	metafile *os.File
	data     []byte
	meta0    *meta
	meta1    *meta
	pageSize int
	opened   bool
	rwtx     *Tx
	txs      []*Tx
	freelist *freelist

	rwlock   sync.Mutex   // Allows only one writer at a time.
	metalock sync.Mutex   // Protects meta page access.
	mmaplock sync.RWMutex // Protects mmap access during remapping.
}

// Path returns the path to currently open database file.
func (db *DB) Path() string {
	return db.path
}

// GoString returns the Go string representation of the database.
func (db *DB) GoString() string {
	return fmt.Sprintf("bolt.DB{path:%q}", db.path)
}

// String returns the string representation of the database.
func (db *DB) String() string {
	return fmt.Sprintf("DB<%q>", db.path)
}

// Open opens a data file at the given path and initializes the database.
// If the file does not exist then it will be created automatically.
func (db *DB) Open(path string, mode os.FileMode) error {
	var err error
	db.metalock.Lock()
	defer db.metalock.Unlock()

	// Exit if the database is currently open.
	if db.opened {
		return ErrDatabaseOpen
	}

	// Open data file and separate sync handler for metadata writes.
	db.path = path
	if db.file, err = os.OpenFile(db.path, os.O_RDWR|os.O_CREATE, mode); err != nil {
		db.close()
		return err
	}
	if db.metafile, err = os.OpenFile(db.path, os.O_RDWR|os.O_SYNC, mode); err != nil {
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
	db.freelist = &freelist{pending: make(map[txid][]pgid)}
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
	if db.rwtx != nil {
		db.rwtx.dereference()
	}

	// Unmap existing data before continuing.
	if err := db.munmap(); err != nil {
		return err
	}

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
	size = db.mmapSize(size)

	// Memory-map the data file as a byte slice.
	if db.data, err = syscall.Mmap(int(db.file.Fd()), 0, size, syscall.PROT_READ, syscall.MAP_SHARED); err != nil {
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
func (db *DB) munmap() error {
	if db.data != nil {
		if err := syscall.Munmap(db.data); err != nil {
			return fmt.Errorf("unmap error: " + err.Error())
		}
		db.data = nil
	}
	return nil
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
	db.pageSize = os.Getpagesize()

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
		m.txid = txid(i)
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
func (db *DB) Close() error {
	db.metalock.Lock()
	defer db.metalock.Unlock()
	return db.close()
}

func (db *DB) close() error {
	db.opened = false

	db.freelist = nil
	db.path = ""

	// Close the mmap.
	if err := db.munmap(); err != nil {
		return err
	}

	// Close file handles.
	if db.file != nil {
		if err := db.file.Close(); err != nil {
			return fmt.Errorf("db file close error: %s", err)
		}
		db.file = nil
	}
	if db.metafile != nil {
		if err := db.metafile.Close(); err != nil {
			return fmt.Errorf("db metafile close error: %s", err)
		}
		db.metafile = nil
	}

	return nil
}

// Tx creates a read-only transaction.
// Multiple read-only transactions can be used concurrently.
//
// IMPORTANT: You must close the transaction after you are finished or else the database will not reclaim old pages.
func (db *DB) Tx() (*Tx, error) {
	db.metalock.Lock()
	defer db.metalock.Unlock()

	// Obtain a read-only lock on the mmap. When the mmap is remapped it will
	// obtain a write lock so all transactions must finish before it can be
	// remapped.
	db.mmaplock.RLock()

	// Exit if the database is not open yet.
	if !db.opened {
		return nil, ErrDatabaseNotOpen
	}

	// Create a transaction associated with the database.
	t := &Tx{}
	t.init(db)

	// Keep track of transaction until it closes.
	db.txs = append(db.txs, t)

	return t, nil
}

// RWTx creates a read/write transaction.
// Only one read/write transaction is allowed at a time.
// You must call Commit() or Rollback() on the transaction to close it.
func (db *DB) RWTx() (*Tx, error) {
	db.metalock.Lock()
	defer db.metalock.Unlock()

	// Obtain writer lock. This is released by the transaction when it closes.
	db.rwlock.Lock()

	// Exit if the database is not open yet.
	if !db.opened {
		db.rwlock.Unlock()
		return nil, ErrDatabaseNotOpen
	}

	// Create a transaction associated with the database.
	t := &Tx{writable: true}
	t.init(db)
	db.rwtx = t

	// Free any pages associated with closed read-only transactions.
	var minid txid = 0xFFFFFFFFFFFFFFFF
	for _, t := range db.txs {
		if t.id() < minid {
			minid = t.id()
		}
	}
	if minid > 0 {
		db.freelist.release(minid - 1)
	}

	return t, nil
}

// removeTx removes a transaction from the database.
func (db *DB) removeTx(t *Tx) {
	db.metalock.Lock()
	defer db.metalock.Unlock()

	// Release the read lock on the mmap.
	db.mmaplock.RUnlock()

	// Remove the transaction.
	for i, tx := range db.txs {
		if tx == t {
			db.txs = append(db.txs[:i], db.txs[i+1:]...)
			break
		}
	}
}

// Do executes a function within the context of a read-write transaction.
// If no error is returned from the function then the transaction is committed.
// If an error is returned then the entire transaction is rolled back.
// Any error that is returned from the function or returned from the commit is
// returned from the Do() method.
func (db *DB) Do(fn func(*Tx) error) error {
	t, err := db.RWTx()
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

// With executes a function within the context of a transaction.
// Any error that is returned from the function is returned from the With() method.
func (db *DB) With(fn func(*Tx) error) error {
	t, err := db.Tx()
	if err != nil {
		return err
	}
	defer t.Rollback()

	// If an error is returned from the function then pass it through.
	return fn(t)
}

// Copy writes the entire database to a writer.
// A reader transaction is maintained during the copy so it is safe to continue
// using the database while a copy is in progress.
func (db *DB) Copy(w io.Writer) error {
	// Maintain a reader transaction so pages don't get reclaimed.
	t, err := db.Tx()
	if err != nil {
		return err
	}
	defer t.Rollback()

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

// Stat retrieves stats on the database and its page usage.
// Returns an error if the database is not open.
func (db *DB) Stat() (*Stat, error) {
	// Obtain meta & mmap locks.
	db.metalock.Lock()
	db.mmaplock.RLock()

	var s = &Stat{
		MmapSize: len(db.data),
		TxCount:  len(db.txs),
	}

	// Release locks.
	db.mmaplock.RUnlock()
	db.metalock.Unlock()

	err := db.Do(func(t *Tx) error {
		s.PageCount = int(t.meta.pgid)
		s.FreePageCount = len(db.freelist.all())
		s.PageSize = db.pageSize
		return nil
	})
	if err != nil {
		return nil, err
	}
	return s, nil
}

// page retrieves a page reference from the mmap based on the current page size.
func (db *DB) page(id pgid) *page {
	pos := id * pgid(db.pageSize)
	return (*page)(unsafe.Pointer(&db.data[pos]))
}

// pageInBuffer retrieves a page reference from a given byte array based on the current page size.
func (db *DB) pageInBuffer(b []byte, id pgid) *page {
	return (*page)(unsafe.Pointer(&b[id*pgid(db.pageSize)]))
}

// meta retrieves the current meta page reference.
func (db *DB) meta() *meta {
	if db.meta0.txid > db.meta1.txid {
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
	p.id = db.rwtx.meta.pgid
	var minsz = int((p.id+pgid(count))+1) * db.pageSize
	if minsz >= len(db.data) {
		if err := db.mmap(minsz); err != nil {
			return nil, &Error{"mmap allocate error", err}
		}
	}

	// Move the page id high water mark.
	db.rwtx.meta.pgid += pgid(count)

	return p, nil
}

// Stat represents stats on the database such as free pages and sizes.
type Stat struct {
	// PageCount is the total number of allocated pages. This is a high water
	// mark in the database that represents how many pages have actually been
	// used. This will be smaller than the MmapSize / PageSize.
	PageCount int

	// FreePageCount is the total number of pages which have been previously
	// allocated but are no longer used.
	FreePageCount int

	// PageSize is the size, in bytes, of individual database pages.
	PageSize int

	// MmapSize is the mmap-allocated size of the data file. When the data file
	// grows beyond this size, the database will obtain a lock on the mmap and
	// resize it.
	MmapSize int

	// TxCount is the total number of reader transactions.
	TxCount int
}
