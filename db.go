package bolt

import (
	"os"
	"sync"
	"syscall"
	"unsafe"
)

const (
	db_nosync = iota
	db_nometasync
)

const minPageSize = 0x1000

var (
	DatabaseNotOpenError       = &Error{"db is not open", nil}
	DatabaseAlreadyOpenedError = &Error{"db already open", nil}
	TransactionInProgressError = &Error{"writable transaction is already in progress", nil}
)

type DB struct {
	sync.Mutex
	opened bool

	os            _os
	syscall       _syscall
	path          string
	file          file
	metafile      file
	data          []byte
	meta0         *meta
	meta1         *meta
	pageSize      int
	rwtransaction *RWTransaction
	transactions  []*Transaction
}

// NewDB creates a new DB instance.
func NewDB() *DB {
	return &DB{}
}

// Path returns the path to currently open database file.
func (db *DB) Path() string {
	return db.path
}

// Open opens a data file at the given path and initializes the database.
// If the file does not exist then it will be created automatically.
func (db *DB) Open(path string, mode os.FileMode) error {
	var err error
	db.Lock()
	defer db.Unlock()

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
		return DatabaseAlreadyOpenedError
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
		var buf [minPageSize]byte
		if _, err := db.file.ReadAt(buf[:], 0); err == nil {
			m := db.pageInBuffer(buf[:], 0).meta()
			if err := m.validate(); err != nil {
				return &Error{"meta error", err}
			}
			db.pageSize = int(m.pageSize)
		}
	}

	// Memory map the data file.
	if err := db.mmap(); err != nil {
		db.close()
		return err
	}

	// Mark the database as opened and return.
	db.opened = true
	return nil
}

// mmap opens the underlying memory-mapped file and initializes the meta references.
func (db *DB) mmap() error {
	info, err := db.file.Stat()
	if err != nil {
		return &Error{"mmap stat error", err}
	} else if int(info.Size()) < db.pageSize*2 {
		return &Error{"file size too small", err}
	}
	size := int(info.Size())

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

// init creates a new database file and initializes its meta pages.
func (db *DB) init() error {
	// Set the page size to the OS page size.
	db.pageSize = db.os.Getpagesize()

	// Create two meta pages on a buffer.
	buf := make([]byte, db.pageSize*2)
	for i := 0; i < 2; i++ {
		p := db.pageInBuffer(buf[:], pgid(i))
		p.id = pgid(i)
		p.flags = p_meta

		m := p.meta()
		m.magic = magic
		m.version = Version
		m.pageSize = uint32(db.pageSize)
	}

	// Write the buffer to our data file.
	if _, err := db.metafile.WriteAt(buf, 0); err != nil {
		return err
	}

	return nil
}

// Close releases all resources related to the database.
func (db *DB) Close() {
	db.Lock()
	defer db.Unlock()
	db.close()
}

func (db *DB) close() {
	// TODO: Undo everything in Open().
}

// Transaction creates a read-only transaction.
// Multiple read-only transactions can be used concurrently.
func (db *DB) Transaction() (*Transaction, error) {
	db.Lock()
	defer db.Unlock()

	// Exit if the database is not open yet.
	if !db.opened {
		return nil, DatabaseNotOpenError
	}

	// Create a transaction associated with the database.
	t := &Transaction{}
	t.init(db, db.meta())

	return t, nil
}

// RWTransaction creates a read/write transaction.
// Only one read/write transaction is allowed at a time.
func (db *DB) RWTransaction() (*RWTransaction, error) {
	db.Lock()
	defer db.Unlock()

	// TODO: db.writerMutex.Lock()
	// TODO: Add unlock to RWTransaction.Commit() / Abort()

	// Exit if the database is not open yet.
	if !db.opened {
		return nil, DatabaseNotOpenError
	}

	// Create a transaction associated with the database.
	t := &RWTransaction{}
	t.init(db, db.meta())

	return t, nil
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

// sync flushes the file descriptor to disk.
func (db *DB) sync(force bool) error {
	if err := syscall.Fsync(int(db.file.Fd())); err != nil {
		return err
	}
	return nil
}

func (db *DB) Stat() *Stat {
	// TODO: Calculate size, depth, page count (by type), entry count, readers, etc.
	return nil
}
