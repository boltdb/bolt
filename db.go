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

	// Read enough data to get both meta pages.
	var m, m0, m1 *meta
	var buf [minPageSize]byte
	if _, err := db.file.ReadAt(buf[:], 0); err == nil {
		if m0, _ = db.pageInBuffer(buf[:], 0).meta(); m0 != nil {
			db.pageSize = int(m0.pageSize)
		}
	}
	if _, err := db.file.ReadAt(buf[:], int64(db.pageSize)); err == nil {
		m1, _ = db.pageInBuffer(buf[:], 0).meta()
	}
	if m0 != nil && m1 != nil {
		if m0.txnid > m1.txnid {
			m = m0
		} else {
			m = m1
		}
	}

	// Initialize the page size for new environments.
	if m == nil {
		if err := db.init(); err != nil {
			return err
		}
	}

	// Memory map the data file.
	if err := db.mmap(); err != nil {
		db.close()
		return err
	}

	// TODO: Initialize meta.
	// if (newenv) {
	//   i = mdb_env_init_meta(env, &meta);
	//   if (i != MDB_SUCCESS) {
	//     return i;
	//   }
	// }

	// Mark the database as opened and return.
	db.opened = true
	return nil
}

// mmap opens the underlying memory-mapped file and initializes the meta references.
func (db *DB) mmap() error {
	var err error

	// Determine the map size based on the file size.
	var size int
	if info, err := db.file.Stat(); err != nil {
		return err
	} else if info.Size() < int64(db.pageSize*2) {
		return &Error{"file size too small", nil}
	} else {
		size = int(info.Size())
	}

	// Memory-map the data file as a byte slice.
	if db.data, err = db.syscall.Mmap(int(db.file.Fd()), 0, size, syscall.PROT_READ, syscall.MAP_SHARED); err != nil {
		return err
	}

	// Save references to the meta pages.
	if db.meta0, err = db.page(0).meta(); err != nil {
		return &Error{"meta0 error", err}
	}
	if db.meta1, err = db.page(1).meta(); err != nil {
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
		p.init(db.pageSize)
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
