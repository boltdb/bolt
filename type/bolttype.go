/*
Package bolttype provides auto-generated single and paired primitive functions on top of the standard BoltDB library.

The main entry point is the DB struct, which wraps a bolt.DB. A DB
is a drop-in replacement for a bolt.DB, but with additional functions.
The only change needed to be made is to update anonymous functions to
use a bolttype Bucket and Tx instead of a bolt Bucket and Tx.

Types handled so far:

	[]byte
	string
	uint16
	uint32
	uint64
	pair([]byte,[]byte)
	pair(uint16,uint16)
	pair(uint32,uint32)
	pair(uint64,uint64)
	pair(uint16,[]byte)
	pair(uint32,[]byte)
	pair(uint64,[]byte)
	pair([]byte,uint16)
	pair([]byte,uint32)
	pair([]byte,uint64)

There is always a key type and a value type, think of bolt mapping from type1 to type2.

Functions for single types are of the form GetType1ToType2, PutType1ToType2, and so on.
Functions for pair types are of the form GetPairType1PairType2ToType3 and so on.

Functions covered:

	Bucket.Get
	Bucket.Put
	Bucket.ForEach
	Bucket.Delete
	Cursor.First
	Cursor.Last
	Cursor.Next
	Cursor.Prev
	Cursor.Seek

There is also Bucket.GetRequired, which returns the value and an error.
The error will be of type KeyNotFoundError if the key is not found.

Examples:

	GetUint16ToUint16(key uint16) uint16
	GetUint16ToUint32(key uint16) uint16
	GetStringToString(key string) string
	GetRequiredUint16ToUint32(key uint16) (uint16, error)
	PutUint16ToUint32(key uint16, value uint32) error
	GetUint64Uint64ToUint32(key1 uint64, key2 uint64) uint32
	PutUint64Uint64ToUint32(key1 uint64, key2 uint64, value uint32) error
	GetUint64Uint64ToBytes(key1 uint64, key2 uint64) []byte // []byte is special-cased for naming
	GetUint64Uint64ToUint64Bytes(key1 uint64, key2 uint64) (uint64, []byte)
	GetRequiredUint64Uint64ToUint64Bytes(key1 uint64, key2 uint64) (uint64, []byte, error)
	PutUint64Uint64ToUint64Bytes(key1 uint64, key2 uint64, value1 uint64, value2 []byte) error
	ForEachUint16Uint16ToUint64(fn func(key1 uint16, key2 uint16, value uint64) error) error
	ForEachUint16Uint16ToUint64Uint64(fn func(key1 uint16, key2 uint16, value1 uint64, uint64) error) error
	DeleteUint32(key uint32) error
	DeleteUint32Uint32(key1 uint32, key2 uint32) error

See the tests for some basic usage examples.
*/
package bolttype

import (
	"encoding/binary"
	"fmt"
	"os"

	"github.com/boltdb/bolt"
)

// DB wraps a bolt DB.
type DB struct {
	*bolt.DB
}

// Open matches the bolt Open call.
func Open(path string, mode os.FileMode, options *bolt.Options) (*DB, error) {
	boltDB, err := bolt.Open(path, mode, options)
	var db *DB
	if boltDB != nil {
		db = &DB{boltDB}
	}
	return db, err
}

// Batch matches the bolt Batch call.
func (d *DB) Batch(fn func(*Tx) error) error {
	return d.DB.Batch(newTxFunc(fn))
}

// Begin matches the bolt Begin Call.
func (d *DB) Begin(writable bool) (*Tx, error) {
	boltTx, err := d.DB.Begin(writable)
	var tx *Tx
	if boltTx != nil {
		tx = &Tx{boltTx}
	}
	return tx, err
}

// Update matches the bolt Update call.
func (d *DB) Update(fn func(*Tx) error) error {
	return d.DB.Update(newTxFunc(fn))
}

// View matches the bolt View call.
func (d *DB) View(fn func(*Tx) error) error {
	return d.DB.View(newTxFunc(fn))
}

// Tx wraps a bolt Tx.
type Tx struct {
	*bolt.Tx
}

// Bucket matches the bolt Bucket call.
func (t *Tx) Bucket(name []byte) *Bucket {
	boltBucket := t.Tx.Bucket(name)
	if boltBucket == nil {
		return nil
	}
	return &Bucket{boltBucket}
}

// CreateBucket matches the bolt CreateBucket call.
func (t *Tx) CreateBucket(name []byte) (*Bucket, error) {
	boltBucket, err := t.Tx.CreateBucket(name)
	var bucket *Bucket
	if boltBucket != nil {
		bucket = &Bucket{boltBucket}
	}
	return bucket, err
}

// CreateBucketIfNotExists matches the bolt CreateBucketIfNotExists call.
func (t *Tx) CreateBucketIfNotExists(name []byte) (*Bucket, error) {
	boltBucket, err := t.Tx.CreateBucketIfNotExists(name)
	var bucket *Bucket
	if boltBucket != nil {
		bucket = &Bucket{boltBucket}
	}
	return bucket, err
}

// Cursor matches the bolt Cursor call.
func (t *Tx) Cursor() *Cursor {
	boltCursor := t.Tx.Cursor()
	if boltCursor == nil {
		return nil
	}
	return &Cursor{boltCursor}
}

// DB matches the bolt DB call.
func (t *Tx) DB() *DB {
	boltDB := t.Tx.DB()
	if boltDB == nil {
		return nil
	}
	return &DB{boltDB}
}

// ForEach matches the bolt ForEach call.
func (t *Tx) ForEach(fn func(name []byte, b *Bucket) error) error {
	if fn == nil {
		return t.Tx.ForEach(nil)
	}
	return t.Tx.ForEach(
		func(name []byte, boltBucket *bolt.Bucket) error {
			return fn(name, &Bucket{boltBucket})
		},
	)
}

// Cursor wraps a bolt Cursor.
type Cursor struct {
	*bolt.Cursor
}

// Bucket matches the bolt Bucket call.
func (c *Cursor) Bucket() *Bucket {
	boltBucket := c.Cursor.Bucket()
	if boltBucket == nil {
		return nil
	}
	return &Bucket{boltBucket}
}

// Bucket wraps a bolt Bucket.
type Bucket struct {
	*bolt.Bucket
}

// KeyNotFoundError is a key that was not found.
type KeyNotFoundError struct {
	Key []interface{}
}

// NewKeyNotFoundError creates a new KeyNotFoundError.
func NewKeyNotFoundError(key ...interface{}) *KeyNotFoundError {
	return &KeyNotFoundError{Key: key}
}

func (k *KeyNotFoundError) Error() string {
	return fmt.Sprintf("bolttype: key not found: %v", k.Key)
}

func newTxFunc(fn func(*Tx) error) func(*bolt.Tx) error {
	if fn == nil {
		return nil
	}
	return func(boltTx *bolt.Tx) error { return fn(&Tx{boltTx}) }
}

func uint16Tobytes(value uint16) []byte {
	buffer := make([]byte, 2)
	binary.BigEndian.PutUint16(buffer, value)
	return buffer
}

func bytesTouint16(value []byte) uint16 {
	if value == nil {
		return 0
	}
	return binary.BigEndian.Uint16(value)
}

func uint32Tobytes(value uint32) []byte {
	buffer := make([]byte, 4)
	binary.BigEndian.PutUint32(buffer, value)
	return buffer
}

func bytesTouint32(value []byte) uint32 {
	if value == nil {
		return 0
	}
	return binary.BigEndian.Uint32(value)
}

func uint64Tobytes(value uint64) []byte {
	buffer := make([]byte, 8)
	binary.BigEndian.PutUint64(buffer, value)
	return buffer
}

func bytesTouint64(value []byte) uint64 {
	if value == nil {
		return 0
	}
	return binary.BigEndian.Uint64(value)
}

func uint16uint16Tobytes(value1 uint16, value2 uint16) []byte {
	buffer := make([]byte, 4)
	binary.BigEndian.PutUint16(buffer, value1)
	binary.BigEndian.PutUint16(buffer[2:], value2)
	return buffer
}

func bytesTouint16uint16(value []byte) (uint16, uint16) {
	if value == nil {
		return 0, 0
	}
	return binary.BigEndian.Uint16(value[0:2]), binary.BigEndian.Uint16(value[2:4])
}

func uint32uint32Tobytes(value1 uint32, value2 uint32) []byte {
	buffer := make([]byte, 8)
	binary.BigEndian.PutUint32(buffer, value1)
	binary.BigEndian.PutUint32(buffer[4:], value2)
	return buffer
}

func bytesTouint32uint32(value []byte) (uint32, uint32) {
	if value == nil {
		return 0, 0
	}
	return binary.BigEndian.Uint32(value[0:4]), binary.BigEndian.Uint32(value[4:8])
}

func uint64uint64Tobytes(value1 uint64, value2 uint64) []byte {
	buffer := make([]byte, 16)
	binary.BigEndian.PutUint64(buffer, value1)
	binary.BigEndian.PutUint64(buffer[8:], value2)
	return buffer
}

func bytesTouint64uint64(value []byte) (uint64, uint64) {
	if value == nil {
		return 0, 0
	}
	return binary.BigEndian.Uint64(value[0:8]), binary.BigEndian.Uint64(value[8:16])
}

func uint16bytesTobytes(value1 uint16, value2 []byte) []byte {
	buffer := make([]byte, 2+len(value2))
	binary.BigEndian.PutUint16(buffer, value1)
	if len(value2) > 0 {
		copy(buffer[2:], value2)
	}
	return buffer
}

func bytesTouint16bytes(value []byte) (uint16, []byte) {
	if value == nil {
		return 0, nil
	}
	return binary.BigEndian.Uint16(value[0:2]), value[2:]
}

func uint32bytesTobytes(value1 uint32, value2 []byte) []byte {
	buffer := make([]byte, 4+len(value2))
	binary.BigEndian.PutUint32(buffer, value1)
	if len(value2) > 0 {
		copy(buffer[4:], value2)
	}
	return buffer
}

func bytesTouint32bytes(value []byte) (uint32, []byte) {
	if value == nil {
		return 0, nil
	}
	return binary.BigEndian.Uint32(value[0:4]), value[4:]
}

func uint64bytesTobytes(value1 uint64, value2 []byte) []byte {
	buffer := make([]byte, 8+len(value2))
	binary.BigEndian.PutUint64(buffer, value1)
	if len(value2) > 0 {
		copy(buffer[8:], value2)
	}
	return buffer
}

func bytesTouint64bytes(value []byte) (uint64, []byte) {
	if value == nil {
		return 0, nil
	}
	return binary.BigEndian.Uint64(value[0:8]), value[8:]
}

func bytesuint16Tobytes(value1 []byte, value2 uint16) []byte {
	buffer := make([]byte, 2+len(value1))
	if len(value1) > 0 {
		copy(buffer, value1)
	}
	binary.BigEndian.PutUint16(buffer[len(value1):], value2)
	return buffer
}

func bytesTobytesuint16(value []byte) ([]byte, uint16) {
	if value == nil {
		return nil, 0
	}
	return value[len(value)-2:], binary.BigEndian.Uint16(value[len(value)-2:])
}

func bytesuint32Tobytes(value1 []byte, value2 uint32) []byte {
	buffer := make([]byte, 4+len(value1))
	if len(value1) > 0 {
		copy(buffer, value1)
	}
	binary.BigEndian.PutUint32(buffer[len(value1):], value2)
	return buffer
}

func bytesTobytesuint32(value []byte) ([]byte, uint32) {
	if value == nil {
		return nil, 0
	}
	return value[len(value)-4:], binary.BigEndian.Uint32(value[len(value)-4:])
}

func bytesuint64Tobytes(value1 []byte, value2 uint64) []byte {
	buffer := make([]byte, 8+len(value1))
	if len(value1) > 0 {
		copy(buffer, value1)
	}
	binary.BigEndian.PutUint64(buffer[len(value1):], value2)
	return buffer
}

func bytesTobytesuint64(value []byte) ([]byte, uint64) {
	if value == nil {
		return nil, 0
	}
	return value[len(value)-8:], binary.BigEndian.Uint64(value[len(value)-8:])
}
