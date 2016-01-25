Package bolttype provides auto-generated single and paired primitive functions on top of the standard BoltDB library.

The main entry point is the DB struct, which wraps a bolt.DB. A DB
is a drop-in replacement for a bolt.DB, but with additional functions.
The only change needed to be made is to update anonymous functions to
use a bolttype Bucket and Tx instead of a bolt Bucket and Tx.

Types handled so far:

* `[]byte`
* `string`
* `uint16`
* `uint32`
* `uint64`
* `pair(uint16,uint16)`
* `pair(uint32,uint32)`
* `pair(uint64,uint64)`
* `pair(uint16,[]byte)`
* `pair(uint32,[]byte)`
* `pair(uint64,[]byte)`
* `pair([]byte,uint16)`
* `pair([]byte,uint32)`
* `pair([]byte,uint64)`

There is always a key type and a value type, think of bolt mapping from type1 to type2.

Functions for single types are of the form `GetType1ToType2`, `PutType1ToType2`, and so on.  
Functions for pair types are of the form `GetPairType1PairType2ToType3` and so on.

Functions covered:

* `Bucket.Get`
* `Bucket.Put`
* `Bucket.ForEach`
* `Bucket.Delete`
* `Cursor.First`
* `Cursor.Last`
* `Cursor.Next`
* `Cursor.Prev`
* `Cursor.Seek`

There is also `Bucket.GetRequired`, which returns the value and an error.
The error will be of type `KeyNotFoundError` if the key is not found.

Examples:

* `GetUint16ToUint16(key uint16) uint16`
* `GetUint16ToUint32(key uint16) uint16`
* `GetStringToString(key string) string`
* `GetRequiredUint16ToUint32(key uint16) (uint16, error)`
* `PutUint16ToUint32(key uint16, value uint32) error`
* `GetUint64Uint64ToUint32(key1 uint64, key2 uint64) uint32`
* `PutUint64Uint64ToUint32(key1 uint64, key2 uint64, value uint32) error`
* `GetUint64Uint64ToBytes(key1 uint64, key2 uint64) []byte // []byte is special-cased for naming` 
* `GetUint64Uint64ToUint64Bytes(key1 uint64, key2 uint64) (uint64, []byte)`
* `GetRequiredUint64Uint64ToUint64Bytes(key1 uint64, key2 uint64) (uint64, []byte, error)`
* `PutUint64Uint64ToUint64Bytes(key1 uint64, key2 uint64, value1 uint64, value2 []byte) error`
* `ForEachUint16Uint16ToUint64(fn func(key1 uint16, key2 uint16, value uint64) error) error`
* `ForEachUint16Uint16ToUint64Uint64(fn func(key1 uint16, key2 uint16, value1 uint64, uint64) error) error`
* `DeleteUint32(key uint32) error`
* `DeleteUint32Uint32(key1 uint32, key2 uint32) error`

See the tests for some basic usage examples.
