/*
Package bolt implements a low-level key/value store in pure Go. It supports
fully serializable transactions, ACID semantics, and lock-free MVCC with
multiple readers and a single writer. Bolt can be used for projects that
want a simple data store without the need to add large dependencies such as
Postgres or MySQL.

Bolt is a single-level, zero-copy, B+tree data store. This means that Bolt is
optimized for fast read access and does not require recovery in the event of a
system crash. Transactions which have not finished committing will simply be
rolled back in the event of a crash.

The design of Bolt is based on Howard Chu's LMDB database project.

Basics

There are only a few types in Bolt: DB, Bucket, Tx, RWTx, and Cursor. The DB is
a collection of buckets and is represented by a single file on disk. A bucket is
a collection of unique keys that are associated with values.

Txs provide read-only access to data inside the database. They can retrieve
key/value pairs and can use Cursors to iterate over the entire dataset. RWTxs
provide read-write access to the database. They can create and delete buckets
and they can insert and remove keys. Only one RWTx is allowed at a time.


Caveats

The database uses a read-only, memory-mapped data file to ensure that
applications cannot corrupt the database, however, this means that keys and
values returned from Bolt cannot be changed. Writing to a read-only byte slice
will cause Go to panic. If you need to work with data returned from a Get() you
need to first copy it to a new byte slice.

Bolt currently works on Mac OS and Linux. Windows support is coming soon.

*/
package bolt
