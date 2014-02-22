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

The design of Bolt is based on Howard Chu's LMDB project.

Basics

There are only a few types in Bolt: DB, Bucket, Transaction, and Cursor. The DB
is a collection of buckets and is represented by a single file on disk. A
bucket is a collection of unique keys that are associated with values.

Transactions provide a consistent view of the database. They can be used for
retrieving, setting, and deleting properties. They can also be used to iterate
over all the values in a bucket. Only one writer Transaction can be in use at
a time.


Caveats

The database uses a read-only, memory-mapped data file to ensure that
applications cannot corrupt the database, however, this means that keys and
values returned from Bolt cannot be changed. Writing to a read-only byte slice
will cause Go to panic. If you need to alter data returned from a Transaction
you need to first copy it to a new byte slice.

Bolt currently works on Mac OS and Linux. Windows support is coming soon.

*/
package bolt
