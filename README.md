Bolt [![Build Status](https://drone.io/github.com/boltdb/bolt/status.png)](https://drone.io/github.com/boltdb/bolt/latest) [![Coverage Status](https://coveralls.io/repos/boltdb/bolt/badge.png?branch=master)](https://coveralls.io/r/boltdb/bolt?branch=master) ![Project status](http://img.shields.io/status/pre-alpha.png?color=red)
====

## Overview

Bolt is a pure Go key/value store inspired by [Howard Chu](https://twitter.com/hyc_symas) and the [LMDB project](http://symas.com/mdb/). The goal of the project is to provide a simple, fast, and reliable database for projects that don't require a full database server such as Postgres or MySQL. It is also meant to be educational. Most of us use tools without understanding how the underlying data really works.

Since Bolt is meant to be used as such a low-level piece of functionality, simplicity is key. The API will be small and only center around getting values and setting values. That's it. If you want to see additional functionality added then we encourage you submit a Github issue and we can discuss developing it as a separate fork.

> Simple is the new beautiful.
>
> — [Tobias Lütke](https://twitter.com/tobi)


## Project Status

Bolt is currently in development and is currently not functional.


## Comparing Bolt vs LMDB

Bolt is inspired by [LMDB](http://symas.com/mdb/) so there are many similarities between the two:

1. Both use a [B+Tree](http://en.wikipedia.org/wiki/B%2B_tree) data structure.

2. ACID semantics with fully [serializable transactions](http://en.wikipedia.org/wiki/Isolation_(database_systems)#Serializable).

3. Lock-free MVCC support using a single writer and multiple readers.


There are also several differences between Bolt and LMDB:

1. LMDB supports more additional features such as multi-value keys, fixed length keys, multi-key insert, direct writes, and bi-directional cursors. Bolt only supports basic `Get()`, `Put()`, and `Delete()` operations and unidirectional cursors.

2. LMDB databases can be shared between processes. Bolt only allows a single process to use a database at a time.

3. LMDB is written in C and extremely fast. Bolt is written in pure Go and, while it's fast, it is not as fast as LMDB.

4. LMDB is a more mature library and is used heavily in projects such as [OpenLDAP](http://www.openldap.org/).


So why use Bolt? The goal of Bolt is provide a simple, fast data store that is easily integrated into Go projects. The library does not require CGO so it is compatible with `go get` and you can easily build static binaries with it. We are not accepting additional functionality into the library so the API and file format are stable. Bolt also has near 100% unit test coverage and also includes heavy black box testing using the [testing/quick](http://golang.org/pkg/testing/quick/) package.


## API

### Database

The database is the object that represents your data as a whole
It is split up into buckets which is analogous to tables in a relational database.

#### Opening and closing a database

```go
db := DB()
err := db.Open("/path/to/db", 0666)
...
err := db.Close()
```


### Transactions

Versioning of data in the bucket data happens through a Transaction.
These transactions can be either be read-only or read/write transactions.
Transactions are what keeps Bolt consistent and allows data to be rolled back if needed.

It may seem strange that read-only access needs to be wrapped in a transaction but this is so Bolt can keep track of what version of the data is currently in use.
The space used to hold data is kept until the transaction closes.

One important note is that long running transactions can cause the database to grow in size quickly.
Please double check that you are appropriately closing all transactions after you're done with them.

#### Creating and closing a read-only transaction

```go
t, err := db.Transaction()
t.Close()
```

#### Creating and committing a read/write transaction

```
t, err := db.RWTransaction()
err := t.Commit()
```

#### Creating and aborting a read/write transaction

```
t, err := db.RWTransaction()
err := t.Abort()
```


### Buckets

Buckets are where your actual key/value data gets stored.
You can create new buckets from the database and look them up by name.

#### Creating a bucket

```go
t, err := db.RWTransaction()
err := t.CreateBucket("widgets")
```

#### Renaming a bucket

```go
t, err := db.RWTransaction()
err := t.RenameBucket("widgets", "woojits")
```

#### Deleting a bucket

```go
t, err := db.RWTransaction()
err := t.DeleteBucket("widgets")
```

#### Retrieve an existing bucket

```go
t, err := db.Transaction()
b, err := t.Bucket("widgets")
```

#### Retrieve a list of all buckets

```go
t, err := db.Transaction()
buckets, err := db.Buckets()
```


### Key/Value Access

#### Retrieve a value for a specific key

```go
t, err := db.Transaction()
value, err := t.Get("widgets", []byte("foo"))
value, err := t.GetString("widgets", "foo")
```

#### Set the value for a key

```go
t, err := db.RWTransaction()
err := t.Put("widgets", []byte("foo"), []byte("bar"))
err := t.PutString("widgets", "foo", "bar")
```

#### Delete a given key

```go
t, err := db.RWTransaction()
err := t.Delete("widgets", []byte("foo"))
err := t.DeleteString("widgets", "foo")
```


### Cursors

Cursors provide fast read-only access to a specific bucket within a transaction.


#### Creating a read-only cursor

```go
t, err := db.Transaction()
c, err := b.Cursor("widgets")
```

#### Iterating over a cursor

```go
for k, v, err := c.First(); k != nil; k, v, err = c.Next() {
	if err != nil {
		return err
	}
	... DO SOMETHING ...
}
```


## Internals

The Bolt database is meant to be a clean, readable implementation of a fast single-level key/value data store.
This section gives an overview of the basic concepts and structure of the file format.

### B+ Tree

Bolt uses a data structure called an append-only B+ tree to store its data.
This structure allows for efficient traversal of data.

TODO: Explain better. :)


### Pages

Bolt stores its data in discrete units called pages.
The page size can be configured but is typically between 4KB and 32KB.

There are several different types of pages:

* Meta pages - The first two pages in a database are meta pages. These are used to store references to root pages for system buckets as well as keep track of the last transaction identifier.

* Branch pages - These pages store references to the location of deeper branch pages or leaf pages.

* Leaf pages - These pages store the actual key/value data.

* Overflow pages - These are special pages used when a key's data is too large for a leaf page and needs to spill onto additional pages.


### Nodes

Within each page there are one or more elements called nodes.
In branch pages, these nodes store references to other child pages in the tree.
In leaf pages, these nodes store the actual key/value data.


## TODO

The following is a list of items to do on the Bolt project:

1. Calculate freelist on db.Open(). (Traverse branches, set bitmap index, load free pages into a list -- lazy load in the future).
2. Resize map. (Make sure there are no reader txns before resizing)
3. DB.Copy()
4. Merge pages.
5. Rebalance (after deletion).
