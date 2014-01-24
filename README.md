Bolt
====

## Overview

A low-level key/value database for Go.


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



### Cursors

Cursors provide access to a specific bucket within a transaction.


#### Creating a read-only cursor

```go
t, err := db.Transaction()
c, err := b.Cursor("widgets")
```

#### Creating a read/write cursor

```go
t, err := db.RWTransaction()
c, err := t.RWCursor("widgets")
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

#### Retrieve a value for a specific key

```go
value, err := c.Get([]byte("foo"))
value, err := c.GetString("foo")
```

#### Set the value for a key (RWCursor only)

```go
err := c.Put([]byte("foo"), []byte("bar"))
err := c.PutString("foo", "bar")
```

#### Delete a given key

```go
err := c.Delete([]byte("foo"))
err := c.DeleteString("foo")
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

1. Resize map. (Make sure there are no reader txns before resizing)
2. DB.Copy()
