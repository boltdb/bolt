bolt
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


### Buckets

Buckets are where your actual key/value data gets stored.
You can create new buckets from the database and look them up by name.

#### Creating a bucket

```go
b, err := db.CreateBucket("widgets")
```

#### Retrieve an existing bucket

```go
b, err := db.Bucket("widgets")
```

#### Retrieve a list of all buckets

```go
buckets, err := db.Buckets()
```

#### Deleting a bucket

```go
err := db.DeleteBucket("widgets")
```


### Transactions

All access to the bucket data happens through a Transaction.
These transactions can be either be read-only or read/write transactions.
Transactions are what keeps Bolt consistent and allows data to be rolled back if needed.

It may seem strange that read-only access needs to be wrapped in a transaction but this is so Bolt can keep track of what version of the data is currently in use.
The space used to hold data is kept until the transaction closes.

One important note is that long running transactions can cause the database to grow in size quickly.
Please double check that you are appropriately closing all transactions after you're done with them.

#### Creating and closing a read-only transaction

```go
t, err := db.Transaction()

// Read/write txn:
t, err := db.RWTransaction()
```





* Cursor


```
DB
Bucket
Transaction / RWTransaction
Cursor / RWCursor

page
meta
branchNode
leafNode
```
