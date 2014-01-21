bolt
====

## Overview

A low-level key/value database for Go.


## API

### DB

### Creating a database

```
db := DB()
err := db.Open("/path/to/db", 0666)
...
err := db.Close()
```

### Creating a bucket


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
