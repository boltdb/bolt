Bolt [![Build Status](https://drone.io/github.com/boltdb/bolt/status.png)](https://drone.io/github.com/boltdb/bolt/latest) [![Coverage Status](https://coveralls.io/repos/boltdb/bolt/badge.png?branch=master)](https://coveralls.io/r/boltdb/bolt?branch=master) [![GoDoc](https://godoc.org/github.com/boltdb/bolt?status.png)](https://godoc.org/github.com/boltdb/bolt) ![Project status](http://img.shields.io/status/pre-alpha.png?color=red)
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

