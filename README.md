Bolt [![Build Status](https://drone.io/github.com/boltdb/bolt/status.png)](https://drone.io/github.com/boltdb/bolt/latest) [![Coverage Status](https://coveralls.io/repos/boltdb/bolt/badge.png?branch=master)](https://coveralls.io/r/boltdb/bolt?branch=master) [![GoDoc](https://godoc.org/github.com/boltdb/bolt?status.png)](https://godoc.org/github.com/boltdb/bolt) ![Project status](http://img.shields.io/status/beta.png?color=blue)
====

## Overview

Bolt is a pure Go key/value store inspired by [Howard Chu](https://twitter.com/hyc_symas) and the [LMDB project](http://symas.com/mdb/). The goal of the project is to provide a simple, fast, and reliable database for projects that don't require a full database server such as Postgres or MySQL. It is also meant to be educational. Most of us use tools without understanding how the underlying data really works.

Since Bolt is meant to be used as such a low-level piece of functionality, simplicity is key. The API will be small and only center around getting values and setting values. That's it. If you want to see additional functionality added then we encourage you submit a Github issue and we can discuss developing it as a separate fork.

> Simple is the new beautiful.
>
> — [Tobias Lütke](https://twitter.com/tobi)


## Project Status

Bolt is functionally complete and has nearly full unit test coverage. The library test suite also includes randomized black box testing to ensure database consistency and thread safety. Bolt is currently in use in a few projects, however, it is still at a beta stage so please use with caution and report any bugs found.


## Comparing Bolt vs LMDB

Bolt is inspired by [LMDB](http://symas.com/mdb/) so there are many similarities between the two:

1. Both use a [B+Tree](http://en.wikipedia.org/wiki/B%2B_tree) data structure.

2. ACID semantics with fully [serializable transactions](http://en.wikipedia.org/wiki/Isolation_(database_systems)#Serializable).

3. Lock-free MVCC support using a single writer and multiple readers.


There are also several differences between Bolt and LMDB:

1. LMDB supports more additional features such as multi-value keys, fixed length keys, multi-key insert, and direct writes. Bolt only supports basic `Get()`, `Put()`, and `Delete()` operations and bidirectional cursors.

2. LMDB databases can be shared between processes. Bolt only allows a single process access.

3. LMDB is written in C and extremely fast. Bolt is fast but not as fast as LMDB.

4. LMDB is a more mature library and is used heavily in projects such as [OpenLDAP](http://www.openldap.org/).


So why use Bolt? The goal of Bolt is provide a simple, fast data store that is easily integrated into Go projects. The library does not require CGO so it is compatible with `go get` and you can easily build static binaries with it. We are not accepting additional functionality into the library so the API and file format are stable. Bolt also has near 100% unit test coverage and also includes heavy black box testing using the [testing/quick](http://golang.org/pkg/testing/quick/) package.


## Other Projects Using Bolt

Below is a list of public, open source projects that use Bolt:

* [Bazil](https://github.com/bazillion/bazil) - A file system that lets your data reside where it is most convenient for it to reside.
* [DVID](https://github.com/janelia-flyem/dvid) - Added Bolt as optional storage engine and testing it against Basho-tuned leveldb.
* [Skybox Analytics](https://github.com/skybox/skybox) - A standalone funnel analysis tool for web analytics.
* [Scuttlebutt](https://github.com/benbjohnson/scuttlebutt) - Uses Bolt to store and process all Twitter mentions of GitHub projects.
* [Wiki](https://github.com/peterhellberg/wiki) - A tiny wiki using Goji, BoltDB and Blackfriday.
* [ChainStore](https://github.com/nulayer/chainstore) - Simple key-value interface to a variety of storage engines organized as a chain of operations.
* [MetricBase](https://github.com/msiebuhr/MetricBase) - Single-binary version of Graphite.
* [Gitchain](https://github.com/gitchain/gitchain) - Decentralized, peer-to-peer Git repositories aka "Git meets Bitcoin".
* [SkyDB](https://github.com/skydb/sky) - Behavioral analytics database.
* [event-shuttle](https://github.com/sclasen/event-shuttle) - A Unix system service to collect and reliably deliver messages to Kafka.
* [ipxed](https://github.com/kelseyhightower/ipxed) - Web interface and api for ipxed.


If you are using Bolt in a project please send a pull request to add it to the list.

