package bolt

import (
	"errors"
	"sort"
	"time"
	"unsafe"
)

var (
	// ErrTxNotWritable is returned when performing a write operation on a
	// read-only transaction.
	ErrTxNotWritable = errors.New("tx not writable")

	// ErrTxClosed is returned when committing or rolling back a transaction
	// that has already been committed or rolled back.
	ErrTxClosed = errors.New("tx closed")
)

// txid represents the internal transaction identifier.
type txid uint64

// Tx represents a read-only or read/write transaction on the database.
// Read-only transactions can be used for retrieving values for keys and creating cursors.
// Read/write transactions can create and remove buckets and create and remove keys.
//
// IMPORTANT: You must commit or rollback transactions when you are done with
// them. Pages can not be reclaimed by the writer until no more transactions
// are using them. A long running read transaction can cause the database to
// quickly grow.
type Tx struct {
	writable       bool
	managed        bool
	db             *DB
	meta           *meta
	buckets        *buckets
	nodes          map[pgid]*node
	pages          map[pgid]*page
	pending        []*node
	stats          TxStats
	commitHandlers []func()
}

// init initializes the transaction.
func (tx *Tx) init(db *DB) {
	tx.db = db
	tx.pages = nil

	// Copy the meta page since it can be changed by the writer.
	tx.meta = &meta{}
	db.meta().copy(tx.meta)

	// Read in the buckets page.
	tx.buckets = &buckets{}
	tx.buckets.read(tx.page(tx.meta.buckets))

	if tx.writable {
		tx.pages = make(map[pgid]*page)
		tx.nodes = make(map[pgid]*node)

		// Increment the transaction id.
		tx.meta.txid += txid(1)
	}
}

// id returns the transaction id.
func (tx *Tx) id() txid {
	return tx.meta.txid
}

// DB returns a reference to the database that created the transaction.
func (tx *Tx) DB() *DB {
	return tx.db
}

// Writable returns whether the transaction can perform write operations.
func (tx *Tx) Writable() bool {
	return tx.writable
}

// Stats retrieves a copy of the current transaction statistics.
func (tx *Tx) Stats() TxStats {
	return tx.stats
}

// Bucket retrieves a bucket by name.
// Returns nil if the bucket does not exist.
func (tx *Tx) Bucket(name string) *Bucket {
	b := tx.buckets.get(name)
	if b == nil {
		return nil
	}

	return &Bucket{
		bucket: b,
		name:   name,
		tx:     tx,
	}
}

// Buckets retrieves a list of all buckets.
func (tx *Tx) Buckets() []*Bucket {
	buckets := make([]*Bucket, 0, len(tx.buckets.items))
	for name, b := range tx.buckets.items {
		bucket := &Bucket{
			bucket: b,
			name:   name,
			tx:     tx,
		}
		buckets = append(buckets, bucket)
	}
	sort.Sort(bucketsByName(buckets))
	return buckets
}

// CreateBucket creates a new bucket.
// Returns an error if the bucket already exists, if the bucket name is blank, or if the bucket name is too long.
func (tx *Tx) CreateBucket(name string) error {
	if tx.db == nil {
		return ErrTxClosed
	} else if !tx.writable {
		return ErrTxNotWritable
	} else if b := tx.Bucket(name); b != nil {
		return ErrBucketExists
	} else if len(name) == 0 {
		return ErrBucketNameRequired
	} else if len(name) > MaxBucketNameSize {
		return ErrBucketNameTooLarge
	}

	// Create a blank root leaf page.
	p, err := tx.allocate(1)
	if err != nil {
		return err
	}
	p.flags = leafPageFlag

	// Add bucket to buckets page.
	tx.buckets.put(name, &bucket{root: p.id})

	return nil
}

// CreateBucketIfNotExists creates a new bucket if it doesn't already exist.
// Returns an error if the bucket name is blank, or if the bucket name is too long.
func (tx *Tx) CreateBucketIfNotExists(name string) error {
	err := tx.CreateBucket(name)
	if err != nil && err != ErrBucketExists {
		return err
	}
	return nil
}

// DeleteBucket deletes a bucket.
// Returns an error if the bucket cannot be found.
func (tx *Tx) DeleteBucket(name string) error {
	if tx.db == nil {
		return ErrTxClosed
	} else if !tx.writable {
		return ErrTxNotWritable
	}

	b := tx.Bucket(name)
	if b == nil {
		return ErrBucketNotFound
	}

	// Remove from buckets page.
	tx.buckets.del(name)

	// Free all pages.
	tx.forEachPage(b.root, 0, func(p *page, depth int) {
		tx.db.freelist.free(tx.id(), p)
	})

	return nil
}

// OnCommit adds a handler function to be executed after the transaction successfully commits.
func (tx *Tx) OnCommit(fn func()) {
	tx.commitHandlers = append(tx.commitHandlers, fn)
}

// Commit writes all changes to disk and updates the meta page.
// Returns an error if a disk write error occurs.
func (tx *Tx) Commit() error {
	if tx.managed {
		panic("managed tx commit not allowed")
	} else if tx.db == nil {
		return ErrTxClosed
	} else if !tx.writable {
		return ErrTxNotWritable
	}

	// TODO(benbjohnson): Use vectorized I/O to write out dirty pages.

	// Rebalance nodes which have had deletions.
	var startTime = time.Now()
	tx.rebalance()
	tx.stats.RebalanceTime += time.Since(startTime)

	// spill data onto dirty pages.
	startTime = time.Now()
	if err := tx.spill(); err != nil {
		tx.close()
		return err
	}
	tx.stats.SpillTime += time.Since(startTime)

	// Spill buckets page.
	p, err := tx.allocate((tx.buckets.size() / tx.db.pageSize) + 1)
	if err != nil {
		tx.close()
		return err
	}
	tx.buckets.write(p)

	// Free previous bucket page and update meta.
	tx.db.freelist.free(tx.id(), tx.page(tx.meta.buckets))
	tx.meta.buckets = p.id

	// Free the freelist and allocate new pages for it. This will overestimate
	// the size of the freelist but not underestimate the size (which would be bad).
	tx.db.freelist.free(tx.id(), tx.page(tx.meta.freelist))
	p, err = tx.allocate((tx.db.freelist.size() / tx.db.pageSize) + 1)
	if err != nil {
		tx.close()
		return err
	}
	tx.db.freelist.write(p)
	tx.meta.freelist = p.id

	// Write dirty pages to disk.
	startTime = time.Now()
	if err := tx.write(); err != nil {
		tx.close()
		return err
	}

	// Write meta to disk.
	if err := tx.writeMeta(); err != nil {
		tx.close()
		return err
	}
	tx.stats.WriteTime += time.Since(startTime)

	// Finalize the transaction.
	tx.close()

	// Execute commit handlers now that the locks have been removed.
	for _, fn := range tx.commitHandlers {
		fn()
	}

	return nil
}

// Rollback closes the transaction and ignores all previous updates.
func (tx *Tx) Rollback() error {
	if tx.managed {
		panic("managed tx rollback not allowed")
	} else if tx.db == nil {
		return ErrTxClosed
	}
	tx.close()
	return nil
}

func (tx *Tx) close() {
	if tx.writable {
		// Merge statistics.
		tx.db.metalock.Lock()
		tx.db.stats.TxStats.add(&tx.stats)
		tx.db.metalock.Unlock()

		// Remove writer lock.
		tx.db.rwlock.Unlock()
	} else {
		tx.db.removeTx(tx)
	}
	tx.db = nil
}

// allocate returns a contiguous block of memory starting at a given page.
func (tx *Tx) allocate(count int) (*page, error) {
	p, err := tx.db.allocate(count)
	if err != nil {
		return nil, err
	}

	// Save to our page cache.
	tx.pages[p.id] = p

	// Update statistics.
	tx.stats.PageCount++
	tx.stats.PageAlloc += count * tx.db.pageSize

	return p, nil
}

// rebalance attempts to balance all nodes.
func (tx *Tx) rebalance() {
	for _, n := range tx.nodes {
		n.rebalance()
	}
}

// spill writes all the nodes to dirty pages.
func (tx *Tx) spill() error {
	// Keep track of the current root nodes.
	// We will update this at the end once all nodes are created.
	type root struct {
		node *node
		pgid pgid
	}
	var roots []root

	// Sort nodes by highest depth first.
	nodes := make(nodesByDepth, 0, len(tx.nodes))
	for _, n := range tx.nodes {
		nodes = append(nodes, n)
	}
	sort.Sort(nodes)

	// Spill nodes by deepest first.
	for i := 0; i < len(nodes); i++ {
		n := nodes[i]

		// Save existing root buckets for later.
		if n.parent == nil && n.pgid != 0 {
			roots = append(roots, root{n, n.pgid})
		}

		// Split nodes into appropriate sized nodes.
		// The first node in this list will be a reference to n to preserve ancestry.
		newNodes := n.split(tx.db.pageSize)
		tx.pending = newNodes

		// If this is a root node that split then create a parent node.
		if n.parent == nil && len(newNodes) > 1 {
			n.parent = &node{tx: tx, isLeaf: false}
			nodes = append(nodes, n.parent)
		}

		// Add node's page to the freelist.
		if n.pgid > 0 {
			tx.db.freelist.free(tx.id(), tx.page(n.pgid))
		}

		// Write nodes to dirty pages.
		for i, newNode := range newNodes {
			// Allocate contiguous space for the node.
			p, err := tx.allocate((newNode.size() / tx.db.pageSize) + 1)
			if err != nil {
				return err
			}

			// Write the node to the page.
			newNode.write(p)
			newNode.pgid = p.id
			newNode.parent = n.parent

			// The first node should use the existing entry, other nodes are inserts.
			var oldKey []byte
			if i == 0 {
				oldKey = n.key
			} else {
				oldKey = newNode.inodes[0].key
			}

			// Update the parent entry.
			if newNode.parent != nil {
				newNode.parent.put(oldKey, newNode.inodes[0].key, nil, newNode.pgid)
			}

			// Update the statistics.
			tx.stats.Spill++
		}

		tx.pending = nil
	}

	// Update roots with new roots.
	for _, root := range roots {
		tx.buckets.updateRoot(root.pgid, root.node.root().pgid)
	}

	// Clear out nodes now that they are all spilled.
	tx.nodes = make(map[pgid]*node)

	return nil
}

// write writes any dirty pages to disk.
func (tx *Tx) write() error {
	// Sort pages by id.
	pages := make(pages, 0, len(tx.pages))
	for _, p := range tx.pages {
		pages = append(pages, p)
	}
	sort.Sort(pages)

	// Write pages to disk in order.
	for _, p := range pages {
		size := (int(p.overflow) + 1) * tx.db.pageSize
		buf := (*[maxAllocSize]byte)(unsafe.Pointer(p))[:size]
		offset := int64(p.id) * int64(tx.db.pageSize)
		if _, err := tx.db.ops.writeAt(buf, offset); err != nil {
			return err
		}

		// Update statistics.
		tx.stats.Write++
	}
	if err := fdatasync(tx.db.file); err != nil {
		return err
	}

	// Clear out page cache.
	tx.pages = make(map[pgid]*page)

	return nil
}

// writeMeta writes the meta to the disk.
func (tx *Tx) writeMeta() error {
	// Create a temporary buffer for the meta page.
	buf := make([]byte, tx.db.pageSize)
	p := tx.db.pageInBuffer(buf, 0)
	tx.meta.write(p)

	// Write the meta page to file.
	if _, err := tx.db.ops.writeAt(buf, int64(p.id)*int64(tx.db.pageSize)); err != nil {
		return err
	}
	if err := fdatasync(tx.db.file); err != nil {
		return err
	}

	// Update statistics.
	tx.stats.Write++

	return nil
}

// node creates a node from a page and associates it with a given parent.
func (tx *Tx) node(pgid pgid, parent *node) *node {
	// Retrieve node if it's already been created.
	if tx.nodes == nil {
		return nil
	} else if n := tx.nodes[pgid]; n != nil {
		return n
	}

	// Otherwise create a branch and cache it.
	n := &node{tx: tx, parent: parent}
	if n.parent != nil {
		n.depth = n.parent.depth + 1
	}
	n.read(tx.page(pgid))
	tx.nodes[pgid] = n

	// Update statistics.
	tx.stats.NodeCount++

	return n
}

// dereference removes all references to the old mmap.
func (tx *Tx) dereference() {
	for _, n := range tx.nodes {
		n.dereference()
	}

	for _, n := range tx.pending {
		n.dereference()
	}

	// Update statistics
	tx.stats.NodeDeref += len(tx.nodes) + len(tx.pending)
}

// page returns a reference to the page with a given id.
// If page has been written to then a temporary bufferred page is returned.
func (tx *Tx) page(id pgid) *page {
	// Check the dirty pages first.
	if tx.pages != nil {
		if p, ok := tx.pages[id]; ok {
			return p
		}
	}

	// Otherwise return directly from the mmap.
	return tx.db.page(id)
}

// pageNode returns the in-memory node, if it exists.
// Otherwise returns the underlying page.
func (tx *Tx) pageNode(id pgid) (*page, *node) {
	if tx.nodes != nil {
		if n := tx.nodes[id]; n != nil {
			return nil, n
		}
	}
	return tx.page(id), nil
}

// forEachPage iterates over every page within a given page and executes a function.
func (tx *Tx) forEachPage(pgid pgid, depth int, fn func(*page, int)) {
	p := tx.page(pgid)

	// Execute function.
	fn(p, depth)

	// Recursively loop over children.
	if (p.flags & branchPageFlag) != 0 {
		for i := 0; i < int(p.count); i++ {
			elem := p.branchPageElement(uint16(i))
			tx.forEachPage(elem.pgid, depth+1, fn)
		}
	}
}

// Page returns page information for a given page number.
// This is only available from writable transactions.
func (tx *Tx) Page(id int) (*PageInfo, error) {
	if tx.db == nil {
		return nil, ErrTxClosed
	} else if !tx.writable {
		return nil, ErrTxNotWritable
	} else if pgid(id) >= tx.meta.pgid {
		return nil, nil
	}

	// Build the page info.
	p := tx.page(pgid(id))
	info := &PageInfo{
		ID:            id,
		Count:         int(p.count),
		OverflowCount: int(p.overflow),
	}

	// Determine the type (or if it's free).
	if tx.db.freelist.isFree(pgid(id)) {
		info.Type = "free"
	} else {
		info.Type = p.typ()
	}

	return info, nil
}

// TxStats represents statistics about the actions performed by the transaction.
type TxStats struct {
	// Page statistics.
	PageCount int // number of page allocations
	PageAlloc int // total bytes allocated

	// Cursor statistics.
	CursorCount int // number of cursors created

	// Node statistics
	NodeCount int // number of node allocations
	NodeDeref int // number of node dereferences

	// Rebalance statistics.
	Rebalance     int           // number of node rebalances
	RebalanceTime time.Duration // total time spent rebalancing

	// Spill statistics.
	Spill     int           // number of node spilled
	SpillTime time.Duration // total time spent spilling

	// Write statistics.
	Write     int           // number of writes performed
	WriteTime time.Duration // total time spent writing to disk
}

func (s *TxStats) add(other *TxStats) {
	s.PageCount += other.PageCount
	s.PageAlloc += other.PageAlloc
	s.CursorCount += other.CursorCount
	s.NodeCount += other.NodeCount
	s.NodeDeref += other.NodeDeref
	s.Rebalance += other.Rebalance
	s.RebalanceTime += other.RebalanceTime
	s.Spill += other.Spill
	s.SpillTime += other.SpillTime
	s.Write += other.Write
	s.WriteTime += other.WriteTime
}

// Sub calculates and returns the difference between two sets of transaction stats.
// This is useful when obtaining stats at two different points and time and
// you need the performance counters that occurred within that time span.
func (s *TxStats) Sub(other *TxStats) TxStats {
	var diff TxStats
	diff.PageCount = s.PageCount - other.PageCount
	diff.PageAlloc = s.PageAlloc - other.PageAlloc
	diff.CursorCount = s.CursorCount - other.CursorCount
	diff.NodeCount = s.NodeCount - other.NodeCount
	diff.NodeDeref = s.NodeDeref - other.NodeDeref
	diff.Rebalance = s.Rebalance - other.Rebalance
	diff.RebalanceTime = s.RebalanceTime - other.RebalanceTime
	diff.Spill = s.Spill - other.Spill
	diff.SpillTime = s.SpillTime - other.SpillTime
	diff.Write = s.Write - other.Write
	diff.WriteTime = s.WriteTime - other.WriteTime
	return diff
}
