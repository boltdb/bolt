package bolt

import (
	"sort"
	"unsafe"
)

// buckets represents a in-memory buckets page.
type buckets struct {
	pgid    pgid
	items map[string]*bucket
}

// size returns the size of the page after serialization.
func (b *buckets) size() int {
	var size int = pageHeaderSize
	for key, _ := range b.items {
		size += int(unsafe.Sizeof(bucket{})) + len(key)
	}
	return size
}

// get retrieves a bucket by name.
func (b *buckets) get(key string) *bucket {
	return b.items[key]
}

// put sets a new value for a bucket.
func (b *buckets) put(key string, item *bucket) {
	b.items[key] = item
}

// del deletes a bucket by name.
func (b *buckets) del(key string) {
	if item := b.items[key]; item != nil {
		delete(b.items, key)
	}
}

// read initializes the data from an on-disk page.
func (b *buckets) read(p *page) {
	b.pgid = p.id
	b.items = make(map[string]*bucket)

	var items []*bucket
	var keys []string

	// Read items.
	nodes := (*[maxNodesPerPage]bucket)(unsafe.Pointer(&p.ptr))
	for i := 0; i < int(p.count); i++ {
		node := &nodes[i]
		items = append(items, node)
	}

	// Read keys.
	buf := (*[maxAllocSize]byte)(unsafe.Pointer(&nodes[p.count]))[:]
	for i := 0; i < int(p.count); i++ {
		size := int(buf[0])
		buf = buf[1:]
		keys = append(keys, string(buf[:size]))
		buf = buf[size:]
	}

	// Associate keys and items.
	for index, key := range keys {
		b.items[key] = &bucket{items[index].root}
	}
}

// write writes the items onto a page.
func (b *buckets) write(p *page) {
	// Initialize page.
	p.flags |= p_buckets
	p.count = uint16(len(b.items))

	// Sort keys.
	var keys []string
	for key, _ := range b.items {
		keys = append(keys, key)
	}
	sort.StringSlice(keys).Sort()

	// Write each bucket to the page.
	items := (*[maxNodesPerPage]bucket)(unsafe.Pointer(&p.ptr))
	for index, key := range keys {
		items[index] = *b.items[key]
	}

	// Write each key to the page.
	buf := (*[maxAllocSize]byte)(unsafe.Pointer(&items[p.count]))[:]
	for _, key := range keys {
		buf[0] = byte(len(key))
		buf = buf[1:]
		copy(buf, []byte(key))
		buf = buf[len(key):]
	}
}

// updateRoot finds a bucket by root id and then updates it to point to a new root.
func (b *buckets) updateRoot(oldid, newid pgid) {
	for _, b := range b.items {
		if b.root == oldid {
			b.root = newid
			return
		}
	}
}
