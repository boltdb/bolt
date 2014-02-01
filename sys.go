package bolt

import (
	"sort"
	"unsafe"
)

// sys represents a in-memory system page.
type sys struct {
	pgid    pgid
	buckets map[string]*bucket
}

// size returns the size of the page after serialization.
func (s *sys) size() int {
	var size int = pageHeaderSize
	for key, _ := range s.buckets {
		size += int(unsafe.Sizeof(bucket{})) + len(key)
	}
	return size
}

// get retrieves a bucket by name.
func (s *sys) get(key string) *bucket {
	return s.buckets[key]
}

// getByRoot retrieves a bucket by root page id.
func (s *sys) getByRoot(pgid pgid) *bucket {
	for _, b := range s.buckets {
		if b.root == pgid {
			return b
		}
	}
	panic("root not found")
}

// put sets a new value for a bucket.
func (s *sys) put(key string, b *bucket) {
	s.buckets[key] = b
}

// del deletes a bucket by name.
func (s *sys) del(key string) {
	if b := s.buckets[key]; b != nil {
		delete(s.buckets, key)
	}
}

// read initializes the data from an on-disk page.
func (s *sys) read(p *page) {
	s.pgid = p.id
	s.buckets = make(map[string]*bucket)

	var buckets []*bucket
	var keys []string

	// Read buckets.
	nodes := (*[maxNodesPerPage]bucket)(unsafe.Pointer(&p.ptr))
	for i := 0; i < int(p.count); i++ {
		node := &nodes[i]
		buckets = append(buckets, node)
	}

	// Read keys.
	buf := (*[maxAllocSize]byte)(unsafe.Pointer(&nodes[p.count]))[:]
	for i := 0; i < int(p.count); i++ {
		size := int(buf[0])
		buf = buf[1:]
		keys = append(keys, string(buf[:size]))
		buf = buf[size:]
	}

	// Associate keys and buckets.
	for index, key := range keys {
		b := &bucket{buckets[index].root}
		s.buckets[key] = b
	}
}

// write writes the items onto a page.
func (s *sys) write(p *page) {
	// Initialize page.
	p.flags |= p_sys
	p.count = uint16(len(s.buckets))

	// Sort keys.
	var keys []string
	for key, _ := range s.buckets {
		keys = append(keys, key)
	}
	sort.StringSlice(keys).Sort()

	// Write each bucket to the page.
	buckets := (*[maxNodesPerPage]bucket)(unsafe.Pointer(&p.ptr))
	for index, key := range keys {
		buckets[index] = *s.buckets[key]
	}

	// Write each key to the page.
	buf := (*[maxAllocSize]byte)(unsafe.Pointer(&buckets[p.count]))[:]
	for _, key := range keys {
		buf[0] = byte(len(key))
		buf = buf[1:]
		copy(buf, []byte(key))
		buf = buf[len(key):]
	}
}
