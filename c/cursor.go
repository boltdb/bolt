package c

/*
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <inttypes.h>

//------------------------------------------------------------------------------
// Constants
//------------------------------------------------------------------------------

// This represents the maximum number of levels that a cursor can traverse.
#define MAX_DEPTH	100

// These flags mark the type of page and are set in the page.flags.
#define PAGE_BRANCH   0x01
#define PAGE_LEAF     0x02
#define PAGE_META     0x04
#define PAGE_FREELIST 0x10


//------------------------------------------------------------------------------
// Typedefs
//------------------------------------------------------------------------------

// These types MUST have the same layout as their corresponding Go types

typedef int64_t	pgid;

// Page represents a header struct of a block in the mmap.
typedef struct page {
	pgid     id;
	uint16_t flags;
	uint16_t count;
	uint32_t overflow;
} page;

// The branch element represents an a item in a branch page
// that points to a child page.
typedef struct branch_element {
	uint32_t pos;
	uint32_t ksize;
	pgid     pgid;
} branch_element;

// The leaf element represents an a item in a leaf page
// that points to a key/value pair.
typedef struct leaf_element {
	uint32_t flags;
	uint32_t pos;
	uint32_t ksize;
	uint32_t vsize;
} leaf_element;

// elem_ref represents a pointer to an element inside of a page.
// It is used by the cursor stack to track the position at each level.
typedef struct elem_ref {
	page     *page;
	uint16_t index;
} elem_ref;

// bolt_val represents a pointer to a fixed-length series of bytes.
// It is used to represent keys and values returned by the cursor.
typedef struct bolt_val {
    uint32_t size;
    void     *data;
} bolt_val;

// bolt_cursor represents a cursor attached to a bucket.
typedef struct bolt_cursor {
	void     *data;
	pgid     root;
	size_t   pgsz;
	int      top;
	elem_ref stack[MAX_DEPTH];
} bolt_cursor;


//------------------------------------------------------------------------------
// Forward Declarations
//------------------------------------------------------------------------------

elem_ref *cursor_push(bolt_cursor *c, pgid id);

elem_ref *cursor_current(bolt_cursor *c);

elem_ref *cursor_pop(bolt_cursor *c);

void cursor_key_value(bolt_cursor *c, bolt_val *key, bolt_val *value, uint32_t *flags);

void cursor_search(bolt_cursor *c, bolt_val key, pgid id);

void cursor_search_branch(bolt_cursor *c, bolt_val key);

void cursor_search_leaf(bolt_cursor *c, bolt_val key);

//------------------------------------------------------------------------------
// Public Functions
//------------------------------------------------------------------------------

// Initializes a cursor.
void bolt_cursor_init(bolt_cursor *c, void *data, size_t pgsz, pgid root) {
	c->data = data;
	c->root = root;
	c->pgsz = pgsz;
	c->top = -1;
}

// Positions the cursor to the first leaf element and returns the key/value pair.
void bolt_cursor_first(bolt_cursor *c, bolt_val *key, bolt_val *value, uint32_t *flags) {
	// reset stack to initial state
	elem_ref *ref = cursor_push(c, c->root);

	// Find first leaf and return key/value.
	cursor_key_value(c, key, value, flags);
}

// Positions the cursor to the next leaf element and returns the key/value pair.
void bolt_cursor_next(bolt_cursor *c, bolt_val *key, bolt_val *value, uint32_t *flags) {
	int i;
	elem_ref *ref;

	// Attempt to move over one element until we're successful.
	// Move up the stack as we hit the end of each page in our stack.
	for (ref = cursor_current(c); ref != NULL; ref = cursor_current(c)) {
		ref->index++;
		if (ref->index < ref->page->count) break;
		cursor_pop(c);
	};

	// Find first leaf and return key/value.
	cursor_key_value(c, key, value, flags);
}

// Positions the cursor first leaf element starting from a given key.
// If there is a matching key then the cursor will be place on that key.
// If there not a match then the cursor will be placed on the next key, if available.
void bolt_cursor_seek(bolt_cursor *c, bolt_val seek, bolt_val *key, bolt_val *value, uint32_t *flags) {
	// Start from root page/node and traverse to correct page.
	cursor_push(c, c->root);
	if (seek.size > 0) cursor_search(c, seek, c->root);

	// Find first leaf and return key/value.
	cursor_key_value(c, key, value, flags);
}


//------------------------------------------------------------------------------
// Private Functions
//------------------------------------------------------------------------------

// Push ref to the first element of the page onto the cursor stack
// If the page is the root page reset the stack to initial state
elem_ref *cursor_push(bolt_cursor *c, pgid id) {
	elem_ref *ref;
	if (id == c->root)
		c->top = 0;
	else
		c->top++;
	ref = &(c->stack[c->top]);
	ref->page = (page *)(c->data + (c->pgsz * id));
	ref->index = 0;
	return ref;
}

// Return current element ref from the cursor stack
// If stack is empty return null
elem_ref *cursor_current(bolt_cursor *c) {
	if (c->top < 0) return NULL;
	return &c->stack[c->top];
}

// Pop current element ref off the cursor stack
elem_ref *cursor_pop(bolt_cursor *c) {
	elem_ref *ref = cursor_current(c);
	if (ref != NULL) c->top--;
	return ref;
}

// Returns the branch element at a given index on a given page.
branch_element *page_branch_element(page *p, uint16_t index) {
	branch_element *elements = (branch_element*)((void*)(p) + sizeof(page));
	return &elements[index];
}

// Returns the leaf element at a given index on a given page.
leaf_element *page_leaf_element(page *p, uint16_t index) {
	leaf_element *elements = (leaf_element*)((void*)(p) + sizeof(page));
	return &elements[index];
}

// Returns the key/value pair for the current position of the cursor.
void cursor_key_value(bolt_cursor *c, bolt_val *key, bolt_val *value, uint32_t *flags) {
	elem_ref *ref = cursor_current(c);
	if (ref == NULL) {
		key->size = value->size = 0;
		key->data = value->data = NULL;
		*flags = 0;
		return;
	};

	// Descend to the current leaf page if we're on branch page
	while (ref->page->flags & PAGE_BRANCH) {
		branch_element *elem = page_branch_element(ref->page,ref->index);
		ref = cursor_push(c, elem->pgid);
	};

	leaf_element *elem = page_leaf_element(ref->page,ref->index);

	// Assign key pointer.
	key->size = elem->ksize;
	key->data = ((void*)elem) + elem->pos;

	// Assign value pointer.
	value->size = elem->vsize;
	value->data = key->data + key->size;

	// Return the element flags.
	*flags = elem->flags;
}

// Recursively performs a binary search against a given page/node until it finds a given key.
void cursor_search(bolt_cursor *c, bolt_val key, pgid id) {
	// Push page onto the cursor stack.
	elem_ref *ref = cursor_push(c, id);

	// If we're on a leaf page/node then find the specific node.
	if (ref->page->flags & PAGE_LEAF) {
		cursor_search_leaf(c, key);
		return;
	}

	// Otherwise search the branch page.
	cursor_search_branch(c, key);
}

// Recursively search over a leaf page for a key.
void cursor_search_leaf(bolt_cursor *c, bolt_val key) {
	elem_ref *ref = cursor_current(c);
	int i;

	// HACK: Simply loop over elements to find the right one. Replace with a binary search.
	leaf_element *elems = (leaf_element*)((void*)(ref->page) + sizeof(page));
	for (i=0; i<ref->page->count; i++) {
		leaf_element *elem = &elems[i];
		int rc = memcmp(key.data, ((void*)elem) + elem->pos, (elem->ksize < key.size ? elem->ksize : key.size));

		// printf("? %.*s | %.*s\n", key.size, key.data, elem->ksize, ((void*)elem) + elem->pos);
		// printf("rc=%d; key.size(%d) >= elem->ksize(%d)\n", rc, key.size, elem->ksize);
		if ((rc == 0 && key.size <= elem->ksize) || rc < 0) {
			ref->index = i;
			return;
		}
	}

	// If nothing was greater than the key then pop the current page off the stack.
	cursor_pop(c);
}

// Recursively search over a branch page for a key.
void cursor_search_branch(bolt_cursor *c, bolt_val key) {
	elem_ref *ref = cursor_current(c);
	int i;

	// HACK: Simply loop over elements to find the right one. Replace with a binary search.
	branch_element *elems = (branch_element*)((void*)(ref->page) + sizeof(page));
	for (i=0; i<ref->page->count; i++) {
		branch_element *elem = &elems[i];
		int rc = memcmp(key.data, ((void*)elem) + elem->pos, (elem->ksize < key.size ? elem->ksize : key.size));

		if (rc == 0 && key.size == elem->ksize) {
			// exact match, done
			ref->index = i;
			return;
		} else if ((rc == 0 && key.size < elem->ksize) || rc < 0) {
			// if key is less than anything in this subtree we are done
			if (i == 0) return;
			// otherwise search the previous subtree
			cursor_search(c, key, elems[i-1].pgid);
			// didn't find anything greater than key?
			if (cursor_current(c) == ref)
				ref->index = i;
			else
				ref->index = i-1;
			return;
		}
	}

	// If nothing was greater than the key then pop the current page off the stack.
	cursor_pop(c);
}

*/
import "C"

import (
	"fmt"
	"os"
	"unsafe"

	"github.com/boltdb/bolt"
)

// Cursor represents a wrapper around a Bolt C cursor.
type Cursor struct {
	C *C.bolt_cursor
}

// NewCursor creates a C cursor from a Bucket.
func NewCursor(b *bolt.Bucket) *Cursor {
	info := b.Tx().DB().Info()
	root := b.Root()
	c := &Cursor{C: new(C.bolt_cursor)}
	C.bolt_cursor_init(c.C, unsafe.Pointer(&info.Data[0]), C.size_t(info.PageSize), C.pgid(root))
	return c
}

// Next moves the cursor to the first element and returns the key and value.
// Returns a nil key if there are no elements.
func (c *Cursor) First() (key, value []byte) {
	var k, v C.bolt_val
	var flags C.uint32_t
	C.bolt_cursor_first(c.C, &k, &v, &flags)
	if k.data == nil {
		return nil, nil
	}
	return C.GoBytes(k.data, C.int(k.size)), C.GoBytes(v.data, C.int(v.size))
}

// Next moves the cursor to the next element and returns the key and value.
// Returns a nil key if there are no more key/value pairs.
func (c *Cursor) Next() (key, value []byte) {
	var k, v C.bolt_val
	var flags C.uint32_t
	C.bolt_cursor_next(c.C, &k, &v, &flags)
	if k.data == nil {
		return nil, nil
	}
	return C.GoBytes(k.data, C.int(k.size)), C.GoBytes(v.data, C.int(v.size))
}

// Seek moves the cursor to a given key and returns it.
// If the key does not exist then the next key is used. If no keys
// follow, an empty value is returned.
func (c *Cursor) Seek(seek []byte) (key, value []byte, flags int) {
	var _flags C.uint32_t
	var _seek, k, v C.bolt_val
	if len(seek) > 0 {
		_seek.size = C.uint32_t(len(seek))
		_seek.data = unsafe.Pointer(&seek[0])
	}
	C.bolt_cursor_seek(c.C, _seek, &k, &v, &_flags)
	//fmt.Printf("Key %v [%v]\n", k.data, k.size)
	//fmt.Printf("Value %v [%v]\n", k.data, k.size)
	if k.data == nil {
		return nil, nil, 0
	}

	return C.GoBytes(k.data, C.int(k.size)), C.GoBytes(v.data, C.int(v.size)), int(_flags)
}

func warn(v ...interface{}) {
	fmt.Fprintln(os.Stderr, v...)
}

func warnf(msg string, v ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", v...)
}
