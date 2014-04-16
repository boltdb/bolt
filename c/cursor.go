package c

/*
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
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
	pgid     page;
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

page *cursor_page(bolt_cursor *c, pgid id);

void cursor_first_leaf(bolt_cursor *c);

void cursor_key_value(bolt_cursor *c, bolt_val *key, bolt_val *value, uint32_t *flags);


//------------------------------------------------------------------------------
// Public Functions
//------------------------------------------------------------------------------

// Initializes a cursor.
void bolt_cursor_init(bolt_cursor *c, void *data, size_t pgsz, pgid root) {
	c->data = data;
	c->root = root;
	c->pgsz = pgsz;
}

// Positions the cursor to the first leaf element and returns the key/value pair.
void bolt_cursor_first(bolt_cursor *c, bolt_val *key, bolt_val *value, uint32_t *flags) {
	// reset stack to initial state
	c->top = 0;
	elem_ref *ref = &(c->stack[c->top]);
	ref->page = cursor_page(c, c->root);
	ref->index = 0;

	// Find first leaf and return key/value.
	cursor_first_leaf(c);
	cursor_key_value(c, key, value, flags);
}

// Positions the cursor to the next leaf element and returns the key/value pair.
void bolt_cursor_next(bolt_cursor *c, bolt_val *key, bolt_val *value, uint32_t *flags) {
	// Attempt to move over one element until we're successful.
	// Move up the stack as we hit the end of each page in our stack.
	for (int i = c->top; i >= 0; i--) {
		elem_ref *elem = &c->stack[i];
		if (elem->index < elem->page->count - 1) {
			elem->index++;
			break;
		}
		c->top--;
	}

	// If we are at the top of the stack then return a blank key/value pair.
	if (c->top == -1) {
		key->size = value->size = 0;
		key->data = value->data = NULL;
		*flags = 0;
		return;
	}

	// Find first leaf and return key/value.
	cursor_first_leaf(c);
	cursor_key_value(c, key, value, flags);
}


//------------------------------------------------------------------------------
// Private Functions
//------------------------------------------------------------------------------

// Returns a page pointer from a page identifier.
page *cursor_page(bolt_cursor *c, pgid id) {
	return (page *)(c->data + (c->pgsz * id));
}

// Returns the leaf element at a given index on a given page.
branch_element *branch_page_element(page *p, uint16_t index) {
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
	elem_ref *ref = &(c->stack[c->top]);
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

// Traverses from the current stack position down to the first leaf element.
void cursor_first_leaf(bolt_cursor *c) {
	elem_ref *ref = &(c->stack[c->top]);
	branch_element *branch;
	while (ref->page->flags & PAGE_BRANCH) {
		branch = branch_page_element(ref->page,ref->index);
		c->top++;
		ref = &c->stack[c->top];
		ref->index = 0;
		ref->page = cursor_page(c, branch->page);
	};
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
	return C.GoBytes(k.data, C.int(k.size)), C.GoBytes(v.data, C.int(v.size))
}

// Next moves the cursor to the next element and returns the key and value.
// Returns a nil key if there are no more key/value pairs.
func (c *Cursor) Next() (key, value []byte) {
	var k, v C.bolt_val
	var flags C.uint32_t
	C.bolt_cursor_next(c.C, &k, &v, &flags)
	return C.GoBytes(k.data, C.int(k.size)), C.GoBytes(v.data, C.int(v.size))
}

func warn(v ...interface{}) {
	fmt.Fprintln(os.Stderr, v...)
}

func warnf(msg string, v ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", v...)
}
