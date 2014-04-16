package c

/*
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <inttypes.h>

#define MAX_DEPTH	100
#define BRANCH_PAGE	1

// These types MUST have the same layout as their corresponding Go types

typedef int64_t	pgid;

typedef struct page {
	pgid		id;
	uint16_t	flags;
	uint16_t	count;
	uint32_t	overflow;
} page;

typedef struct branch_elem {
	uint32_t	pos;
	uint32_t	ksize;
	pgid		page;
} branch_elem;

typedef struct leaf_elem {
	uint32_t	flags;
	uint32_t	pos;
	uint32_t	ksize;
	uint32_t	vsize;
} leaf_elem;

// private types

typedef struct elem_ref {
	page	*page;
	uint16_t	index;
} elem_ref;

// public types

typedef struct bolt_val {
    uint32_t size;
    void     *data;
} bolt_val;

typedef struct bolt_cursor {
	void			*data;
	pgid			root;
	size_t			pgsz;
	unsigned int	stackp;
	elem_ref		stack[MAX_DEPTH];
} bolt_cursor;


// int bolt_cursor_seek(bolt_cursor *c, bolt_val *key, bolt_val *actual_key, bolt_val *value)

// private functions

// Returns a page pointer from a page identifier.
page *get_page(bolt_cursor *c, pgid id) {
	return (page *)(c->data + (c->pgsz * id));
}

// Returns the leaf element at a given index on a given page.
branch_elem *branch_page_element(page *p, uint16_t index) {
	branch_elem *elements = (branch_elem*)((void*)(p) + sizeof(page));
	return &elements[index];
}

// Returns the leaf element at a given index on a given page.
leaf_elem *leaf_page_element(page *p, uint16_t index) {
	leaf_elem *elements = (leaf_elem*)((void*)(p) + sizeof(page));
	return &elements[index];
}

// Sets the key and value for a leaf element to a bolt value.
void key_value(leaf_elem *leaf, bolt_val *key, bolt_val *value) {
	key->size = leaf->ksize;
	key->data = ((void*)leaf) + leaf->pos;
	value->size = leaf->vsize;
	value->data = key->data + key->size;
}

// Traverses from the current stack position down to the first leaf element.
int bolt_cursor_first_leaf(bolt_cursor *c, bolt_val *key, bolt_val *value) {
	elem_ref *ref = &(c->stack[c->stackp]);
	branch_elem *branch;
	while (ref->page->flags & BRANCH_PAGE) {
		branch = branch_page_element(ref->page,ref->index);
		c->stackp++;
		ref = &c->stack[c->stackp];
		ref->index = 0;
		ref->page = get_page(c, branch->page);
	};
	key_value(leaf_page_element(ref->page,ref->index), key, value);
	return 0;
}

// public functions

void bolt_cursor_init(bolt_cursor *c, void *data, size_t pgsz, pgid root) {
	c->data = data;
	c->root = root;
	c->pgsz = pgsz;
}

int bolt_cursor_first(bolt_cursor *c, bolt_val *key, bolt_val *value) {
	leaf_elem	*leaf;
	elem_ref	*ref;

	// reset stack to initial state
	c->stackp = 0;
	ref = &(c->stack[c->stackp]);
	ref->page = get_page(c, c->root);
	ref->index = 0;

	// get current leaf element
	return bolt_cursor_first_leaf(c, key, value);
}

int bolt_cursor_next(bolt_cursor *c, bolt_val *key, bolt_val *value) {
	elem_ref *ref = &c->stack[c->stackp];

	// increment element index
	ref->index++;
	// if we're past last element pop the stack and repeat
	while (ref->index >= ref->page->count ) {
		c->stackp--;
		ref = &c->stack[c->stackp];
		ref->index++;
	};

	// get current leaf element
	return bolt_cursor_first_leaf(c, key, value);
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

// first moves the cursor to the first element and returns the key and value.
// Returns a nil key if there are no elements.
func first(c *Cursor) (key, value []byte) {
	var k, v C.bolt_val
	C.bolt_cursor_first(c.C, &k, &v)
	return C.GoBytes(k.data, C.int(k.size)), C.GoBytes(v.data, C.int(v.size))
}

// next moves the cursor to the next element and returns the key and value.
// Returns a nil key if at the end of the bucket.
func next(c *Cursor) (key, value []byte) {
	var k, v C.bolt_val
	C.bolt_cursor_next(c.C, &k, &v)
	return C.GoBytes(k.data, C.int(k.size)), C.GoBytes(v.data, C.int(v.size))
}

func warn(v ...interface{}) {
	fmt.Fprintln(os.Stderr, v...)
}

func warnf(msg string, v ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", v...)
}
