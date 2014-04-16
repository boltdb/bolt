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

page *get_page(bolt_cursor *c, pgid id) {
	printf("get_page: c->data=%d, c->pgsz=%d, pgid=%d\n\n", c->data, c->pgsz, id);
	return (page *)(c->data + (c->pgsz * id));
}

branch_elem *branch_page_element(page *p, uint16_t index) {
	return (branch_elem*)(p + sizeof(page) + index * sizeof(branch_elem));
}

leaf_elem *leaf_page_element(page *p, uint16_t index) {
	printf("leaf_page_element: page=%d, index=%d, sizeof(page)=%d, sizeof(leaf_elem)=%d\n\n", p, index, sizeof(page), sizeof(leaf_elem));
	printf("leaf_page_element: elem=%x\n", (leaf_elem*)(p + sizeof(page) + index * sizeof(leaf_elem))[0]);
	return (leaf_elem*)(p + sizeof(page) + index * sizeof(leaf_elem));
}

// return current leaf element
// if stack points at a branch page descend down to the first elemenet
// of the first leaf page
int leaf_element(bolt_cursor *c, bolt_val *key, bolt_val *value) {
	printf("leaf_element:1:\n\n");
	elem_ref *ref = &(c->stack[c->stackp]);
	printf("leaf_element:2:, ref->page->flags=%d\n\n", ref->page->flags);
	branch_elem *branch;
	while (ref->page->flags & BRANCH_PAGE) {
		printf("leaf_element:2.1, ref->page->flags=%d\n\n", ref->page->flags);
		branch = branch_page_element(ref->page,ref->index);
		printf("leaf_element:2.2\n\n");
		c->stackp++;
		//printf("leaf_element:2.3, c->stack=%d, c->stackp=%d\n\n", c->stack, c->stackp);
		ref = &c->stack[c->stackp];
		//printf("leaf_element:2.4, ref=%d\n\n", ref);
		ref->index = 0;
		printf("leaf_element:2.5\n\n");
		ref->page = get_page(c, branch->page);
		printf("leaf_element:2.6\n\n");
	};
	printf("leaf_element:3, key=%s, value=%s\n\n", key, value);
	set_key_value(leaf_page_element(ref->page,ref->index), key, value);
	printf("leaf_element:3, key=%s, value=%s\n\n", key, value);
	return 0;
}

set_key_value(leaf_elem *leaf, bolt_val *key, bolt_val *value) {
	key->size = leaf->ksize;
	key->data = leaf + leaf->pos;
	value->size = leaf->vsize;
	value->data = key->data + key->size;
	printf("set_key_value: key=%s (%d), value=%s (%d)\n\n", key->data, key->size, value->data, value->size);
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
	return leaf_element(c, key, value);
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
	return leaf_element(c, key, value);
}
*/
import "C"

import (
	// "fmt"
	"unsafe"

	"github.com/boltdb/bolt"
)

type bolt_cursor *C.bolt_cursor

func NewCursor(b *bolt.Bucket) bolt_cursor {
	info := b.Tx().DB().Info()
	root := b.Root()
	cursor := new(C.bolt_cursor)
	C.bolt_cursor_init(cursor, unsafe.Pointer(&info.Data[0]), (C.size_t)(info.PageSize), (C.pgid)(root))
	return cursor
}

func first(c bolt_cursor) (key, value []byte) {
	var k, v C.bolt_val
	// fmt.Println("cursor =", c)
	// fmt.Println("key =", k)
	// fmt.Println("value =", v)
	C.bolt_cursor_first(c, &k, &v)
	return C.GoBytes(k.data, C.int(k.size)), C.GoBytes(v.data, C.int(v.size))
}

func next(c bolt_cursor) (key, value []byte) {
	var k, v C.bolt_val
	C.bolt_cursor_next(c, &k, &v)
	return C.GoBytes(k.data, C.int(k.size)), C.GoBytes(v.data, C.int(v.size))
}
